// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "driver/sfs/object.h"

#include "driver/sfs/multipart.h"
#include "driver/sfs/object_state.h"
#include "driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "driver/sfs/types.h"
#include "rgw_common.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

// Read

std::unique_ptr<SFSObject::ReadOp> SFSObject::get_read_op() {
  sfs::VersionedObjectHandle* vo =
      sfs::VersionedObjectHandle::resolve(store->db_conn, get_key());

  if (vo) {
    refresh_meta();  // ReadOp requires size set
    return std::make_unique<SFSObject::SFSReadOp>(this, *vo);
  } else {
    return nullptr;
    // TODO handle read to non existing / new / deleted object
    // use separate operation?
  }
}

SFSObject::SFSReadOp::SFSReadOp(
    SFSObject* _source, const sfs::VersionedObjectHandle& _vo
)
    : source(_source),
      vo(_vo),
      objdata(_source->store->get_data_path() / _vo.path()) {}

int SFSObject::SFSReadOp::prepare(
    optional_yield y, const DoutPrefixProvider* dpp
) {
  // TODO move existence check elsewhere. Perhaps a separate ReadOp.
  const auto state = sfs::ObjectAttr::state(source->store->db_conn, vo);
  if (!state.has_value() || state.value() == ObjectState::DELETED) {
    return -ENOENT;
  }

  // open the file to ensure that regardless of concurrent deletes, we
  // can finish the read operation that we started
  int ret = ::open(objdata.c_str(), O_CLOEXEC | O_RDONLY, 0644);
  if (ret < 0) {
    if (errno == ENOENT) {
      lsfs_dout(dpp, 10) << "object data file disappeared. likely from a "
                            "concurrent delete. returning ENOENT."
                         << dendl;
      return -ENOENT;
    }
    lsfs_dout(dpp, -1) << "error opening file " << objdata << ": "
                       << cpp_strerror(errno) << dendl;
    return -ERR_INTERNAL_ERROR;
  }
  fd = ret;

  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", fn: " << objdata.string() << ", fd: " << fd
                     << ", size: " << source->get_obj_size() << dendl;
  if (params.lastmod) {
    *params.lastmod = source->get_mtime();
  }
  return 0;
}

SFSObject::SFSReadOp::~SFSReadOp() {
  if (fd >= 0) {
    ::close(fd);
  }
}

int SFSObject::SFSReadOp::get_attr(
    const DoutPrefixProvider* dpp, const char* name, bufferlist& dest,
    optional_yield y
) {
  const auto maybe_attrs = sfs::ObjectAttr::get(source->store->db_conn, vo);
  lsfs_dout(dpp, 20) << fmt::format(
                            "{} name:{} have_attrs:{}", vo, name,
                            maybe_attrs.has_value()
                        )
                     << dendl;

  if (!maybe_attrs.has_value()) {
    return -ENOENT;
  }

  const auto attrs = maybe_attrs.value();
  if (const auto search = attrs.find(name); search != attrs.end()) {
    lsfs_dout(dpp, 20) << fmt::format(
                              "{} name:{} nattrs:{} value:{}", vo, name,
                              attrs.size(), search->second.to_str()
                          )
                       << dendl;
    dest.append(search->second);
    return 0;
  } else {
    return -ENODATA;
  }
}

// sync read
int SFSObject::SFSReadOp::read(
    int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
    const DoutPrefixProvider* dpp
) {
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 20) << fmt::format(
                            "{} sync bucket:{} {}:{} fd:{}, osize:{}", vo,
                            source->bucket->get_name(), ofs, len, fd,
                            source->get_obj_size()
                        )
                     << dendl;
  return read_to_bl(dpp, ofs, len, bl);
}

int SFSObject::SFSReadOp::read_to_bl(
    const DoutPrefixProvider* dpp, int64_t ofs, int64_t len, bufferlist& bl
) {
  const ssize_t lseek_ret = ::lseek64(fd, ofs, SEEK_SET);
  if (lseek_ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "seek to offset {} failed with {}. "
                              "fn: {}, fd: {}. "
                              "returning internal error",
                              cpp_strerror(errno), ofs, objdata.string(), fd
                          )
                       << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  const ssize_t read_ret = bl.read_fd(fd, len);
  if (read_ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "read at offset:len {}:{}  failed with {}. "
                              "fn: {}, fd: {}. "
                              "returning internal error",
                              ofs, len, cpp_strerror(errno), objdata.string(),
                              fd
                          )
                       << dendl;
    return -ERR_INTERNAL_ERROR;
  }
  return read_ret;
}

// async read
int SFSObject::SFSReadOp::iterate(
    const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb,
    optional_yield y
) {
  // TODO bounds check, etc.
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 20) << fmt::format(
                            "{} async bucket:{} {}:{} fd:{}, osize:{}", vo,
                            source->bucket->get_name(), ofs, len, fd,
                            source->get_obj_size()
                        )
                     << dendl;
  const uint64_t max_chunk_size = 10485760;  // 10MB
  uint64_t missing = len;
  while (missing > 0) {
    const uint64_t size = std::min(missing, max_chunk_size);
    bufferlist bl;
    int ret = read_to_bl(dpp, ofs, size, bl);
    if (ret < 0) {
      return ret;
    }
    missing -= ret;
    lsfs_dout(dpp, 10) << "return " << ret << "/" << len << ", offset: " << ofs
                       << ", requested size " << size
                       << ", missing: " << missing << dendl;
    ret = cb->handle_data(bl, 0, size);
    if (ret < 0) {
      lsfs_dout(dpp, 0) << "failed to return object data: " << ret << dendl;
      return -ERR_INTERNAL_ERROR;
    }
    ofs += ret;
  }
  return len;
}

SFSObject::SFSDeleteOp::SFSDeleteOp(
    SFSObject* _source, sfs::BucketRef _bucketref
)
    : source(_source), bucketref(_bucketref) {}

int SFSObject::SFSDeleteOp::delete_obj(
    const DoutPrefixProvider* dpp, optional_yield y
) {
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << "bucket versioning: "
                     << source->bucket->versioning_enabled()
                     << ", object: " << source->get_name()
                     << ", instance: " << source->get_instance() << dendl;

  // do the quick and dirty thing for now
  ceph_assert(bucketref);
  if (!source->objref) {
    source->refresh_meta();
  }

  auto version_id = source->get_instance();
  if (source->objref) {
    bucketref->delete_object(source->objref, source->get_key());
  } else if (source->bucket->versioning_enabled()) {
    // create delete marker
    // even the object does not exist AWS creates a delete marker for it
    // if versioning is enabled
    version_id =
        bucketref->create_non_existing_object_delete_marker(source->get_key());
  }

  // versioning is enabled set x-amz-delete-marker to true in the response
  // and return the version id
  if (source->bucket->versioning_enabled()) {
    result.version_id = version_id;
    result.delete_marker = true;
  }
  return 0;
}

int SFSObject::delete_object(
    const DoutPrefixProvider* dpp, optional_yield y, bool prevent_versioning
) {
  lsfs_dout(dpp, 10) << "prevent_versioning: " << prevent_versioning << dendl;
  auto ref = store->get_bucket_ref(get_bucket()->get_name());
  SFSObject::SFSDeleteOp del(this, ref);
  return del.delete_obj(dpp, y);
}

int SFSObject::copy_object(
    User* user, req_info* info, const rgw_zone_id& source_zone,
    rgw::sal::Object* dst_object, rgw::sal::Bucket* dst_bucket,
    rgw::sal::Bucket* src_bucket, const rgw_placement_rule& dest_placement,
    ceph::real_time* src_mtime, ceph::real_time* mtime,
    const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
    bool high_precision_time, const char* if_match, const char* if_nomatch,
    AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
    RGWObjCategory category, uint64_t olh_epoch,
    boost::optional<ceph::real_time> delete_at, std::string* version_id,
    std::string* tag, std::string* etag, void (*progress_cb)(off_t, void*),
    void* progress_data, const DoutPrefixProvider* dpp, optional_yield y
) {
  lsfs_dout(dpp, 10) << "source(bucket: " << src_bucket->get_name()
                     << ", obj: " << get_name()
                     << "), dest(bucket: " << dst_bucket->get_name()
                     << ", obj: " << dst_object->get_name() << ")" << dendl;

  refresh_meta();
  ceph_assert(objref);
  ceph_assert(bucketref);
  ceph_assert(dst_object);
  ceph_assert(dst_bucket);

  sfs::BucketRef dst_bucket_ref = store->get_bucket_ref(dst_bucket->get_name());
  ceph_assert(dst_bucket_ref);

  std::filesystem::path srcpath =
      store->get_data_path() / objref->get_storage_path();

  sfs::ObjectRef dstref = dst_bucket_ref->get_or_create(dst_object->get_key());
  std::filesystem::path dstpath =
      store->get_data_path() / dstref->get_storage_path();

  if (std::filesystem::exists(dstpath)) {
    // this breaks S3 semantics: as far as we understand, a copy to an existing
    // destination object is essentially a put on that object -- meaning, the
    // object is clobbered.
    lsfs_dout(dpp, 10) << "destination file already exists at '" << dstpath
                       << "'" << dendl;
    return -EEXIST;
  }

  lsfs_dout(dpp, 10) << "copying file from '" << srcpath << "' to '" << dstpath
                     << "'" << dendl;
  std::filesystem::create_directories(dstpath.parent_path());
  bool res = std::filesystem::copy_file(srcpath, dstpath);
  if (!res) {
    lsfs_dout(dpp, 0) << "error copying file from '" << srcpath << "' to '"
                      << dstpath << "'" << dendl;
    return -EIO;
  }

  auto dest_meta = objref->get_meta();
  dest_meta.mtime = ceph::real_clock::now();
  dstref->update_attrs(objref->get_attrs());
  dstref->update_meta(dest_meta);
  dstref->metadata_finish(store);

  return 0;
}

void SFSObject::gen_rand_obj_instance_name() {
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(
      store->ceph_context(), buf, OBJ_INSTANCE_LEN
  );

  state.obj.key.set_instance(buf);
}

/*
  This should be intended as a mere fetch of object's attributes.
  The caller will likely call get_attrs() after get_obj_attrs().
  This is how we have interpreted this call.
  The average usage of this seems to suggest that one first calls
  get_obj_attrs() and then it uses get_attrs().
  Doubts remain that this could be entirely correct for all cases;
  so for the time being, we leave target_obj empty.
  We rely on the fact that, if in the future something could potentially fail
  because target_obj is left empty, that fail will be explicit.
*/
int SFSObject::get_obj_attrs(
    optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj
) {
  refresh_meta();
  return 0;
}

int SFSObject::get_obj_state(
    const DoutPrefixProvider* dpp, RGWObjState** _state, optional_yield y,
    bool follow_olh
) {
  refresh_meta();
  *_state = &state;
  return 0;
}

int SFSObject::set_obj_attrs(
    const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs,
    optional_yield y
) {
  ceph_assert(objref);
  map<string, bufferlist>::iterator iter;

  if (delattrs) {
    for (iter = delattrs->begin(); iter != delattrs->end(); ++iter) {
      objref->del_attr(iter->first);
    }
  }
  if (setattrs) {
    for (iter = setattrs->begin(); iter != setattrs->end(); ++iter) {
      objref->set_attr(iter->first, iter->second);
    }
  }

  //synch attrs caches
  state.attrset = objref->get_attrs();
  state.has_attrs = true;

  objref->metadata_flush_attrs(store);
  return 0;
}

int SFSObject::modify_obj_attrs(
    const char* attr_name, bufferlist& attr_val, optional_yield y,
    const DoutPrefixProvider* dpp
) {
  if (!attr_name) {
    return 0;
  }
  ceph_assert(objref);
  objref->set_attr(attr_name, attr_val);

  //synch attrs caches
  state.attrset = objref->get_attrs();
  state.has_attrs = true;

  objref->metadata_flush_attrs(store);
  return 0;
}

int SFSObject::delete_obj_attrs(
    const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y
) {
  if (!attr_name) {
    return 0;
  }
  ceph_assert(objref);
  if (objref->del_attr(attr_name)) {
    //synch attrs caches
    state.attrset = objref->get_attrs();
    state.has_attrs = true;

    objref->metadata_flush_attrs(store);
  }
  return 0;
}

std::unique_ptr<MPSerializer> SFSObject::get_serializer(
    const DoutPrefixProvider* dpp, const std::string& lock_name
) {
  lsfs_dout(dpp, 10) << "lock name: " << lock_name << dendl;
  return std::make_unique<SFSMultipartSerializer>();
}

int SFSObject::transition(
    Bucket* bucket, const rgw_placement_rule& placement_rule,
    const real_time& mtime, uint64_t olh_epoch, const DoutPrefixProvider* dpp,
    optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::transition_to_cloud(
    Bucket* bucket, rgw::sal::PlacementTier* tier, rgw_bucket_dir_entry& o,
    std::set<std::string>& cloud_targets, CephContext* cct, bool update_object,
    const DoutPrefixProvider* dpp, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": not supported" << dendl;
  return -ENOTSUP;
}

bool SFSObject::placement_rules_match(
    rgw_placement_rule& r1, rgw_placement_rule& r2
) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}

int SFSObject::dump_obj_layout(
    const DoutPrefixProvider* dpp, optional_yield y, Formatter* f
) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::swift_versioning_restore(
    bool& restored, /* out */
    const DoutPrefixProvider* dpp
) {
  ldpp_dout(dpp, 10) << __func__ << ": do nothing." << dendl;
  return 0;
}

int SFSObject::swift_versioning_copy(
    const DoutPrefixProvider* dpp, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": do nothing." << dendl;
  return 0;
}

int SFSObject::omap_get_vals_by_keys(
    const DoutPrefixProvider* dpp, const std::string& oid,
    const std::set<std::string>& keys, Attrs* vals
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::omap_set_val_by_key(
    const DoutPrefixProvider* dpp, const std::string& key, bufferlist& val,
    bool must_exist, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::chown(
    rgw::sal::User& new_user, const DoutPrefixProvider* dpp, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<rgw::sal::Object::DeleteOp> SFSObject::get_delete_op() {
  ceph_assert(bucket != nullptr);
  auto ref = store->get_bucket_ref(bucket->get_name());
  return std::make_unique<SFSObject::SFSDeleteOp>(this, ref);
}

void SFSObject::refresh_meta() {
  if (!bucketref) {
    bucketref = store->get_bucket_ref(bucket->get_name());
  }
  try {
    objref = bucketref->get(get_name());
  } catch (sfs::UnknownObjectException& e) {
    // object probably not created yet?
    return;
  }
  _refresh_meta_from_object();
}

void SFSObject::_refresh_meta_from_object() {
  ceph_assert(objref);
  if (!get_instance().empty() && get_instance() != objref->instance) {
    // object specific version requested and it's not the last one
    sfs::sqlite::SQLiteVersionedObjects db_versioned_objects(store->db_conn);
    auto db_version = db_versioned_objects.get_versioned_object(get_instance());
    if (db_version.has_value()) {
      auto uuid = objref->path.get_uuid();
      auto deleted = db_version->object_state == ObjectState::DELETED;
      objref.reset(sfs::Object::create_for_query(
          get_name(), uuid, deleted, db_version->id
      ));
      set_obj_size(db_version->size);
      objref->update_attrs(db_version->attrs);
      auto meta = objref->get_meta();
      meta.etag = db_version->etag;
      objref->update_meta(meta);
    }
  } else {
    set_obj_size(objref->get_meta().size);
  }
  set_attrs(objref->get_attrs());
  state.mtime = objref->get_meta().mtime;
}

}  // namespace rgw::sal
