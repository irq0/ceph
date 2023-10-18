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

#include <fmt/chrono.h>
#include <fmt/format.h>

#include "driver/sfs/multipart.h"
#include "driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "driver/sfs/types.h"
#include "rgw_common.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SFSObject::SFSReadOp::SFSReadOp(SFSObject* _source) : source(_source) {
  /*
    This initialization code was originally into prepare() but that
    was not sufficient to cover all cases.
    There are pieces of SAL code that are calling get_*() methods
    but they don't call prepare().
    In those cases the SFSReadOp is not properly initialized and those
    calls are going to fail.
  */
  // read op needs to retrieve also the version_id from the db
  source->refresh_meta(true);
  objref = source->get_object_ref();
}

// Handle conditional GET params. If-Match, If-None-Match,
// If-Modified-Since, If-UnModified-Since. Return 0 if we are neutral.
// Otherwise return S3/HTTP error code.
int SFSObject::SFSReadOp::handle_conditionals(const DoutPrefixProvider* dpp
) const {
  if (!params.if_match && !params.if_nomatch && !params.mod_ptr &&
      !params.unmod_ptr) {
    return 0;
  }
  const std::string etag = objref->get_meta().etag;
  const auto mtime = objref->get_meta().mtime;
  int result = 0;

  if (params.if_match) {
    const std::string match = rgw_string_unquote(params.if_match);
    result = (etag == match) ? 0 : -ERR_PRECONDITION_FAILED;
    ldpp_dout(dpp, 10) << fmt::format(
                              "If-Match: etag={} vs. ifmatch={}: {}", etag,
                              match, result
                          )
                       << dendl;
  }
  if (params.if_nomatch) {
    const std::string match = rgw_string_unquote(params.if_nomatch);
    result = (etag == match) ? -ERR_NOT_MODIFIED : 0;
    ldpp_dout(dpp, 10) << fmt::format(
                              "If-None-Match: etag={} vs. ifmatch={}: {}", etag,
                              match, result
                          )
                       << dendl;
  }
  // RFC 7232 3.3. A recipient MUST ignore If-Modified-Since if the
  // request contains an If-None-Match header field
  if (params.mod_ptr && !params.if_nomatch) {
    result = (mtime > *params.mod_ptr) ? 0 : -ERR_NOT_MODIFIED;
    ldpp_dout(dpp, 10)
        << fmt::format(
               "If-Modified-Since: mtime={:%Y-%m-%d %H:%M:%S} vs. "
               "if_time={:%Y-%m-%d %H:%M:%S}: {}",
               fmt::gmtime(ceph::real_clock::to_time_t(mtime)),
               fmt::gmtime(ceph::real_clock::to_time_t(*params.mod_ptr)), result
           )
        << dendl;
  }
  // RFC 7232 3.4. A recipient MUST ignore If-Unmodified-Since if the
  // request contains an If-Match header field
  if (params.unmod_ptr && !params.if_match) {
    result = (mtime < *params.unmod_ptr) ? 0 : -ERR_PRECONDITION_FAILED;
    ldpp_dout(dpp, 10)
        << fmt::format(
               "If-UnModified-Since: mtime={:%Y-%m-%d %H:%M:%S} vs. "
               "if_time={:%Y-%m-%d %H:%M:%S}: {}",
               fmt::gmtime(ceph::real_clock::to_time_t(mtime)),
               fmt::gmtime(ceph::real_clock::to_time_t(*params.unmod_ptr)),
               result
           )
        << dendl;
  }
  ldpp_dout(dpp, 10)
      << fmt::format(
             "Conditional GET (Match/NoneMatch/Mod/UnMod) ({}, {}): {}",
             params.if_match != nullptr, params.if_nomatch != nullptr,
             params.mod_ptr != nullptr, params.unmod_ptr != nullptr, result
         )
      << dendl;
  return result;
}

int SFSObject::SFSReadOp::prepare(
    optional_yield /*y*/, const DoutPrefixProvider* dpp
) {
  if (!objref || objref->deleted) {
    // at this point, we don't have an objectref because
    // the object does not exist.
    return -ENOENT;
  }

  objdata = source->store->get_data_path() / objref->get_storage_path();
  if (!std::filesystem::exists(objdata)) {
    lsfs_dout(dpp, 10) << "object data not found at " << objdata << dendl;
    return -ENOENT;
  }

  lsfs_dout(dpp, 10)
      << fmt::format(
             "bucket:{} obj:{} size:{} versionid:{} "
             "conditionals:(ifmatch:{} ifnomatch:{} ifmod:{} ifunmod:{})",
             source->bucket->get_name(), source->get_name(),
             source->get_obj_size(), source->get_instance(),
             fmt::ptr(params.if_match), fmt::ptr(params.if_nomatch),
             fmt::ptr(params.mod_ptr), fmt::ptr(params.unmod_ptr)
         )
      << dendl;

  if (params.lastmod) {
    *params.lastmod = source->get_mtime();
  }
  return handle_conditionals(dpp);
}

int SFSObject::SFSReadOp::get_attr(
    const DoutPrefixProvider* /*dpp*/, const char* name, bufferlist& dest,
    optional_yield /*y*/
) {
  if (!objref || objref->deleted) {
    return -ENOENT;
  }
  if (!objref->get_attr(name, dest)) {
    return -ENODATA;
  }
  return 0;
}

// sync read
int SFSObject::SFSReadOp::read(
    int64_t ofs, int64_t end, bufferlist& bl, optional_yield /*y*/,
    const DoutPrefixProvider* dpp
) {
  // TODO bounds check, etc.
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size()
                     << ", offset: " << ofs << ", end: " << end
                     << ", len: " << len << dendl;

  ceph_assert(std::filesystem::exists(objdata));

  std::string error;
  int ret = bl.pread_file(objdata.c_str(), ofs, len, &error);
  if (ret < 0) {
    lsfs_dout(dpp, 10) << "failed to read object from file " << objdata
                       << ". Returning EIO." << dendl;
    return -EIO;
  }
  return 0;
}

// async read
int SFSObject::SFSReadOp::iterate(
    const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb,
    optional_yield /*y*/
) {
  // TODO bounds check, etc.
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size()
                     << ", offset: " << ofs << ", end: " << end
                     << ", len: " << len << dendl;

  ceph_assert(std::filesystem::exists(objdata));
  std::string error;

  const uint64_t max_chunk_size = 10485760;  // 10MB
  uint64_t missing = len;
  while (missing > 0) {
    uint64_t size = std::min(missing, max_chunk_size);
    bufferlist bl;
    int ret = bl.pread_file(objdata.c_str(), ofs, size, &error);
    if (ret < 0) {
      lsfs_dout(dpp, 0) << "failed to read object from file '" << objdata
                        << ", offset: " << ofs << ", size: " << size << ": "
                        << error << dendl;
      return -EIO;
    }
    missing -= size;
    lsfs_dout(dpp, 10) << "return " << size << "/" << len << ", offset: " << ofs
                       << ", missing: " << missing << dendl;
    ret = cb->handle_data(bl, 0, size);
    if (ret < 0) {
      lsfs_dout(dpp, 0) << "failed to return object data: " << ret << dendl;
      return -EIO;
    }

    ofs += size;
  }
  return 0;
}

SFSObject::SFSDeleteOp::SFSDeleteOp(
    SFSObject* _source, sfs::BucketRef _bucketref
)
    : source(_source), bucketref(_bucketref) {}

int SFSObject::SFSDeleteOp::delete_obj(
    const DoutPrefixProvider* dpp, optional_yield /*y*/
) {
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << " bucket versioning: "
                     << source->bucket->versioning_enabled()
                     << ", object: " << source->get_name()
                     << ", instance: " << source->get_instance() << dendl;

  // do the quick and dirty thing for now
  ceph_assert(bucketref);
  if (!source->objref) {
    source->refresh_meta();
  }

  auto version_id = source->get_instance();
  std::string delete_marker_version_id;
  if (source->objref) {
    bucketref->delete_object(
        *source->objref, source->get_key(),
        source->bucket->versioning_enabled(), delete_marker_version_id
    );
  } else if (source->bucket->versioning_enabled() && source->get_instance().empty()) {
    // create delete marker
    // even the object does not exist AWS creates a delete marker for it
    // if versioning is enabled and a specific version was not specified
    version_id =
        bucketref->create_non_existing_object_delete_marker(source->get_key());
  }

  // versioning is enabled set x-amz-delete-marker to true in the response
  // and return the version id
  if (source->bucket->versioning_enabled()) {
    result.version_id = version_id;
    if (!delete_marker_version_id.empty()) {
      // a new delete marker was created.
      // Return the version id generated for it.
      result.version_id = delete_marker_version_id;
    }
    source->delete_marker = true;  // needed for multiobject delete
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
    User* /*user*/, req_info* /*info*/, const rgw_zone_id& /*source_zone*/,
    rgw::sal::Object* dst_object, rgw::sal::Bucket* dst_bucket,
    rgw::sal::Bucket* src_bucket, const rgw_placement_rule& /*dest_placement*/,
    ceph::real_time* /*src_mtime*/, ceph::real_time* mtime,
    const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
    bool /*high_precision_time*/, const char* if_match, const char* if_nomatch,
    AttrsMod /*attrs_mod*/, bool /*copy_if_newer*/, Attrs& /*attrs*/,
    RGWObjCategory /*category*/, uint64_t /*olh_epoch*/,
    boost::optional<ceph::real_time> /*delete_at*/, std::string* /*version_id*/,
    std::string* /*tag*/, std::string* etag, void (*)(off_t, void*),
    void* /*progress_data*/
    ,
    const DoutPrefixProvider* dpp, optional_yield /*y*/
) {
  lsfs_dout(dpp, 10) << fmt::format(
                            "bucket:{} obj:{} version:{} size:{} -> bucket:{} "
                            "obj:{} version:{}",
                            src_bucket->get_name(), get_name(), get_instance(),
                            get_obj_size(), dst_bucket->get_name(),
                            dst_object->get_name(), dst_object->get_instance()
                        )
                     << dendl;

  refresh_meta();
  ceph_assert(objref);
  ceph_assert(bucketref);
  ceph_assert(dst_object);
  ceph_assert(dst_bucket);

  const auto check_conditional = handle_copy_object_conditionals(
      dpp, mod_ptr, unmod_ptr, if_match, if_nomatch, objref->get_meta().etag,
      objref->get_meta().mtime
  );
  if (check_conditional != 0) {
    return check_conditional;
  }

  const sfs::BucketRef dst_bucket_ref =
      store->get_bucket_ref(dst_bucket->get_name());
  ceph_assert(dst_bucket_ref);

  const std::filesystem::path srcpath =
      store->get_data_path() / objref->get_storage_path();

  const int src_fd = ::open(srcpath.c_str(), O_RDONLY | O_BINARY);
  if (src_fd < 0) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "unable to open src obj {} file {} for reading: {}",
               objref->name, srcpath.string(), cpp_strerror(errno)
           )
        << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  const sfs::ObjectRef dstref =
      dst_bucket_ref->create_version(dst_object->get_key());
  if (!dstref) {
    ::close(src_fd);
    return -ERR_INTERNAL_ERROR;
  }
  const std::filesystem::path dstpath =
      store->get_data_path() / dstref->get_storage_path();
  std::error_code ec;
  std::filesystem::create_directories(dstpath.parent_path(), ec);
  if (ec) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "failed to create directory hierarchy {} for {}: {}",
               dstpath.parent_path().string(), dstref->name, ec.message()
           )
        << dendl;
    ::close(src_fd);
    return -ERR_INTERNAL_ERROR;
  }
  // Open O_CREAT+O_EXCL as dstref is always a new version without a
  // file yet
  const int dst_fd =
      ::open(dstpath.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_BINARY, 0600);
  if (dst_fd < 0) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "unable to open dst obj {} file {} for writing: {}",
               dstref->name, dstpath.string(), cpp_strerror(errno)
           )
        << dendl;
    ::close(src_fd);
    return -ERR_INTERNAL_ERROR;
  }

  lsfs_dout(dpp, 10) << fmt::format(
                            "copying {} fd:{} -> {} fd:{}", srcpath.string(),
                            src_fd, dstpath.string(), dst_fd
                        )
                     << dendl;

  int ret = ::copy_file_range(
      src_fd, nullptr, dst_fd, nullptr, objref->get_meta().size, 0
  );
  if (ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to copy file from {} to {}: {}",
                              srcpath.string(), dstpath.string(),
                              cpp_strerror(errno)
                          )
                       << dendl;
    ::close(src_fd);
    ::close(dst_fd);
    return -ERR_INTERNAL_ERROR;
  }
  ret = ::close(src_fd);
  if (ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed closing src fd:{} fn:{}: {}", src_fd,
                              srcpath.string(), cpp_strerror(ret)
                          )
                       << dendl;
  }
  ret = ::close(dst_fd);
  if (ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed closing dst fd:{} fn:{}: {}", dst_fd,
                              dstpath.string(), cpp_strerror(ret)
                          )
                       << dendl;
  }

  auto dest_meta = objref->get_meta();
  dest_meta.mtime = ceph::real_clock::now();
  dstref->update_attrs(objref->get_attrs());
  dstref->update_meta(dest_meta);
  dstref->metadata_finish(
      store, dst_bucket_ref->get_info().versioning_enabled()
  );

  // return values for CopyObjectResult response
  if (etag != nullptr) {
    *etag = dstref->get_meta().etag;
  }
  if (mtime != nullptr) {
    *mtime = dstref->get_meta().mtime;
  }
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
    optional_yield /*y*/, const DoutPrefixProvider* /*dpp*/,
    rgw_obj* /*target_obj*/
) {
  refresh_meta();
  return 0;
}

int SFSObject::get_obj_state(
    const DoutPrefixProvider* /*dpp*/, RGWObjState** _state,
    optional_yield /*y*/, bool /*follow_olh*/
) {
  refresh_meta();
  *_state = &state;
  return 0;
}

int SFSObject::set_obj_attrs(
    const DoutPrefixProvider* /*dpp*/, Attrs* setattrs, Attrs* delattrs,
    optional_yield /*y*/
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

bool SFSObject::get_attr(const std::string& name, bufferlist& dest) {
  return objref->get_attr(name, dest);
}

int SFSObject::modify_obj_attrs(
    const char* attr_name, bufferlist& attr_val, optional_yield /*y*/,
    const DoutPrefixProvider* /*dpp*/
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
    const DoutPrefixProvider* /*dpp*/, const char* attr_name,
    optional_yield /*y*/
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
  return std::make_unique<sfs::SFSMultipartSerializer>();
}

int SFSObject::transition(
    Bucket*, const rgw_placement_rule&, const real_time&, uint64_t,
    const DoutPrefixProvider* dpp, optional_yield
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::transition_to_cloud(
    Bucket*, rgw::sal::PlacementTier*, rgw_bucket_dir_entry&,
    std::set<std::string>&, CephContext*, bool, const DoutPrefixProvider* dpp,
    optional_yield
) {
  ldpp_dout(dpp, 10) << __func__ << ": not supported" << dendl;
  return -ENOTSUP;
}

bool SFSObject::placement_rules_match(
    rgw_placement_rule& /*r1*/, rgw_placement_rule& /*r2*/
) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}

int SFSObject::dump_obj_layout(
    const DoutPrefixProvider* /*dpp*/, optional_yield /*y*/, Formatter* /*f*/
) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::swift_versioning_restore(
    bool& /*restored*/, /* out */
    const DoutPrefixProvider* dpp
) {
  ldpp_dout(dpp, 10) << __func__ << ": do nothing." << dendl;
  return 0;
}

int SFSObject::swift_versioning_copy(
    const DoutPrefixProvider* dpp, optional_yield /*y*/
) {
  ldpp_dout(dpp, 10) << __func__ << ": do nothing." << dendl;
  return 0;
}

int SFSObject::omap_get_vals_by_keys(
    const DoutPrefixProvider* dpp, const std::string& /*oid*/,
    const std::set<std::string>& /*keys*/, Attrs* /*vals*/
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::omap_set_val_by_key(
    const DoutPrefixProvider* dpp, const std::string& /*key*/,
    bufferlist& /*val*/, bool /*must_exist*/, optional_yield /*y*/
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::chown(
    rgw::sal::User& /*new_user*/, const DoutPrefixProvider* dpp,
    optional_yield /*y*/
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<rgw::sal::Object::DeleteOp> SFSObject::get_delete_op() {
  ceph_assert(bucket != nullptr);
  auto ref = store->get_bucket_ref(bucket->get_name());
  return std::make_unique<SFSObject::SFSDeleteOp>(this, ref);
}

void SFSObject::refresh_meta(bool update_version_id_from_metadata) {
  if (!bucketref) {
    bucketref = store->get_bucket_ref(bucket->get_name());
  }
  try {
    objref = bucketref->get(rgw_obj_key(get_name(), get_instance()));
  } catch (sfs::UnknownObjectException& e) {
    // object probably not created yet?
    return;
  }
  _refresh_meta_from_object(objref, update_version_id_from_metadata);
}

void SFSObject::_refresh_meta_from_object(
    sfs::ObjectRef obj_to_refresh, bool update_version_id_from_metadata
) {
  ceph_assert(obj_to_refresh);
  // fill values from objref
  set_obj_size(obj_to_refresh->get_meta().size);
  set_attrs(obj_to_refresh->get_attrs());
  state.accounted_size = obj_to_refresh->get_meta().size;
  state.mtime = obj_to_refresh->get_meta().mtime;
  state.exists = true;
  if (update_version_id_from_metadata) {
    set_instance(obj_to_refresh->instance);
  }
}

int SFSObject::handle_copy_object_conditionals(
    const DoutPrefixProvider* dpp, const ceph::real_time* mod_ptr,
    const ceph::real_time* unmod_ptr, const char* if_match,
    const char* if_nomatch, const std::string& etag,
    const ceph::real_time& mtime
) const {
  // This implementation diverts from the getObject one because the Amazon docs
  // are not clear enough and the s3tests expect different returns codes.
  // In addition s3tests don't test mod_ptr nor unmod_ptr
  // I've tested this with another S3 vendor and the return code for all error
  // cases is ERR_PRECONDITION_FAILED
  if (!if_match && !if_nomatch && !mod_ptr && !unmod_ptr) {
    return 0;
  }
  int result = 0;

  if (if_match) {
    const std::string match = rgw_string_unquote(if_match);
    result = (etag == match) ? 0 : -ERR_PRECONDITION_FAILED;
    ldpp_dout(dpp, 10) << fmt::format(
                              "If-Match: etag={} vs. ifmatch={}: {}", etag,
                              match, result
                          )
                       << dendl;
  }
  if (if_nomatch) {
    const std::string match = rgw_string_unquote(if_nomatch);
    result = (etag == match) ? -ERR_PRECONDITION_FAILED : 0;
    ldpp_dout(dpp, 1) << fmt::format(
                             "If-None-Match: etag={} vs. ifmatch={}: {}", etag,
                             match, result
                         )
                      << dendl;
  }
  if (mod_ptr && !if_nomatch) {
    result = (mtime > *mod_ptr) ? 0 : -ERR_PRECONDITION_FAILED;
    ldpp_dout(dpp, 10)
        << fmt::format(
               "If-Modified-Since: mtime={:%Y-%m-%d %H:%M:%S} vs. "
               "if_time={:%Y-%m-%d %H:%M:%S}: {}",
               fmt::gmtime(ceph::real_clock::to_time_t(mtime)),
               fmt::gmtime(ceph::real_clock::to_time_t(*mod_ptr)), result
           )
        << dendl;
  }
  if (unmod_ptr && !if_match) {
    result = (mtime < *unmod_ptr) ? 0 : -ERR_PRECONDITION_FAILED;
    ldpp_dout(dpp, 10)
        << fmt::format(
               "If-UnModified-Since: mtime={:%Y-%m-%d %H:%M:%S} vs. "
               "if_time={:%Y-%m-%d %H:%M:%S}: {}",
               fmt::gmtime(ceph::real_clock::to_time_t(mtime)),
               fmt::gmtime(ceph::real_clock::to_time_t(*unmod_ptr)), result
           )
        << dendl;
  }
  ldpp_dout(dpp, 10)
      << fmt::format(
             "Conditional COPY_OBJ (Match/NoneMatch/Mod/UnMod) ({}, {}): {}",
             if_match != nullptr, if_nomatch != nullptr, mod_ptr != nullptr,
             unmod_ptr != nullptr, result
         )
      << dendl;
  return result;
}

}  // namespace rgw::sal
