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

#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SFSObject::SFSReadOp::SFSReadOp(SFSObject* _source) : source(_source) {}

int SFSObject::SFSReadOp::prepare(
    optional_yield y, const DoutPrefixProvider* dpp
) {
  source->refresh_meta();
  objref = source->get_object_ref();
  if (!objref) {
    // at this point, we don't have an objectref because
    // the object does not exist.
    return -ENOENT;
  }

  objdata = source->store->get_data_path() / objref->path.to_path();
  if (!std::filesystem::exists(objdata)) {
    lsfs_dout(dpp, 10) << "object data not found at " << objdata << dendl;
    return -ENOENT;
  }

  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size() << dendl;
  return 0;
}

int SFSObject::SFSReadOp::get_attr(
    const DoutPrefixProvider* dpp, const char* name, bufferlist& dest,
    optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO: " << name << dendl;

  if (std::strcmp(name, "user.rgw.acl") == 0) {
    // TODO support 'user.rgw.acl' to support read_permissions. Return
    // empty policy since our test user is admin for now.
    RGWAccessControlPolicy policy;
    policy.encode(dest);
    return 0;
  }
  return -ENOTSUP;
}

// sync read
int SFSObject::SFSReadOp::read(
    int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
    const DoutPrefixProvider* dpp
) {
  // TODO bounds check, etc.
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size()
                     << ", offset: " << ofs << ", end: " << end
                     << ", len: " << len << dendl;

  // auto objpath = source->get_data_path();
  // ceph_assert(std::filesystem::exists(objpath));
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
    optional_yield y
) {
  // TODO bounds check, etc.
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size()
                     << ", offset: " << ofs << ", end: " << end
                     << ", len: " << len << dendl;

  // auto objpath = source->get_data_path();
  // ceph_assert(std::filesystem::exists(objpath));
  ceph_assert(std::filesystem::exists(objdata));

  // TODO chunk the read
  bufferlist bl;
  std::string error;
  int ret = bl.pread_file(objdata.c_str(), ofs, len, &error);
  if (ret < 0) {
    lsfs_dout(dpp, 10) << "failed to read object from file " << objdata
                       << ". Returning EIO." << dendl;
    return -EIO;
  }

  cb->handle_data(bl, ofs, len);
  return 0;
}

SFSObject::SFSDeleteOp::SFSDeleteOp(
    SFSObject* _source, sfs::BucketRef _bucketref
)
    : source(_source), bucketref(_bucketref) {}

int SFSObject::SFSDeleteOp::delete_obj(
    const DoutPrefixProvider* dpp, optional_yield y
) {
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", object: " << source->get_name() << dendl;

  // do the quick and dirty thing for now
  ceph_assert(bucketref);
  if (!source->objref) {
    source->refresh_meta();
  }
  ceph_assert(source->objref);
  bucketref->delete_object(source->objref);
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
      store->get_data_path() / objref->path.to_path();

  sfs::ObjectRef dstref = dst_bucket_ref->get_or_create(dst_object->get_name());
  std::filesystem::path dstpath =
      store->get_data_path() / dstref->path.to_path();

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

  dstref->meta = objref->meta;
  dstref->meta.mtime = ceph::real_clock::now();
  dst_bucket_ref->finish(dpp, dst_object->get_name());

  return 0;
}

/** TODO Create a randomized instance ID for this object */
void SFSObject::gen_rand_obj_instance_name() {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SFSObject::get_obj_attrs(
    optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSObject::modify_obj_attrs(
    const char* attr_name, bufferlist& attr_val, optional_yield y,
    const DoutPrefixProvider* dpp
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSObject::delete_obj_attrs(
    const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<MPSerializer> SFSObject::get_serializer(
    const DoutPrefixProvider* dpp, const std::string& lock_name
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
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
    ;
  } catch (sfs::UnknownObjectException& e) {
    // object probably not created yet?
    return;
  }
  set_obj_size(objref->meta.size);
  set_attrs(objref->meta.attrs);
  state.mtime = objref->meta.mtime;
}

}  // namespace rgw::sal
