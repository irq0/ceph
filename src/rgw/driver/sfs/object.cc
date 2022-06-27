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

SFSObject::SFSReadOp::SFSReadOp(SFSObject* _source, RGWObjectCtx* _rctx)
    : source(_source), rctx(_rctx) {}

int SFSObject::SFSReadOp::prepare(
    optional_yield y, const DoutPrefixProvider* dpp
) {
  source->refresh_meta();

  auto objdata = source->get_data_path();
  auto objmeta = source->get_metadata_path();

  if (!std::filesystem::exists(objmeta)) {
    lsfs_dout(dpp, 10) << "object metadata not found at " << objmeta << dendl;
    return -ENOENT;
  } else if (!std::filesystem::exists(objdata)) {
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

  auto objpath = source->get_data_path();
  ceph_assert(std::filesystem::exists(objpath));

  std::string error;
  int ret = bl.pread_file(objpath.c_str(), ofs, len, &error);
  if (ret < 0) {
    lsfs_dout(dpp, 10) << "failed to read object from file " << objpath
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

  auto objpath = source->get_data_path();
  ceph_assert(std::filesystem::exists(objpath));

  // TODO chunk the read
  bufferlist bl;
  std::string error;
  int ret = bl.pread_file(objpath.c_str(), ofs, len, &error);
  if (ret < 0) {
    lsfs_dout(dpp, 10) << "failed to read object from file " << objpath
                       << ". Returning EIO." << dendl;
    return -EIO;
  }

  cb->handle_data(bl, ofs, len);
  return 0;
}

SFSObject::SFSDeleteOp::SFSDeleteOp(SFSObject* _source, BucketMgrRef _mgr)
    : source(_source), mgr(_mgr) {}

int SFSObject::SFSDeleteOp::delete_obj(
    const DoutPrefixProvider* dpp, optional_yield y
) {
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", object: " << source->get_name() << dendl;

  mgr->remove_object(source);

  auto objpath = source->get_data_path();
  auto metapath = source->get_metadata_path();

  if (!std::filesystem::exists(objpath)) {
    return 0;  // no-op; succeed.
  }
  std::filesystem::remove(metapath);
  std::filesystem::remove(objpath);
  return 0;
}

int SFSObject::delete_object(
    const DoutPrefixProvider* dpp, optional_yield y, bool prevent_versioning
) {
  lsfs_dout(dpp, 10) << "prevent_versioning: " << prevent_versioning << dendl;
  auto mgr = store->get_bucket_mgr(get_bucket()->get_name());
  SFSObject::SFSDeleteOp del(this, mgr);
  return del.delete_obj(dpp, y);
}

int SFSObject::copy_object(
    User* user, req_info* info, const rgw_zone_id& source_zone,
    rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
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
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
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
  auto mgr = store->get_bucket_mgr(bucket->get_name());
  return std::make_unique<SFSObject::SFSDeleteOp>(this, mgr);
}

void SFSObject::write_meta() {
  auto metapath = get_metadata_path();

  ofstream ofs(metapath);
  JSONFormatter f(true);
  f.open_object_section("meta");
  encode_json("meta", meta, &f);
  f.close_section();  // meta
  f.flush(ofs);
  ofs.close();
}

void SFSObject::load_meta() {
  auto metapath = get_metadata_path();

  ldout(store->ctx(), 10) << "load metadata for " << get_name() << dendl;

  if (!std::filesystem::exists(metapath)) {
    ldout(store->ctx(), 10) << "unable to find meta for object " << get_name()
                            << " at " << metapath << dendl;
    return;
  }

  JSONParser parser;
  bool res = parser.parse(metapath.c_str());
  ceph_assert(res);

  auto it = parser.find("meta");
  ceph_assert(!it.end());

  JSONDecoder::decode_json("meta", meta, &parser);

  set_obj_size(meta.size);
  set_attrs(meta.attrs);
  state.mtime = meta.mtime;
}

std::filesystem::path SFSObject::get_data_path() {
  return store->object_path(get_bucket()->get_key(), get_key());
}
std::filesystem::path SFSObject::get_metadata_path() {
  return store->object_metadata_path(get_bucket()->get_key(), get_key());
};

}  // namespace rgw::sal
