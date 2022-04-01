// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "rgw_sal_simplefile.h"
#include "driver/simplefile/object.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SimpleFileObject::SimpleFileReadOp::SimpleFileReadOp(SimpleFileObject *_source,
                                                     RGWObjectCtx *_rctx)
    : source(_source), rctx(_rctx) {}

int SimpleFileObject::SimpleFileReadOp::prepare(optional_yield y,
                                                const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::SimpleFileReadOp::get_attr(const DoutPrefixProvider *dpp,
                                                 const char *name,
                                                 bufferlist &dest,
                                                 optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO: " << name << dendl;
  return -ENOTSUP;
}

// sync read
int SimpleFileObject::SimpleFileReadOp::read(int64_t ofs, int64_t end,
                                             bufferlist &bl, optional_yield y,
                                             const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

// async read
int SimpleFileObject::SimpleFileReadOp::iterate(const DoutPrefixProvider *dpp,
                                                int64_t ofs, int64_t end,
                                                RGWGetDataCB *cb,
                                                optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

SimpleFileObject::SimpleFileDeleteOp::SimpleFileDeleteOp(
    SimpleFileObject *_source)
    : source(_source) {}

int SimpleFileObject::SimpleFileDeleteOp::delete_obj(
    const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_object(const DoutPrefixProvider *dpp,
                                    optional_yield y, bool prevent_versioning) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::copy_object(
    User *user, req_info *info, const rgw_zone_id &source_zone,
    rgw::sal::Object *dest_object, rgw::sal::Bucket *dest_bucket,
    rgw::sal::Bucket *src_bucket, const rgw_placement_rule &dest_placement,
    ceph::real_time *src_mtime, ceph::real_time *mtime,
    const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
    bool high_precision_time, const char *if_match, const char *if_nomatch,
    AttrsMod attrs_mod, bool copy_if_newer, Attrs &attrs,
    RGWObjCategory category, uint64_t olh_epoch,
    boost::optional<ceph::real_time> delete_at, std::string *version_id,
    std::string *tag, std::string *etag, void (*progress_cb)(off_t, void *),
    void *progress_data, const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

/** TODO Create a randomized instance ID for this object */
void SimpleFileObject::gen_rand_obj_instance_name() {
  store.ceph_context();
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileObject::get_obj_attrs(optional_yield y,
                                    const DoutPrefixProvider *dpp,
                                    rgw_obj *target_obj) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileObject::modify_obj_attrs(const char *attr_name,
                                       bufferlist &attr_val, optional_yield y,
                                       const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_obj_attrs(const DoutPrefixProvider *dpp,
                                       const char *attr_name,
                                       optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<MPSerializer> SimpleFileObject::get_serializer(
  const DoutPrefixProvider *dpp, const std::string &lock_name) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

int SimpleFileObject::transition(Bucket *bucket,
                                 const rgw_placement_rule &placement_rule,
                                 const real_time &mtime, uint64_t olh_epoch,
                                 const DoutPrefixProvider *dpp,
                                 optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::transition_to_cloud(
  Bucket* bucket,
  rgw::sal::PlacementTier* tier,
  rgw_bucket_dir_entry& o,
  std::set<std::string>& cloud_targets,
  CephContext* cct,
  bool update_object,
  const DoutPrefixProvider* dpp,
  optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": not supported" << dendl;
  return -ENOTSUP;
}

bool SimpleFileObject::placement_rules_match(rgw_placement_rule &r1,
                                             rgw_placement_rule &r2) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}

int SimpleFileObject::dump_obj_layout(const DoutPrefixProvider *dpp,
                                      optional_yield y, Formatter *f) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::swift_versioning_restore(bool &restored, /* out */
                                               const DoutPrefixProvider *dpp) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::swift_versioning_copy(const DoutPrefixProvider *dpp,
                                            optional_yield y) {
  return -ENOTSUP;
}

int SimpleFileObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
                                            const std::string &oid,
                                            const std::set<std::string> &keys,
                                            Attrs *vals) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::omap_set_val_by_key(const DoutPrefixProvider *dpp,
                                          const std::string &key,
                                          bufferlist &val, bool must_exist,
                                          optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::chown(rgw::sal::User &new_user,
                            const DoutPrefixProvider *dpp,
                            optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

} // ns rgw::sal
