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
#include "multipart.h"

#include "rgw_sal_sfs.h"
#include "writer.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SFSMultipartObject::SFSMultipartObject(
    CephContext* cct, const std::string& _oid, const std::string& _upload_id
) {
  ldout(cct, 10) << " has upload_id: " << _upload_id << dendl;
  init(cct, _oid, _upload_id);
}

SFSMultipartObject::SFSMultipartObject(
    CephContext* cct, const std::string& _oid,
    const std::optional<std::string> _upload_id
) {
  std::string upid = (_upload_id.has_value() ? _upload_id.value() : "");
  init(cct, _oid, upid);
}

std::string SFSMultipartObject::gen_upload_id() {
  auto now = ceph::real_clock::now();
  return ceph::to_iso_8601_no_separators(now, ceph::iso_8601_format::YMDhms);
}

void SFSMultipartObject::init(
    CephContext* _cct, std::string _oid, std::string _upload_id
) {
  boost::trim(_upload_id);
  if (_upload_id.empty()) {
    _upload_id = gen_upload_id();
  }
  ldout(_cct, 10) << "multipart_object::init: upload_id: [" << _upload_id << "]"
                  << dendl;
  oid = _oid;
  upload_id = _upload_id;
  meta = "_meta." + oid + "." + upload_id;
}

std::unique_ptr<rgw::sal::Object> SFSMultipartUpload::get_meta_obj() {
  return bucket->get_object(
      rgw_obj_key(get_meta(), string(), RGW_OBJ_NS_MULTIPART)
  );
}

int SFSMultipartUpload::write_metadata(
    const DoutPrefixProvider* dpp, SFSMultipartMeta& metadata
) {
  return 0;
}

int SFSMultipartUpload::init(
    const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner,
    rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs
) {
  lsfs_dout(dpp, 10) << "owner: " << owner.get_display_name()
                     << ", attrs: " << attrs << dendl;
  lsfs_dout(dpp, 10) << "objid: " << get_key()
                     << ", upload_id: " << get_upload_id()
                     << ", meta: " << get_meta() << dendl;

  SFSMultipartMeta metadata(
      owner, attrs, dest_placement, RGWObjCategory::MultiMeta,
      PUT_OBJ_CREATE_EXCL,  // this comes from rgw_rados.h, we should change it.
      mtime
  );
  write_metadata(dpp, metadata);

  lsfs_dout(dpp, 10) << "return" << dendl;
  return 0;
}

int SFSMultipartUpload::list_parts(
    const DoutPrefixProvider* dpp, CephContext* cct, int num_parts, int marker,
    int* next_marker, bool* truncated, bool assume_unsorted
) {
  lsfs_dout(dpp, 10) << "return" << dendl;
  return 0;
}

int SFSMultipartUpload::abort(const DoutPrefixProvider* dpp, CephContext* cct) {
  lsfs_dout(dpp, 10) << "return" << dendl;
  return 0;
}

int SFSMultipartUpload::complete(
    const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
    std::map<int, std::string>& part_etags,
    std::list<rgw_obj_index_key>& remove_objs, uint64_t& accounted_size,
    bool& compressed, RGWCompressionInfo& cs_info, off_t& ofs, std::string& tag,
    ACLOwner& owner, uint64_t olh_epoch, rgw::sal::Object* target_obj
) {
  lsfs_dout(dpp, 10) << "return" << dendl;
  return 0;
}

int SFSMultipartUpload::get_info(
    const DoutPrefixProvider* dpp, optional_yield y, rgw_placement_rule** rule,
    rgw::sal::Attrs* attrs
) {
  lsfs_dout(dpp, 10) << "return" << dendl;
  return 0;
}

std::unique_ptr<Writer> SFSMultipartUpload::get_writer(
    const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Object* head_obj,
    const rgw_user& _owner, const rgw_placement_rule* ptail_placement_rule,
    uint64_t part_num, const std::string& part_num_str
) {
  lsfs_dout(dpp, 10) << "head obj: " << head_obj << ", owner: " << _owner
                     << ", part num: " << part_num << dendl;

  return std::make_unique<SFSMultipartWriter>(
      dpp, y, this, head_obj, store, _owner, ptail_placement_rule, part_num,
      part_num_str
  );
}

void SFSMultipartUpload::dump(Formatter* f) const {
  // TODO
}

}  // namespace rgw::sal
