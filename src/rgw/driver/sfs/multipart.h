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
#ifndef RGW_STORE_SFS_MULTIPART_H
#define RGW_STORE_SFS_MULTIPART_H

#include "rgw_sal.h"
#include "rgw_sal_store.h"

namespace rgw::sal {

class SFStore;

class SFSMultipartMeta {
  ACLOwner owner;
  Attrs attrs;
  rgw_placement_rule dest_placement;
  RGWObjCategory category;
  int flags;
  ceph::real_time mtime;

 public:
  SFSMultipartMeta(
      ACLOwner& _owner, Attrs& _attrs, rgw_placement_rule& _dest_placement,
      RGWObjCategory _category, int _flags, ceph::real_time& _mtime
  )
      : owner(_owner),
        attrs(_attrs),
        dest_placement(_dest_placement),
        category(_category),
        flags(_flags),
        mtime(_mtime) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(owner, bl);
    encode(attrs, bl);
    encode(dest_placement, bl);
    encode(flags, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(owner, bl);
    decode(attrs, bl);
    decode(dest_placement, bl);
    decode(flags, bl);
    decode(mtime, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(SFSMultipartMeta)

class SFSMultipartObject {
  std::string oid;  // object id / name
  std::string upload_id;
  std::string meta;

  std::string gen_upload_id();
  void init(CephContext* cct, std::string _oid, std::string _upload_id);

 public:
  SFSMultipartObject(
      CephContext* cct, const std::string& _oid, const std::string& _upload_id
  );
  SFSMultipartObject(
      CephContext* cct, const std::string& _oid,
      const std::optional<std::string> _upload_id
  );

  const std::string& get_key() const { return oid; }

  const std::string& get_upload_id() const { return upload_id; }

  const std::string& get_meta() const { return meta; }

  friend inline std::ostream& operator<<(
      std::ostream& out, const SFSMultipartObject& obj
  ) {
    out << "multipart_object(oid: " << obj.get_key()
        << ", upload_id: " << obj.get_upload_id()
        << ", meta: " << obj.get_meta() << ")";
    return out;
  }
};

class SFSMultipartUpload : public StoreMultipartUpload {
  SFStore* store;
  const std::string& oid;
  std::optional<std::string> upload_id;
  SFSMultipartObject obj;
  ACLOwner owner;
  ceph::real_time mtime;

 public:
  SFSMultipartUpload(
      CephContext* _cct, SFStore* _store, Bucket* _bucket,
      const std::string& _oid, std::optional<std::string> _upload_id,
      ACLOwner _owner, ceph::real_time _mtime
  )
      : StoreMultipartUpload(_bucket),
        store(_store),
        oid(_oid),
        upload_id(_upload_id),
        obj(_cct, _oid, _upload_id),
        owner(_owner),
        mtime(_mtime) {}

  virtual ~SFSMultipartUpload() = default;

  virtual const std::string& get_meta() const { return obj.get_meta(); }
  virtual const std::string& get_key() const { return obj.get_key(); }
  virtual const std::string& get_upload_id() const {
    return obj.get_upload_id();
  }
  virtual const ACLOwner& get_owner() const override { return owner; }
  virtual ceph::real_time& get_mtime() { return mtime; }
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;
  virtual int init(
      const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner,
      rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs
  ) override;
  virtual int list_parts(
      const DoutPrefixProvider* dpp, CephContext* cct, int num_parts,
      int marker, int* next_marker, bool* truncated,
      bool assume_unsorted = false
  ) override;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct) override;
  virtual int complete(
      const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
      std::map<int, std::string>& part_etags,
      std::list<rgw_obj_index_key>& remove_objs, uint64_t& accounted_size,
      bool& compressed, RGWCompressionInfo& cs_info, off_t& ofs,
      std::string& tag, ACLOwner& owner, uint64_t olh_epoch,
      rgw::sal::Object* target_obj
  ) override;
  virtual int get_info(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr
  ) override;
  virtual std::unique_ptr<Writer> get_writer(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Object* head_obj, const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
      const std::string& part_num_str
  ) override;

  void dump(Formatter* f) const;
  int write_metadata(const DoutPrefixProvider* dpp, SFSMultipartMeta& metadata);
  inline std::string get_cls_name() { return "multipart_upload"; }
};

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_MULTIPART_H
