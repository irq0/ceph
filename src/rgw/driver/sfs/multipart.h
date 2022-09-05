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

#include "rgw/driver/sfs/bucket.h"
#include "rgw/driver/sfs/object.h"
#include "rgw/driver/sfs/types.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"

namespace rgw::sal {

class SFStore;

/* This class comes as a hack to circumvent the SAL layer's
 * requirement/expectation to for a meta object for a given multipart upload.
 * Unlike other backends, we do not rely on a meta object during our process.
 * Instead of returning an actual object, which entails meeting several
 * criteria, we choose instead to extend the SFSObject class and overriding the
 * bits that are relevant for the SAL layer's expected path.
 *
 * For reference, check 'rgw_op.cc', RGWCompleteMultipart::execute().
 */
struct SFSMultipartMetaObject : public SFSObject {
  SFSMultipartMetaObject(SFSMultipartMetaObject&) = default;
  SFSMultipartMetaObject(
      SFStore* _st, const rgw_obj_key& _k, Bucket* _b, sfs::BucketRef _bucket
  )
      : SFSObject(_st, _k, _b, _bucket, false) {}

  struct SFSMetaObjDeleteOp : public DeleteOp {
    SFSMetaObjDeleteOp() = default;
    virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
        override {
      return 0;
    }
    const std::string get_cls_name() { return "mp_meta_obj_delete"; }
  };

  virtual std::unique_ptr<Object> clone() override {
    return std::unique_ptr<Object>(new SFSMultipartMetaObject{*this});
  }
  SFSMultipartMetaObject& operator=(const SFSMultipartMetaObject&) = delete;

  virtual std::unique_ptr<DeleteOp> get_delete_op() override {
    return std::make_unique<SFSMetaObjDeleteOp>();
  }

  virtual int delete_object(
      const DoutPrefixProvider* dpp, optional_yield y, bool prevent_versioning
  ) {
    return 0;
  }
};

class SFSMultipartPart : public StoreMultipartPart {
  uint32_t partnum;
  sfs::MultipartObjectRef mpobj;

 public:
  SFSMultipartPart(uint32_t _num, sfs::MultipartObjectRef _mpobj)
      : partnum(_num), mpobj(_mpobj) {}
  virtual ~SFSMultipartPart() = default;

  virtual uint32_t get_num() override { return partnum; }

  virtual uint64_t get_size() override { return mpobj->len; }

  virtual const std::string& get_etag() override { return mpobj->etag; }

  virtual ceph::real_time& get_mtime() override { return mpobj->mtime; }
};

class SFSMultipartUpload : public StoreMultipartUpload {
  SFStore* store;
  SFSBucket* bucket;
  sfs::BucketRef bucketref;
  sfs::MultipartUploadRef mp;

 public:
  SFSMultipartUpload(
      SFStore* _store, SFSBucket* _bucket, sfs::BucketRef _bucketref,
      sfs::MultipartUploadRef _mp
  )
      : StoreMultipartUpload(_bucket),
        store(_store),
        bucket(_bucket),
        bucketref(_bucketref),
        mp(_mp) {}

  virtual ~SFSMultipartUpload() = default;

  virtual const std::string& get_meta() const { return mp->get_meta_str(); }
  virtual const std::string& get_key() const { return mp->get_obj_name(); }
  virtual const std::string& get_upload_id() const {
    return mp->get_upload_id();
  }
  virtual const ACLOwner& get_owner() const override { return mp->get_owner(); }
  virtual ceph::real_time& get_mtime() override { return mp->get_mtime(); }
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
  inline std::string get_cls_name() { return "multipart_upload"; }
};

struct SFSMultipartSerializer : public StoreMPSerializer {
  SFSMultipartSerializer() = default;

  virtual int try_lock(
      const DoutPrefixProvider* dpp, utime_t dur, optional_yield y
  ) override {
    return 0;
  }

  virtual int unlock() override { return 0; }
};

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_MULTIPART_H
