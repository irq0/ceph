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
#ifndef RGW_STORE_SFS_OBJECT_H
#define RGW_STORE_SFS_OBJECT_H

#include <filesystem>

#include "rgw/driver/sfs/bucket.h"
#include "rgw/driver/sfs/types.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"
#include "uuid_path.h"

namespace rgw::sal {

class SFStore;

class SFSObject : public StoreObject {
 private:
  SFStore* store;
  RGWAccessControlPolicy acls;
  sfs::BucketRef bucketref;
  sfs::ObjectRef objref;

 protected:
  SFSObject(SFSObject&) = default;

 public:
  /**
   * reads an object's contents.
   */
  struct SFSReadOp : public ReadOp {
   private:
    SFSObject* source;
    sfs::ObjectRef objref;
    std::filesystem::path objdata;

   public:
    SFSReadOp(SFSObject* _source);

    virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp)
        override;
    virtual int read(
        int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
        const DoutPrefixProvider* dpp
    ) override;
    virtual int iterate(
        const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
        RGWGetDataCB* cb, optional_yield y
    ) override;
    virtual int get_attr(
        const DoutPrefixProvider* dpp, const char* name, bufferlist& dest,
        optional_yield y
    ) override;

    const std::string get_cls_name() { return "object_read"; }
  };

  /**
   * deletes an object.
   */
  struct SFSDeleteOp : public DeleteOp {
   private:
    SFSObject* source;
    sfs::BucketRef bucketref;

   public:
    SFSDeleteOp(SFSObject* _source, sfs::BucketRef _bucketref);
    virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
        override;

    const std::string get_cls_name() { return "object_delete"; }
  };

  // SFSObject continues here.
  //

  SFSObject& operator=(const SFSObject&) = delete;

  SFSObject(SFStore* _st, const rgw_obj_key& _k)
      : StoreObject(_k), store(_st) {}
  SFSObject(
      SFStore* _st, const rgw_obj_key& _k, Bucket* _b, sfs::BucketRef _bucket,
      bool load_metadata = true
  )
      : StoreObject(_k, _b), store(_st), bucketref(_bucket) {
    if (load_metadata) {
      refresh_meta();
    }
  }

  virtual std::unique_ptr<Object> clone() override {
    return std::unique_ptr<Object>(new SFSObject{*this});
  }

  virtual int delete_object(
      const DoutPrefixProvider* dpp, optional_yield y,
      bool prevent_versioning = false
  ) override;
  virtual int copy_object(
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
  ) override;

  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
  virtual int set_acl(const RGWAccessControlPolicy& acl) override {
    acls = acl;
    return 0;
  }

  virtual int get_obj_attrs(
      optional_yield y, const DoutPrefixProvider* dpp,
      rgw_obj* target_obj = NULL
  ) override;
  virtual int modify_obj_attrs(
      const char* attr_name, bufferlist& attr_val, optional_yield y,
      const DoutPrefixProvider* dpp
  ) override;
  virtual int delete_obj_attrs(
      const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y
  ) override;
  virtual bool is_expired() override { return false; }
  virtual void gen_rand_obj_instance_name() override;
  virtual std::unique_ptr<MPSerializer> get_serializer(
      const DoutPrefixProvider* dpp, const std::string& lock_name
  ) override;

  virtual int transition(
      Bucket* bucket, const rgw_placement_rule& placement_rule,
      const real_time& mtime, uint64_t olh_epoch, const DoutPrefixProvider* dpp,
      optional_yield y
  ) override;
  /** Move an object to the cloud */
  virtual int transition_to_cloud(
      Bucket* bucket, rgw::sal::PlacementTier* tier, rgw_bucket_dir_entry& o,
      std::set<std::string>& cloud_targets, CephContext* cct,
      bool update_object, const DoutPrefixProvider* dpp, optional_yield y
  );

  virtual bool placement_rules_match(
      rgw_placement_rule& r1, rgw_placement_rule& r2
  ) override;
  virtual int dump_obj_layout(
      const DoutPrefixProvider* dpp, optional_yield y, Formatter* f
  ) override;
  virtual int swift_versioning_restore(
      bool& restored, /* out */
      const DoutPrefixProvider* dpp
  ) override;
  virtual int swift_versioning_copy(
      const DoutPrefixProvider* dpp, optional_yield y
  ) override;

  /**
   * Obtain a Read Operation.
   */
  virtual std::unique_ptr<ReadOp> get_read_op() override {
    return std::make_unique<SFSObject::SFSReadOp>(this);
  }
  /**
   * Obtain a Delete Operation.
   */
  virtual std::unique_ptr<DeleteOp> get_delete_op() override;

  virtual int omap_get_vals_by_keys(
      const DoutPrefixProvider* dpp, const std::string& oid,
      const std::set<std::string>& keys, Attrs* vals
  ) override;
  virtual int omap_set_val_by_key(
      const DoutPrefixProvider* dpp, const std::string& key, bufferlist& val,
      bool must_exist, optional_yield y
  ) override;
  virtual int chown(
      rgw::sal::User& new_user, const DoutPrefixProvider* dpp, optional_yield y
  ) override;
  // will be removed in the future..
  virtual int get_obj_state(
      const DoutPrefixProvider* dpp, RGWObjState** _state, optional_yield y,
      bool follow_olh = true
  ) override {
    *_state = &state;
    return 0;
  }
  virtual int set_obj_attrs(
      const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs,
      optional_yield y
  ) override {
    return 0;
  }

  sfs::ObjectRef get_object_ref() { return objref; }

  void refresh_meta();

  const std::string get_cls_name() { return "object"; }
};

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_OBJECT_H
