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
#ifndef RGW_STORE_SFS_BUCKET_H
#define RGW_STORE_SFS_BUCKET_H

#include <filesystem>
#include <memory>
#include <string>

#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "driver/sfs/bucket_mgr.h"
#include "driver/sfs/multipart.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"

namespace rgw::sal {

class SFStore;
class SFSObject;

class SFSBucket : public StoreBucket {
 private:
  SFStore* store;
  BucketMgrRef mgr;
  const std::filesystem::path path;
  RGWAccessControlPolicy acls;

 protected:
  class Meta {
   public:
    RGWBucketInfo info;
    std::set<std::string> multipart;

    void dump(ceph::Formatter* f) const {
      f->open_object_section("info");
      info.dump(f);
      f->close_section();  // info
      // encode_json("multipart", multipart, f);
    }

    void decode_json(JSONObj* obj) {
      JSONDecoder::decode_json("info", info, obj);
      // JSONDecoder::decode_json("multipart", multipart, obj);
    }
  };

  SFSBucket(const SFSBucket&) = default;

  void write_meta(const DoutPrefixProvider* dpp);
  // void read_meta(const DoutPrefixProvider *dpp);
  // void add_multipart(const std::string &oid, const std::string &meta);
  // void remove_multipart(const std::string &oid, const std::string &meta);

 public:
  SFSBucket(
      const std::filesystem::path& _path, SFStore* _store, BucketMgrRef _mgr
  );
  SFSBucket& operator=(const SFSBucket&) = delete;

  void init(const DoutPrefixProvider* dpp, const rgw_bucket& b);

  std::filesystem::path bucket_path() const;
  std::filesystem::path bucket_metadata_path() const;
  std::filesystem::path objects_path() const;

  virtual std::unique_ptr<Bucket> clone() override {
    return std::unique_ptr<Bucket>(new SFSBucket{*this});
  }

  /**
   * Obtain an object by its key.
   */
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
  /**
   * List existing objects in this bucket.
   */
  virtual int list(
      const DoutPrefixProvider* dpp, ListParams&, int, ListResults&,
      optional_yield y
  ) override;
  /**
   * Remove this bucket.
   */
  virtual int remove_bucket(
      const DoutPrefixProvider* dpp, bool delete_children,
      bool forward_to_master, req_info* req_info, optional_yield y
  ) override;
  virtual int remove_bucket_bypass_gc(
      int concurrent_max, bool keep_index_consistent, optional_yield y,
      const DoutPrefixProvider* dpp
  ) override;
  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }

  virtual int set_acl(
      const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl,
      optional_yield y
  ) override {
    acls = acl;
    return 0;
  }
  /**
   * Load this bucket from disk.
   */
  virtual int load_bucket(
      const DoutPrefixProvider* dpp, optional_yield y, bool get_stats = false
  ) override;
  virtual int read_stats(
      const DoutPrefixProvider* dpp,
      const bucket_index_layout_generation& idx_layout, int shard_id,
      std::string* bucket_ver, std::string* master_ver,
      std::map<RGWObjCategory, RGWStorageStats>& stats,
      std::string* max_marker = nullptr, bool* syncstopped = nullptr
  ) override;
  virtual int read_stats_async(
      const DoutPrefixProvider* dpp,
      const bucket_index_layout_generation& idx_layout, int shard_id,
      RGWGetBucketStats_CB* ctx
  ) override;
  virtual int sync_user_stats(const DoutPrefixProvider* dpp, optional_yield y)
      override;
  virtual int update_container_stats(const DoutPrefixProvider* dpp) override;
  virtual int check_bucket_shards(const DoutPrefixProvider* dpp) override;
  virtual int chown(
      const DoutPrefixProvider* dpp, User& new_user, optional_yield y
  ) override;
  virtual int put_info(
      const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime
  ) override;
  virtual bool is_owner(User* user) override;
  virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y)
      override;
  virtual int check_quota(
      const DoutPrefixProvider* dpp, RGWQuota& quota, uint64_t obj_size,
      optional_yield y, bool check_size_only = false
  ) override;
  virtual int merge_and_store_attrs(
      const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y
  ) override;
  virtual int try_refresh_info(
      const DoutPrefixProvider* dpp, ceph::real_time* pmtime
  ) override;
  virtual int read_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry>& usage
  ) override;
  virtual int trim_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch
  ) override;
  virtual int rebuild_index(const DoutPrefixProvider* dpp) override;

  /**
   * Obtain multipart upload, which may or may not previously exist.
   */
  virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
      const std::string& oid,
      std::optional<std::string> upload_id = std::nullopt, ACLOwner owner = {},
      ceph::real_time mtime = real_clock::now()
  ) override;
  /**
   * List multipart uploads. These are on-going and unfinished.
   */
  virtual int list_multiparts(
      const DoutPrefixProvider* dpp, const std::string& prefix,
      std::string& marker, const std::string& delim, const int& max_uploads,
      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
      std::map<std::string, bool>* common_prefixes, bool* is_truncated
  ) override;
  /**
   * Abort all multipart uploads.
   */
  virtual int abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct)
      override;

  // maybe removed from api..
  virtual int remove_objs_from_index(
      const DoutPrefixProvider* dpp,
      std::list<rgw_obj_index_key>& objs_to_unlink
  ) override {
    return -ENOTSUP;
  }
  virtual int check_index(
      const DoutPrefixProvider* dpp,
      std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
      std::map<RGWObjCategory, RGWStorageStats>& calculated_stats
  ) override {
    return -ENOTSUP;
  }
  virtual int set_tag_timeout(const DoutPrefixProvider* dpp, uint64_t timeout)
      override {
    return 0;
  }
  virtual int purge_instance(const DoutPrefixProvider* dpp) override {
    return -ENOTSUP;
  }

  void mark_multipart_complete(
      const DoutPrefixProvider* dpp, const std::string& oid,
      const std::string& meta
  );
  inline std::string get_cls_name() { return "bucket"; }
};

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_BUCKET_H
