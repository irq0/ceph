// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

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
 *
 */
#ifndef RGW_STORE_SFS_H
#define RGW_STORE_SFS_H

#include <filesystem>

#include "driver/sfs/bucket.h"
#include "driver/sfs/object.h"
#include "driver/sfs/sqlite/dbconn.h"
#include "driver/sfs/sqlite/sqlite_buckets.h"
#include "driver/sfs/sqlite/sqlite_users.h"
#include "driver/sfs/user.h"
#include "driver/sfs/zone.h"
#include "rgw_multi.h"
#include "rgw_notify.h"
#include "rgw_oidc_provider.h"
#include "rgw_putobj_processor.h"
#include "rgw_rados.h"
#include "rgw_role.h"
#include "rgw_sal.h"

#define lsfs_dout(_dpp, _lvl)                                               \
  ldpp_dout(_dpp, _lvl) << "> " << this->get_cls_name() << "::" << __func__ \
                        << " "

namespace rgw::sal {

class SFStore;

class UnsupportedLuaManager : public StoreLuaManager {
 public:
  UnsupportedLuaManager() = default;
  UnsupportedLuaManager(const UnsupportedLuaManager&) = delete;
  UnsupportedLuaManager& operator=(const UnsupportedLuaManager&) = delete;
  virtual ~UnsupportedLuaManager() = default;

  virtual int get_script(
      const DoutPrefixProvider* dpp, optional_yield y, const std::string& key,
      std::string& script
  ) override {
    return -ENOENT;
  }
  virtual int put_script(
      const DoutPrefixProvider* dpp, optional_yield y, const std::string& key,
      const std::string& script
  ) override {
    return -ENOENT;
  }
  virtual int del_script(
      const DoutPrefixProvider* dpp, optional_yield y, const std::string& key
  ) override {
    return -ENOENT;
  }
  virtual int add_package(
      const DoutPrefixProvider* dpp, optional_yield y,
      const std::string& package_name
  ) override {
    return -ENOENT;
  }
  virtual int remove_package(
      const DoutPrefixProvider* dpp, optional_yield y,
      const std::string& package_name
  ) override {
    return -ENOENT;
  }
  virtual int list_packages(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::lua::packages_t& packages
  ) {
    return -ENOENT;
  }
};

class SFStore : public StoreDriver {
 private:
  RGWSyncModuleInstanceRef sync_module;
  SFSZone zone;
  const std::filesystem::path data_path;
  CephContext* const cctx;
  ceph::mutex buckets_map_lock = ceph::make_mutex("buckets_map_lock");
  std::map<std::string, sfs::BucketRef> buckets;

 public:
  sfs::sqlite::DBConnRef db_conn;

  SFStore(CephContext* c, const std::filesystem::path& data_path);
  SFStore(const SFStore&) = delete;
  SFStore& operator=(const SFStore&) = delete;
  ~SFStore();

  virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp)
      override;
  virtual void finalize(void) override;
  void maybe_init_store();
  void init_buckets();

  virtual const std::string get_name() const override { return "sfs"; }

  virtual std::string get_cluster_id(
      const DoutPrefixProvider* dpp, optional_yield y
  ) override {
    return "NA";
  }
  virtual bool is_meta_master() override { return true; }
  /**
   * Obtain an internal representation of an object by its key. This does not
   * directly specify the bucket, so there's no direct mapping to an actual
   * object (existing or not).
   *
   * The SAL layer will often call this function during its operation, setting
   * the bucket accordingly at some point.
   */
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) {
    return std::make_unique<SFSObject>(this, k);
  }

  virtual RGWCoroutinesManagerRegistry* get_cr_registry() override {
    return nullptr;
  }
  virtual CephContext* ctx(void) override { return cctx; };
  CephContext* ceph_context() const { return cctx; };

  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual int get_user_by_access_key(
      const DoutPrefixProvider* dpp, const std::string& key, optional_yield y,
      std::unique_ptr<User>* user
  ) override;
  virtual int get_user_by_email(
      const DoutPrefixProvider* dpp, const std::string& email, optional_yield y,
      std::unique_ptr<User>* user
  ) override;
  virtual int get_user_by_swift(
      const DoutPrefixProvider* dpp, const std::string& user_str,
      optional_yield y, std::unique_ptr<User>* user
  ) override;
  /* Obtain bucket. */
  virtual int get_bucket(
      User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket
  ) override;
  virtual int get_bucket(
      const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b,
      std::unique_ptr<Bucket>* bucket, optional_yield y
  ) override;
  virtual int get_bucket(
      const DoutPrefixProvider* dpp, User* u, const std::string& tenant,
      const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y
  ) override;
  virtual int forward_request_to_master(
      const DoutPrefixProvider* dpp, User* user, obj_version* objv,
      bufferlist& in_data, JSONParser* jp, req_info& info, optional_yield y
  ) override;
  virtual int forward_iam_request_to_master(
      const DoutPrefixProvider* dpp, const RGWAccessKey& key, obj_version* objv,
      bufferlist& in_data, RGWXMLDecoder::XMLParser* parser, req_info& info,
      optional_yield y
  ) override;
  virtual Zone* get_zone() override { return &zone; }
  virtual std::string zone_unique_id(uint64_t unique_num) override;
  virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
  virtual int get_zonegroup(
      const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup
  ) override;
  virtual int list_all_zones(
      const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids
  ) override;
  virtual int cluster_stat(RGWClusterStat& stats) override;
  virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
  virtual RGWLC* get_rgwlc(void) override;
  virtual std::unique_ptr<Notification> get_notification(
      rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s,
      rgw::notify::EventType event_type, optional_yield y,
      const std::string* object_name = nullptr
  ) override;
  virtual std::unique_ptr<Notification> get_notification(
      const DoutPrefixProvider* dpp, rgw::sal::Object* obj,
      rgw::sal::Object* src_obj, rgw::notify::EventType event_type,
      rgw::sal::Bucket* _bucket, std::string& _user_id,
      std::string& _user_tenant, std::string& _req_id, optional_yield y
  ) override;

  /** Log usage data to the store.  Usage data is things like bytes
   * sent/received and op count */
  virtual int log_usage(
      const DoutPrefixProvider* dpp,
      std::map<rgw_user_bucket, RGWUsageBatch>& usage_info
  ) override;

  /** Log OP data to the store.  Data is opaque to SAL */
  virtual int log_op(
      const DoutPrefixProvider* dpp, std::string& oid, bufferlist& bl
  ) override;

  virtual int register_to_service_map(
      const DoutPrefixProvider* dpp, const std::string& daemon_type,
      const std::map<std::string, std::string>& meta
  ) override;

  virtual void get_quota(RGWQuota& quota) override;

  virtual void get_ratelimit(
      RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit,
      RGWRateLimitInfo& anon_ratelimit
  ) override;

  virtual int set_buckets_enabled(
      const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets,
      bool enabled
  ) override;

  virtual uint64_t get_new_req_id() override { return 0; }
  virtual int get_sync_policy_handler(
      const DoutPrefixProvider* dpp, std::optional<rgw_zone_id> zone,
      std::optional<rgw_bucket> bucket, RGWBucketSyncPolicyHandlerRef* phandler,
      optional_yield y
  ) override;

  /** Get a status manager for bucket sync */
  virtual RGWDataSyncStatusManager* get_data_sync_manager(
      const rgw_zone_id& source_zone
  ) override;

  virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override;
  virtual void wakeup_data_sync_shards(
      const DoutPrefixProvider* dpp, const rgw_zone_id& source_zone,
      boost::container::flat_map<
          int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids
  ) override;
  virtual int clear_usage(const DoutPrefixProvider* dpp) override { return 0; }
  virtual int read_all_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry>& usage
  ) override;
  virtual int trim_all_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch
  ) override;
  virtual int get_config_key_val(std::string name, bufferlist* bl) override;
  virtual int meta_list_keys_init(
      const DoutPrefixProvider* dpp, const std::string& section,
      const std::string& marker, void** phandle
  ) override;
  virtual int meta_list_keys_next(
      const DoutPrefixProvider* dpp, void* handle, int max,
      std::list<std::string>& keys, bool* truncated
  ) override;
  virtual void meta_list_keys_complete(void* handle) override;
  virtual std::string meta_get_marker(void* handle) override;
  virtual int meta_remove(
      const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y
  ) override;

  virtual const RGWSyncModuleInstanceRef& get_sync_module() override;

  virtual std::string get_host_id() override;

  virtual std::unique_ptr<LuaManager> get_lua_manager() override;

  virtual std::unique_ptr<RGWRole> get_role(
      std::string name, std::string tenant, std::string path = "",
      std::string trust_policy = "", std::string max_session_duration_str = "",
      std::multimap<std::string, std::string> tags = {}
  ) override;

  virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
  virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) override;
  virtual int get_roles(
      const DoutPrefixProvider* dpp, optional_yield y,
      const std::string& path_prefix, const std::string& tenant,
      std::vector<std::unique_ptr<RGWRole>>& roles
  ) override;

  virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;

  virtual int get_oidc_providers(
      const DoutPrefixProvider* dpp, const std::string& tenant,
      std::vector<std::unique_ptr<RGWOIDCProvider>>& providers
  ) override {
    return 0;
  }

  /**
   * Unsure what this does.
   */
  virtual std::unique_ptr<Writer> get_append_writer(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Object* _head_obj, const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule,
      const std::string& unique_tag, uint64_t position,
      uint64_t* cur_accounted_size
  ) override;

  /**
   * Obtain a writer to write an object in one go. The writer may be called
   * several times by us, but should become available to the user as a whole
   * once finished, and will not be a multipart upload. This is usually for
   * smaller objects (up to ~10MiB).
   */
  virtual std::unique_ptr<Writer> get_atomic_writer(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Object* _head_obj, const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule, uint64_t olh_epoch,
      const std::string& unique_tag
  ) override;

  virtual const std::string& get_compression_type(const rgw_placement_rule& rule
  ) override {
    return zone.get_params().get_compression_type(rule);
  }
  virtual bool valid_placement(const rgw_placement_rule& rule) override {
    return zone.get_params().valid_placement(rule);
  }

  virtual void register_admin_apis(RGWRESTMgr* mgr) override;

  std::filesystem::path get_data_path() const { return data_path; }

  /**
   * Returns path to meta directory.
   */
  std::filesystem::path meta_path() const { return data_path / "meta"; }
  /**
   * Returns path to buckets directory, where buckets are kept as directories.
   */
  std::filesystem::path buckets_path() const { return data_path / "buckets"; }
  /**
   * Returns path to users directory.
   */
  std::filesystem::path users_path() const { return data_path / "users"; }
  /**
   * Returns path to a specific bucket's directory.
   */
  std::filesystem::path bucket_path(const rgw_bucket& bucket) const {
    return buckets_path() / bucket.name;
  }
  /**
   * Returns path to a specific bucket's directory.
   */
  std::filesystem::path bucket_path(const std::string& bucket) const {
    return buckets_path() / bucket;
  }
  /**
   * Returns path to a specific bucket's metadata file.
   */
  std::filesystem::path bucket_metadata_path(const rgw_bucket& bucket) const {
    return bucket_path(bucket) / "_meta.json";
  }
  /**
   * Returns path to a bucket's objects directory.
   */
  std::filesystem::path objects_path(const rgw_bucket& bucket) const {
    return bucket_path(bucket) / "objects";
  }

  /**
   * Returns the path for a given object's data location.
   */
  std::filesystem::path object_path(
      const rgw_bucket& bucket, const std::string& obj
  ) const {
    return objects_path(bucket) / obj;
  }

  /**
   * Returns the path for a given object's data location.
   */
  std::filesystem::path object_path(
      const rgw_bucket& bucket, const rgw_obj_key& obj
  ) const {
    return object_path(bucket, hash_rgw_obj_key(obj.name));
  }

  /**
   * Returns the path for a given object's metadata file.
   */
  std::filesystem::path object_metadata_path(
      const rgw_bucket& bucket, const rgw_obj_key& obj
  ) const {
    const std::string metafn = "_meta." + hash_rgw_obj_key(obj.name);
    return object_path(bucket, metafn);
  }

  inline std::string hash_rgw_obj_key(const rgw_obj_key& obj) const {
    const std::string_view in{obj.name};
    const auto hash = calc_hash_sha256(in);
    return hash.to_str();
  }

  bool _bucket_exists(const std::string& name) {
    const auto it = buckets.find(name);
    return it != buckets.cend();
  }

  bool bucket_exists(const rgw_bucket& bucket) {
    std::lock_guard l(buckets_map_lock);
    return _bucket_exists(bucket.name);
  }

  sfs::BucketRef bucket_create(
      const rgw_bucket& bucket, const RGWUserInfo& owner
  ) {
    std::lock_guard l(buckets_map_lock);
    if (_bucket_exists(bucket.name)) {
      return nullptr;
    }

    sfs::sqlite::DBOPBucketInfo info;
    info.binfo.bucket = bucket;
    info.binfo.owner = owner.user_id;
    info.binfo.creation_time = ceph::real_clock::now();
    info.binfo.placement_rule.name = "default";
    info.binfo.placement_rule.storage_class = "STANDARD";

    auto meta_buckets = sfs::get_meta_buckets(db_conn);
    meta_buckets->store_bucket(info);

    sfs::BucketRef b =
        std::make_shared<sfs::Bucket>(ctx(), this, bucket, owner);
    buckets[bucket.name] = b;
    return b;
  }

  void _refresh_buckets_safe() {
    std::lock_guard l(buckets_map_lock);
    _refresh_buckets();
  }

  void _refresh_buckets() {
    auto meta_buckets = sfs::get_meta_buckets(db_conn);
    auto existing = meta_buckets->get_buckets();
    buckets.clear();
    sfs::sqlite::SQLiteUsers users(db_conn);
    for (auto& b : existing) {
      auto user = users.get_user(b.binfo.owner.id);
      sfs::BucketRef ref = std::make_shared<sfs::Bucket>(
          ctx(), this, b.binfo.bucket, user->uinfo
      );
      buckets[b.binfo.bucket.name] = ref;
    }
  }

  std::list<sfs::BucketRef> bucket_list() {
    _refresh_buckets_safe();

    std::list<sfs::BucketRef> lst;
    std::lock_guard l(buckets_map_lock);
    for (const auto& [name, bucketref] : buckets) {
      lst.push_back(bucketref);
    }
    return lst;
  }

  sfs::BucketRef get_bucket_ref(const std::string& name) {
    std::lock_guard l(buckets_map_lock);
    auto it = buckets.find(name);
    if (it == buckets.end()) {
      return nullptr;
    }
    return it->second;
  }

  std::string get_cls_name() const { return "sfstore"; }
};

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_H
