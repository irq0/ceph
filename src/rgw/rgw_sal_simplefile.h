// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2021
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <filesystem>

#include "rgw_multi.h"
#include "rgw_notify.h"
#include "rgw_oidc_provider.h"
#include "rgw_putobj_processor.h"
#include "rgw_rados.h"
#include "rgw_role.h"
#include "rgw_sal.h"
#include "rgw_sal_dbstore.h"

namespace rgw::sal {

class SimpleFileStore;

class SimpleFileUser : public User {
 private:
  const SimpleFileStore& store;

 protected:
  SimpleFileUser(SimpleFileUser&) = default;
  SimpleFileUser& operator=(const SimpleFileUser&) = default;

 public:
  SimpleFileUser(const rgw_user &_u, const SimpleFileStore& _store) : User(_u), store(_store) {}
  SimpleFileUser(const RGWUserInfo &_i, const SimpleFileStore& _store) : User(_i), store(_store) {}
  virtual ~SimpleFileUser() = default;

  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new SimpleFileUser{*this});
  }
  virtual int list_buckets(const DoutPrefixProvider *dpp,
                           const std::string &marker,
                           const std::string &end_marker, uint64_t max,
                           bool need_stats, BucketList &buckets,
                           optional_yield y) override;
  virtual int create_bucket(
      const DoutPrefixProvider *dpp, const rgw_bucket &b,
      const std::string &zonegroup_id, rgw_placement_rule &placement_rule,
      std::string &swift_ver_location, const RGWQuotaInfo *pquota_info,
      const RGWAccessControlPolicy &policy, Attrs &attrs, RGWBucketInfo &info,
      obj_version &ep_objv, bool exclusive, bool obj_lock_enabled,
      bool *existed, req_info &req_info, std::unique_ptr<Bucket> *bucket,
      optional_yield y) override;
  virtual int read_attrs(const DoutPrefixProvider *dpp,
                         optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                    Attrs &new_attrs,
                                    optional_yield y) override;
  virtual int read_stats(const DoutPrefixProvider *dpp, optional_yield y,
                         RGWStorageStats *stats,
                         ceph::real_time *last_stats_sync = nullptr,
                         ceph::real_time *last_stats_update = nullptr) override;
  virtual int read_stats_async(const DoutPrefixProvider *dpp,
                               RGWGetUserStats_CB *cb) override;
  virtual int complete_flush_stats(const DoutPrefixProvider *dpp,
                                   optional_yield y) override;
  virtual int read_usage(
      const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) override;
  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
                         uint64_t end_epoch) override;
  virtual int load_user(const DoutPrefixProvider *dpp,
                        optional_yield y) override;
  virtual int store_user(const DoutPrefixProvider *dpp, optional_yield y,
                         bool exclusive,
                         RGWUserInfo *old_info = nullptr) override;
  virtual int remove_user(const DoutPrefixProvider *dpp,
                          optional_yield y) override;
};

class SimpleFileBucket : public Bucket {
 private:
  const SimpleFileStore& store;
  const std::filesystem::path path;
  RGWAccessControlPolicy acls;
 protected:
  SimpleFileBucket(const SimpleFileBucket&) = default;

 public:
  SimpleFileBucket(const std::filesystem::path& _path, const SimpleFileStore& _store);
  SimpleFileBucket& operator=(const SimpleFileBucket&) = delete;

  const std::filesystem::path& get_path() const { return path; }

  virtual std::unique_ptr<Bucket> clone() override {
    return std::unique_ptr<Bucket>(new SimpleFileBucket{*this});
  }

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key &key) override;
  virtual int list(const DoutPrefixProvider *dpp, ListParams &, int,
                   ListResults &, optional_yield y) override;
  virtual int remove_bucket(const DoutPrefixProvider *dpp, bool delete_children,
                            bool forward_to_master, req_info *req_info,
                            optional_yield y) override;
  virtual int remove_bucket_bypass_gc(int concurrent_max,
                                      bool keep_index_consistent,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp) override;
  virtual RGWAccessControlPolicy &get_acl(void) override { return acls; }

  virtual int set_acl(const DoutPrefixProvider *dpp,
                      RGWAccessControlPolicy &acl, optional_yield y) override {
    acls = acl;
    return 0;
  }
  virtual int load_bucket(const DoutPrefixProvider *dpp, optional_yield y,
                          bool get_stats = false) override;
  virtual int read_stats(const DoutPrefixProvider *dpp, int shard_id,
                         std::string *bucket_ver, std::string *master_ver,
                         std::map<RGWObjCategory, RGWStorageStats> &stats,
                         std::string *max_marker = nullptr,
                         bool *syncstopped = nullptr) override;
  virtual int read_stats_async(const DoutPrefixProvider *dpp, int shard_id,
                               RGWGetBucketStats_CB *ctx) override;
  virtual int sync_user_stats(const DoutPrefixProvider *dpp,
                              optional_yield y) override;
  virtual int update_container_stats(const DoutPrefixProvider *dpp) override;
  virtual int check_bucket_shards(const DoutPrefixProvider *dpp) override;
  virtual int chown(const DoutPrefixProvider *dpp, User *new_user,
                    User *old_user, optional_yield y,
                    const std::string *marker = nullptr) override;
  virtual int put_info(const DoutPrefixProvider *dpp, bool exclusive,
                       ceph::real_time mtime) override;
  virtual bool is_owner(User *user) override;
  virtual int check_empty(const DoutPrefixProvider *dpp,
                          optional_yield y) override;
  virtual int check_quota(const DoutPrefixProvider *dpp,
                          RGWQuotaInfo &user_quota, RGWQuotaInfo &bucket_quota,
                          uint64_t obj_size, optional_yield y,
                          bool check_size_only = false) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                    Attrs &new_attrs,
                                    optional_yield y) override;
  virtual int try_refresh_info(const DoutPrefixProvider *dpp,
                               ceph::real_time *pmtime) override;
  virtual int read_usage(
      const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) override;
  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
                         uint64_t end_epoch) override;
  virtual int rebuild_index(const DoutPrefixProvider *dpp) override;
  virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
      const std::string &oid,
      std::optional<std::string> upload_id = std::nullopt, ACLOwner owner = {},
      ceph::real_time mtime = real_clock::now()) override;
  virtual int list_multiparts(
      const DoutPrefixProvider *dpp, const std::string &prefix,
      std::string &marker, const std::string &delim, const int &max_uploads,
      std::vector<std::unique_ptr<MultipartUpload>> &uploads,
      std::map<std::string, bool> *common_prefixes,
      bool *is_truncated) override;
  virtual int abort_multiparts(const DoutPrefixProvider *dpp,
                               CephContext *cct) override;

  // maybe removed from api..
  virtual int remove_objs_from_index(
      const DoutPrefixProvider *dpp,
      std::list<rgw_obj_index_key> &objs_to_unlink) override {
    return -ENOTSUP;
  }
  virtual int check_index(
      const DoutPrefixProvider *dpp,
      std::map<RGWObjCategory, RGWStorageStats> &existing_stats,
      std::map<RGWObjCategory, RGWStorageStats> &calculated_stats) override {
    return -ENOTSUP;
  }
  virtual int set_tag_timeout(const DoutPrefixProvider *dpp,
                              uint64_t timeout) override {
    return 0;
  }
  virtual int purge_instance(const DoutPrefixProvider *dpp) override {
    return -ENOTSUP;
  }
};

class SimpleFileObject : public Object {
 private:
  const SimpleFileStore& store;
  RGWAccessControlPolicy acls;
 protected:
  SimpleFileObject(SimpleFileObject&) = default;

 public:
  struct SimpleFileReadOp : public ReadOp {
   private:
    SimpleFileObject *source;
    RGWObjectCtx *rctx;

   public:
    SimpleFileReadOp(SimpleFileObject *_source, RGWObjectCtx *_rctx);

    virtual int prepare(optional_yield y,
                        const DoutPrefixProvider *dpp) override;
    virtual int read(int64_t ofs, int64_t end, bufferlist &bl, optional_yield y,
                     const DoutPrefixProvider *dpp) override;
    virtual int iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB *cb, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider *dpp, const char *name,
                         bufferlist &dest, optional_yield y) override;
  };
  struct SimpleFileDeleteOp : public DeleteOp {
   private:
    SimpleFileObject *source;

   public:
    SimpleFileDeleteOp(SimpleFileObject *_source);
    virtual int delete_obj(const DoutPrefixProvider *dpp,
                           optional_yield y) override;
  };
  SimpleFileObject& operator=(const SimpleFileObject&) = delete;

  SimpleFileObject(const SimpleFileStore& _st, const rgw_obj_key &_k)
      : Object(_k), store(_st) {}
  SimpleFileObject(const SimpleFileStore& _st, const rgw_obj_key &_k, Bucket *_b)
      : Object(_k, _b), store(_st) {}

  virtual std::unique_ptr<Object> clone() override {
    return std::unique_ptr<Object>(new SimpleFileObject{*this});
  }

  virtual int delete_object(const DoutPrefixProvider *dpp, optional_yield y,
                            bool prevent_versioning = false) override;
  virtual int delete_obj_aio(const DoutPrefixProvider *dpp, RGWObjState *astate,
                             Completions *aio, bool keep_index_consistent,
                             optional_yield y) override;
  virtual int copy_object(
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
      void *progress_data, const DoutPrefixProvider *dpp,
      optional_yield y) override;

  virtual RGWAccessControlPolicy &get_acl(void) override { return acls; }
  virtual int set_acl(const RGWAccessControlPolicy &acl) override {
    acls = acl;
    return 0;
  }

  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider *dpp,
                            rgw_obj *target_obj = NULL) override;
  virtual int modify_obj_attrs(const char *attr_name, bufferlist &attr_val,
                               optional_yield y,
                               const DoutPrefixProvider *dpp) override;
  virtual int delete_obj_attrs(const DoutPrefixProvider *dpp,
                               const char *attr_name,
                               optional_yield y) override;
  virtual bool is_expired() override { return false; }
  virtual void gen_rand_obj_instance_name() override;
  virtual MPSerializer *get_serializer(const DoutPrefixProvider *dpp,
                                       const std::string &lock_name) override;

  virtual int transition(Bucket *bucket,
                         const rgw_placement_rule &placement_rule,
                         const real_time &mtime, uint64_t olh_epoch,
                         const DoutPrefixProvider *dpp,
                         optional_yield y) override;
  virtual bool placement_rules_match(rgw_placement_rule &r1,
                                     rgw_placement_rule &r2) override;
  virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y,
                              Formatter *f) override;
  virtual int swift_versioning_restore(bool &restored, /* out */
                                       const DoutPrefixProvider *dpp) override;
  virtual int swift_versioning_copy(const DoutPrefixProvider *dpp,
                                    optional_yield y) override;
  virtual std::unique_ptr<ReadOp> get_read_op() override {
    return std::make_unique<SimpleFileObject::SimpleFileReadOp>(this, nullptr);
  }
  virtual std::unique_ptr<DeleteOp> get_delete_op() override {
    return std::make_unique<SimpleFileObject::SimpleFileDeleteOp>(this);
  }
  virtual int omap_get_vals(const DoutPrefixProvider *dpp,
                            const std::string &marker, uint64_t count,
                            std::map<std::string, bufferlist> *m, bool *pmore,
                            optional_yield y) override;
  virtual int omap_get_all(const DoutPrefixProvider *dpp,
                           std::map<std::string, bufferlist> *m,
                           optional_yield y) override;
  virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
                                    const std::string &oid,
                                    const std::set<std::string> &keys,
                                    Attrs *vals) override;
  virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp,
                                  const std::string &key, bufferlist &val,
                                  bool must_exist, optional_yield y) override;
  // will be removed in the future..
  virtual int get_obj_state(const DoutPrefixProvider *dpp, RGWObjState **state,
                            optional_yield y, bool follow_olh = true) override {
    return 0;
  }
  virtual int set_obj_attrs(const DoutPrefixProvider *dpp, Attrs *setattrs,
                            Attrs *delattrs, optional_yield y) override {
    return 0;
  }
};

class UnsupportedLuaScriptManager : public LuaScriptManager {
 public:
  UnsupportedLuaScriptManager() = default;
  UnsupportedLuaScriptManager(const UnsupportedLuaScriptManager&) = delete;
  UnsupportedLuaScriptManager& operator=(const UnsupportedLuaScriptManager&) = delete;
  virtual ~UnsupportedLuaScriptManager() = default;

  virtual int get(const DoutPrefixProvider *dpp, optional_yield y,
                  const std::string &key, std::string &script) override {
    return -ENOENT;
  }
  virtual int put(const DoutPrefixProvider *dpp, optional_yield y,
                  const std::string &key, const std::string &script) override {
    return -ENOENT;
  }
  virtual int del(const DoutPrefixProvider *dpp, optional_yield y,
                  const std::string &key) override {
    return -ENOENT;
  }
};

class SimpleFileZone : public Zone {
 protected:
  SimpleFileStore *store;
  RGWRealm *realm{nullptr};
  RGWZoneGroup *zonegroup{nullptr};
  RGWZone *zone_public_config{nullptr};
  RGWZoneParams *zone_params{nullptr};
  RGWPeriod *current_period{nullptr};
  rgw_zone_id cur_zone_id;

 public:
  SimpleFileZone(const SimpleFileZone&) = delete;
  SimpleFileZone& operator= (const SimpleFileZone&) = delete;
  SimpleFileZone(SimpleFileStore *_store);
  ~SimpleFileZone() = default;

  virtual const RGWZoneGroup &get_zonegroup() override;
  virtual int get_zonegroup(const std::string &id,
                            RGWZoneGroup &zonegroup) override;
  virtual const RGWZoneParams &get_params() override;
  virtual const rgw_zone_id &get_id() override;
  virtual const RGWRealm &get_realm() override;
  virtual const std::string &get_name() const override;
  virtual bool is_writeable() override;
  virtual bool get_redirect_endpoint(std::string *endpoint) override;
  virtual bool has_zonegroup_api(const std::string &api) const override;
  virtual const std::string &get_current_period_id() override;
};

class SimpleFileStore : public Store {
 private:
  RGWUserInfo dummy_user;
  RGWSyncModuleInstanceRef sync_module;
  SimpleFileZone zone;
  const std::filesystem::path data_path;
  std::string luarocks_path = "";
  CephContext *const cctx;

 public:
  SimpleFileStore(CephContext *c, const std::filesystem::path &data_path);
  SimpleFileStore(const SimpleFileStore&) = delete;
  SimpleFileStore& operator=(const SimpleFileStore&) = delete;
  ~SimpleFileStore() {}
  virtual void finalize(void) override;

  virtual const char *get_name() const override { return "simplefile"; }
  virtual std::string get_cluster_id(const DoutPrefixProvider *dpp,
                                     optional_yield y) override {
    return "NA";
  }
  virtual bool is_meta_master() override { return true; }
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key &k) {
    ldout(ctx(), 10) << __func__ << ": TODO obj_key=" << k << dendl;
    return std::make_unique<SimpleFileObject>(*this, k);
  }
  virtual RGWCoroutinesManagerRegistry *get_cr_registry() override {
    return nullptr;
  }
  virtual CephContext *ctx(void) override { return cctx; };
  CephContext *ceph_context() const { return cctx; };

  virtual Zone *get_zone() override { return &zone; }
  virtual const std::string &get_luarocks_path() const override {
    return luarocks_path;
  }
  virtual void set_luarocks_path(const std::string &path) override {
    luarocks_path = path;
  }
  virtual std::unique_ptr<User> get_user(const rgw_user &u) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider *dpp,
                                     const std::string &key, optional_yield y,
                                     std::unique_ptr<User> *user) override;
  virtual int get_user_by_email(const DoutPrefixProvider *dpp,
                                const std::string &email, optional_yield y,
                                std::unique_ptr<User> *user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider *dpp,
                                const std::string &user_str, optional_yield y,
                                std::unique_ptr<User> *user) override;
  virtual int get_bucket(User *u, const RGWBucketInfo &i,
                         std::unique_ptr<Bucket> *bucket) override;
  virtual int get_bucket(const DoutPrefixProvider *dpp, User *u,
                         const rgw_bucket &b, std::unique_ptr<Bucket> *bucket,
                         optional_yield y) override;
  virtual int get_bucket(const DoutPrefixProvider *dpp, User *u,
                         const std::string &tenant, const std::string &name,
                         std::unique_ptr<Bucket> *bucket,
                         optional_yield y) override;
  virtual int forward_request_to_master(const DoutPrefixProvider *dpp,
                                        User *user, obj_version *objv,
                                        bufferlist &in_data, JSONParser *jp,
                                        req_info &info,
                                        optional_yield y) override;
  virtual std::string zone_unique_id(uint64_t unique_num) override;
  virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
  virtual int cluster_stat(RGWClusterStat &stats) override;
  virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
  virtual RGWLC *get_rgwlc(void) override;
  virtual std::unique_ptr<Completions> get_completions(void) override;
  virtual std::unique_ptr<Notification> get_notification(
      rgw::sal::Object *obj, rgw::sal::Object *src_obj, struct req_state *s,
      rgw::notify::EventType event_type,
      const std::string *object_name = nullptr) override;
  virtual std::unique_ptr<Notification> get_notification(
      const DoutPrefixProvider *dpp, rgw::sal::Object *obj,
      rgw::sal::Object *src_obj, rgw::notify::EventType event_type,
      rgw::sal::Bucket *_bucket, std::string &_user_id,
      std::string &_user_tenant, std::string &_req_id,
      optional_yield y) override;

  /** Log usage data to the store.  Usage data is things like bytes
   * sent/received and op count */
  virtual int log_usage(
      const DoutPrefixProvider *dpp,
      std::map<rgw_user_bucket, RGWUsageBatch> &usage_info) override;

  /** Log OP data to the store.  Data is opaque to SAL */
  virtual int log_op(const DoutPrefixProvider *dpp, std::string &oid,
                     bufferlist &bl) override;

  virtual int register_to_service_map(
      const DoutPrefixProvider *dpp, const std::string &daemon_type,
      const std::map<std::string, std::string> &meta) override;

  virtual void get_quota(RGWQuotaInfo &bucket_quota,
                         RGWQuotaInfo &user_quota) override;

  virtual void get_ratelimit(RGWRateLimitInfo &bucket_ratelimit,
                             RGWRateLimitInfo &user_ratelimit,
                             RGWRateLimitInfo &anon_ratelimit) override;

  virtual int set_buckets_enabled(const DoutPrefixProvider *dpp,
                                  std::vector<rgw_bucket> &buckets,
                                  bool enabled) override;

  virtual uint64_t get_new_req_id() override { return 0; }
  virtual int get_sync_policy_handler(const DoutPrefixProvider *dpp,
                                      std::optional<rgw_zone_id> zone,
                                      std::optional<rgw_bucket> bucket,
                                      RGWBucketSyncPolicyHandlerRef *phandler,
                                      optional_yield y) override;

  /** Get a status manager for bucket sync */
  virtual RGWDataSyncStatusManager *get_data_sync_manager(
      const rgw_zone_id &source_zone) override;

  virtual void wakeup_meta_sync_shards(std::set<int> &shard_ids) override;
  virtual void wakeup_data_sync_shards(
      const DoutPrefixProvider *dpp, const rgw_zone_id &source_zone,
      std::map<int, std::set<std::string>> &shard_ids) override;
  virtual int clear_usage(const DoutPrefixProvider *dpp) override { return 0; }
  virtual int read_all_usage(
      const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) override;
  virtual int trim_all_usage(const DoutPrefixProvider *dpp,
                             uint64_t start_epoch, uint64_t end_epoch) override;
  virtual int get_config_key_val(std::string name, bufferlist *bl) override;
  virtual int meta_list_keys_init(const DoutPrefixProvider *dpp,
                                  const std::string &section,
                                  const std::string &marker,
                                  void **phandle) override;
  virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void *handle,
                                  int max, std::list<std::string> &keys,
                                  bool *truncated) override;
  virtual void meta_list_keys_complete(void *handle) override;
  virtual std::string meta_get_marker(void *handle) override;
  virtual int meta_remove(const DoutPrefixProvider *dpp,
                          std::string &metadata_key, optional_yield y) override;

  virtual const RGWSyncModuleInstanceRef &get_sync_module() override;

  virtual std::string get_host_id() override;

  virtual std::unique_ptr<LuaScriptManager> get_lua_script_manager() override;

  virtual std::unique_ptr<RGWRole> get_role(
      std::string name, std::string tenant, std::string path = "",
      std::string trust_policy = "", std::string max_session_duration_str = "",
      std::multimap<std::string, std::string> tags = {}) override;

  virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
  virtual int get_roles(const DoutPrefixProvider *dpp, optional_yield y,
                        const std::string &path_prefix,
                        const std::string &tenant,
                        std::vector<std::unique_ptr<RGWRole>> &roles) override;

  virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;

  virtual int get_oidc_providers(
      const DoutPrefixProvider *dpp, const std::string &tenant,
      std::vector<std::unique_ptr<RGWOIDCProvider>> &providers) override {
    return 0;
  }

  virtual std::unique_ptr<Writer> get_append_writer(
      const DoutPrefixProvider *dpp, optional_yield y,
      std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user &owner,
      const rgw_placement_rule *ptail_placement_rule,
      const std::string &unique_tag, uint64_t position,
      uint64_t *cur_accounted_size) override;

  virtual std::unique_ptr<Writer> get_atomic_writer(
      const DoutPrefixProvider *dpp, optional_yield y,
      std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user &owner,
      const rgw_placement_rule *ptail_placement_rule, uint64_t olh_epoch,
      const std::string &unique_tag) override;

  // TODO make proper bucket path
  std::filesystem::path bucket_path() const { return data_path; };
};

}  // namespace rgw::sal
