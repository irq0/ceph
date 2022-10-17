// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t; origami-fold-style: triple-braces -*-
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
 *
 */
#include "rgw_sal_sfs.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <sstream>
#include <system_error>

#include "cls/rgw/cls_rgw_client.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "driver/sfs/notification.h"
#include "driver/sfs/sfs_gc.h"
#include "driver/sfs/writer.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"
#include "rgw_bucket.h"
#include "rgw_lc.h"
#include "rgw_multi.h"
#include "rgw_rest_admin.h"
#include "rgw_rest_bucket.h"
#include "rgw_rest_conn.h"
#include "rgw_rest_log.h"
#include "rgw_rest_metadata.h"
#include "rgw_rest_user.h"
#include "rgw_sal.h"
#include "rgw_service.h"
#include "rgw_tracer.h"
#include "rgw_zone.h"
#include "services/svc_config_key.h"
#include "services/svc_quota.h"
#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

// Lifecycle {{{
std::unique_ptr<Lifecycle> SFStore::get_lifecycle(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
RGWLC* SFStore::get_rgwlc(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

// }}}

// Store > Notifications {{{
std::unique_ptr<Notification> SFStore::get_notification(
    rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s,
    rgw::notify::EventType event_type, optional_yield y,
    const std::string* object_name
) {
  ldout(ctx(), 10) << __func__ << ": return stub notification" << dendl;
  return std::make_unique<SFSNotification>(obj, src_obj, event_type);
}

std::unique_ptr<Notification> SFStore::get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj,
    rgw::sal::Object* src_obj, rgw::notify::EventType event_type,
    rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant,
    std::string& _req_id, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": return stub notification" << dendl;
  return std::make_unique<SFSNotification>(obj, src_obj, event_type);
}

// }}}

// Store > Writer {{{
std::unique_ptr<Writer> SFStore::get_append_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* _head_obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule,
    const std::string& unique_tag, uint64_t position,
    uint64_t* cur_accounted_size
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
/** Get a Writer that atomically writes an entire object */
std::unique_ptr<Writer> SFStore::get_atomic_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* _head_obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t olh_epoch,
    const std::string& unique_tag
) {
  ldpp_dout(dpp, 10) << __func__ << ": return basic atomic writer" << dendl;
  std::string bucketname = _head_obj->get_bucket()->get_name();

  std::lock_guard l(buckets_map_lock);
  ceph_assert(buckets.count(bucketname) > 0);
  auto bucketref = buckets[bucketname];
  return std::make_unique<SFSAtomicWriter>(
      dpp, y, std::move(_head_obj), this, bucketref, owner,
      ptail_placement_rule, olh_epoch, unique_tag
  );
}

// }}}

// Store: Boring Methods {{{
std::unique_ptr<RGWOIDCProvider> SFStore::get_oidc_provider() {
  RGWOIDCProvider* p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int SFStore::forward_request_to_master(
    const DoutPrefixProvider* dpp, User* user, obj_version* objv,
    bufferlist& in_data, JSONParser* jp, req_info& info, optional_yield y
) {
  return 0;
}

int SFStore::forward_iam_request_to_master(
    const DoutPrefixProvider* dpp, const RGWAccessKey& key, obj_version* objv,
    bufferlist& in_data, RGWXMLDecoder::XMLParser* parser, req_info& info,
    optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": not implemented" << dendl;
  return -ENOTSUP;
}

std::string SFStore::zone_unique_id(uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}
std::string SFStore::zone_unique_trans_id(const uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

int SFStore::get_zonegroup(
    const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup
) {
  return -ENOTSUP;
}

int SFStore::list_all_zones(
    const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids
) {
  return -ENOTSUP;
}

int SFStore::cluster_stat(RGWClusterStat& stats) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

void SFStore::wakeup_meta_sync_shards(std::set<int>& shard_ids) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

void SFStore::wakeup_data_sync_shards(
    const DoutPrefixProvider* dpp, const rgw_zone_id& source_zone,
    boost::container::flat_map<
        int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return;
}

int SFStore::register_to_service_map(
    const DoutPrefixProvider* dpp, const string& daemon_type,
    const map<string, string>& meta
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

void SFStore::get_ratelimit(
    RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit,
    RGWRateLimitInfo& anon_ratelimit
) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

void SFStore::get_quota(RGWQuota& quota) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SFStore::get_sync_policy_handler(
    const DoutPrefixProvider* dpp, std::optional<rgw_zone_id> zone,
    std::optional<rgw_bucket> bucket, RGWBucketSyncPolicyHandlerRef* phandler,
    optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

RGWDataSyncStatusManager* SFStore::get_data_sync_manager(
    const rgw_zone_id& source_zone
) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SFStore::read_all_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry>& usage
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SFStore::trim_all_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SFStore::get_config_key_val(string name, bufferlist* bl) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return 0;
}

/*
 The following is a nasty hack to make meta_list_keys_*() work with both
 users and buckets.  What we _should_ do is create some extra class that
 knows how to do meta key listing of various types of object, and pass an
 instance of that around via the handle arguments to these functions.
 This would allow that as-yet-nonexistent class to keep track of max and
 truncated in meta_list_keys_next().  Instead, the immediate nasty hack is
 to make handle point at a const string which indicates what type of
 metadata we're dealing with, then check that value and call an instance
 of the appropriate rgw::sal::sfs::sqlite::SQLite* class to get the
 metdata we care about.

 TODO: replace this nasty hack.
 */
const char* const handle_user = "user";
const char* const handle_bucket = "bucket";

int SFStore::meta_list_keys_init(
    const DoutPrefixProvider* dpp, const string& section, const string& marker,
    void** phandle
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  if (section == "user") {
    *phandle = (void*)handle_user;
  } else if (section == "bucket") {
    *phandle = (void*)handle_bucket;
  }
  return 0;
}

int SFStore::meta_list_keys_next(
    const DoutPrefixProvider* dpp, void* handle, int max, list<string>& keys,
    bool* truncated
) {
  *truncated = false;
  if (handle == (void*)handle_user) {
    rgw::sal::sfs::sqlite::SQLiteUsers sqlite_users(db_conn);
    auto ids = sqlite_users.get_user_ids();
    std::copy(ids.begin(), ids.end(), std::back_inserter(keys));
  } else if (handle == (void*)handle_bucket) {
    rgw::sal::sfs::sqlite::SQLiteBuckets sqlite_buckets(db_conn);
    auto ids = sqlite_buckets.get_bucket_ids();
    std::copy(ids.begin(), ids.end(), std::back_inserter(keys));
  }
  return 0;
}

void SFStore::meta_list_keys_complete(void* handle) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

std::string SFStore::meta_get_marker(void* handle) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

int SFStore::meta_remove(
    const DoutPrefixProvider* dpp, string& metadata_key, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

const RGWSyncModuleInstanceRef& SFStore::get_sync_module() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return sync_module;
}

std::string SFStore::get_host_id() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

std::unique_ptr<LuaManager> SFStore::get_lua_manager() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return std::make_unique<UnsupportedLuaManager>();
}

std::unique_ptr<RGWRole> SFStore::get_role(
    std::string name, std::string tenant, std::string path,
    std::string trust_policy, std::string max_session_duration_str,
    std::multimap<std::string, std::string> tags
) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> SFStore::get_role(std::string id) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> SFStore::get_role(const RGWRoleInfo& info) {
  ldout(ctx(), 10) << __func__ << ": not implemented" << dendl;
  return std::unique_ptr<RGWRole>(nullptr);
}

int SFStore::get_roles(
    const DoutPrefixProvider* dpp, optional_yield y,
    const std::string& path_prefix, const std::string& tenant,
    vector<std::unique_ptr<RGWRole>>& roles
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

// }}}

void SFStore::register_admin_apis(RGWRESTMgr* mgr) {
  mgr->register_resource("user", new RGWRESTMgr_User);
  mgr->register_resource("bucket", new RGWRESTMgr_Bucket);
  /*Registering resource for /admin/metadata */
  mgr->register_resource("metadata", new RGWRESTMgr_Metadata);
  mgr->register_resource("log", new RGWRESTMgr_Log);
};

// Store > Logging {{{
int SFStore::log_usage(
    const DoutPrefixProvider* dpp,
    map<rgw_user_bucket, RGWUsageBatch>& usage_info
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SFStore::log_op(
    const DoutPrefixProvider* dpp, string& oid, bufferlist& bl
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

// }}}

// Initialization {{{

int SFStore::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 10) << __func__ << dendl;
  gc = new sfs::SFSGC();
  gc->initialize(cct, this);
  gc->start_processor();
  return 0;
}

void SFStore::finalize(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  delete gc;
  gc = nullptr;
  return;
}

void SFStore::maybe_init_store() {
  if (!std::filesystem::exists(data_path)) {
    std::filesystem::create_directories(data_path);
  }
}

SFStore::SFStore(CephContext* c, const std::filesystem::path& data_path)
    : sync_module(), zone(this), data_path(data_path), cctx(c) {
  maybe_init_store();
  db_conn = std::make_shared<sfs::sqlite::DBConn>(cctx);

  // no need to be safe, we're in the ctor.
  _refresh_buckets();

  ldout(ctx(), 0) << "sfs serving data from " << data_path << dendl;
}

SFStore::~SFStore() {}

}  // namespace rgw::sal

extern "C" {
void* newSFStore(CephContext* cct) {
  rgw::sal::SFStore* store = new rgw::sal::SFStore(cct, "/tmp");
  return store;
}
}

// }}}
