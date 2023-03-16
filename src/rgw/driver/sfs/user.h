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
#ifndef RGW_STORE_SFS_USER_H
#define RGW_STORE_SFS_USER_H

#include <memory>
#include <string>

#include "driver/sfs/bucket.h"
#include "rgw_sal.h"

namespace rgw::sal {

class SFStore;

class SFSUser : public StoreUser {
 private:
  SFStore* store;

 protected:
  SFSUser(SFSUser&) = default;
  SFSUser& operator=(const SFSUser&) = default;

 public:
  SFSUser(const rgw_user& _u, SFStore* _store) : StoreUser(_u), store(_store) {}
  SFSUser(const RGWUserInfo& _i, SFStore* _store)
      : StoreUser(_i), store(_store) {}
  virtual ~SFSUser() = default;

  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new SFSUser{*this});
  }
  virtual int list_buckets(
      const DoutPrefixProvider* dpp, const std::string& marker,
      const std::string& end_marker, uint64_t max, bool need_stats,
      BucketList& buckets, optional_yield y
  ) override;
  virtual int create_bucket(
      const DoutPrefixProvider* dpp, const rgw_bucket& b,
      const std::string& zonegroup_id, rgw_placement_rule& placement_rule,
      std::string& swift_ver_location, const RGWQuotaInfo* pquota_info,
      const RGWAccessControlPolicy& policy, Attrs& attrs, RGWBucketInfo& info,
      obj_version& ep_objv, bool exclusive, bool obj_lock_enabled,
      bool* existed, req_info& req_info, std::unique_ptr<Bucket>* bucket,
      optional_yield y
  ) override;
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
      override;
  virtual int merge_and_store_attrs(
      const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y
  ) override;
  virtual int read_stats(
      const DoutPrefixProvider* dpp, optional_yield y, RGWStorageStats* stats,
      ceph::real_time* last_stats_sync = nullptr,
      ceph::real_time* last_stats_update = nullptr
  ) override;
  virtual int read_stats_async(
      const DoutPrefixProvider* dpp, RGWGetUserStats_CB* cb
  ) override;
  virtual int complete_flush_stats(
      const DoutPrefixProvider* dpp, optional_yield y
  ) override;
  virtual int read_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry>& usage
  ) override;
  virtual int trim_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch
  ) override;
  virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y)
      override;
  virtual int store_user(
      const DoutPrefixProvider* dpp, optional_yield y, bool exclusive,
      RGWUserInfo* old_info = nullptr
  ) override;
  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y)
      override;

  virtual int verify_mfa(
      const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp,
      optional_yield y
  ) override;

  std::string get_cls_name() const { return "user"; }
};

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_USER_H