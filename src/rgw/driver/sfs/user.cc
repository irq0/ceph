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
#include "driver/sfs/user.h"

#include <filesystem>

#include "driver/sfs/bucket.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

int SFSUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y) {
  return load_user(dpp, y);
}

int SFSUser::merge_and_store_attrs(
    const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  /** Set the attributes in attrs, leaving any other existing attrs set, and
   * write them to the backing store; a merge operation */
  return -ENOTSUP;
}

int SFSUser::read_stats(
    const DoutPrefixProvider* dpp, optional_yield y, RGWStorageStats* stats,
    ceph::real_time* last_stats_sync, ceph::real_time* last_stats_update
) {
  /** Read the User stats from the backing Store, synchronous */
  ldpp_dout(dpp, 1) << __func__ << ": WARNING faked call" << dendl;
  return 0;
}

int SFSUser::read_stats_async(
    const DoutPrefixProvider* dpp, RGWGetUserStats_CB* cb
) {
  /** Read the User stats from the backing Store, asynchronous */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSUser::complete_flush_stats(
    const DoutPrefixProvider* dpp, optional_yield y
) {
  /** Flush accumulated stat changes for this User to the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSUser::read_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
    std::map<rgw_user_bucket, rgw_usage_log_entry>& usage
) {
  /** Read detailed usage stats for this User from the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSUser::trim_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSUser::load_user(const DoutPrefixProvider* dpp, optional_yield y) {
  rgw::sal::sfs::sqlite::SQLiteUsers sqlite_users(store->db_conn);
  auto db_user = sqlite_users.get_user(info.user_id.id);
  if (db_user) {
    info = db_user->uinfo;
    attrs = db_user->user_attrs;
    objv_tracker.read_version = db_user->user_version;
    return 0;
  }
  return -ENOENT;
}

int SFSUser::store_user(
    const DoutPrefixProvider* dpp, optional_yield y, bool exclusive,
    RGWUserInfo* old_info
) {
  rgw::sal::sfs::sqlite::SQLiteUsers sqlite_users(store->db_conn);
  auto db_user = sqlite_users.get_user(info.user_id.id);
  if (db_user) {
    if (old_info) {
      *old_info = db_user->uinfo;
    }
    if (db_user->user_version.ver != objv_tracker.read_version.ver) {
      ldpp_dout(dpp, 0) << "User Read version mismatch err:(" << -ECANCELED
                        << ") " << dendl;
      return -ECANCELED;
    }
  }
  // increment the version. First stored user will have version = 1
  auto user_version = objv_tracker.read_version;
  user_version.ver++;
  user_version.tag =
      "user_version_tag";  // TODO Check if we need this to be stored
  sqlite_users.store_user({info, user_version, attrs});
  return 0;
}

int SFSUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y) {
  rgw::sal::sfs::sqlite::SQLiteUsers sqlite_users(store->db_conn);
  auto db_user = sqlite_users.get_user(info.user_id.id);
  if (!db_user) {
    return ECANCELED;
  }
  sqlite_users.remove_user(info.user_id.id);
  return 0;
}

int SFSUser::list_buckets(
    const DoutPrefixProvider* dpp, const std::string& marker,
    const std::string& end_marker, uint64_t max, bool need_stats,
    BucketList& buckets, optional_yield y
) {
  // TODO this should list buckets assigned to a user. for now we just get every
  // bucket
  ldpp_dout(dpp, 10) << __func__ << ": marker (" << marker << ", " << end_marker
                     << "), max=" << max << dendl;

  std::list<sfs::BucketRef> lst = store->bucket_list();
  for (const auto& bucketref : lst) {
    if (!bucketref->get_deleted_flag()) {
      buckets.add(std::unique_ptr<Bucket>(new SFSBucket{store, bucketref}));
    }
  }

  ldpp_dout(dpp, 10) << __func__ << ": buckets=" << buckets.get_buckets()
                     << dendl;
  return 0;
}

int SFSUser::create_bucket(
    const DoutPrefixProvider* dpp, const rgw_bucket& b,
    const std::string& zonegroup_id, rgw_placement_rule& placement_rule,
    std::string& swift_ver_location, const RGWQuotaInfo* pquota_info,
    const RGWAccessControlPolicy& policy, Attrs& attrs, RGWBucketInfo& info,
    obj_version& ep_objv, bool exclusive, bool obj_lock_enabled, bool* existed,
    req_info& req_info, std::unique_ptr<Bucket>* bucket_out, optional_yield y
) {
  ceph_assert(bucket_out != nullptr);

  if (store->bucket_exists(b)) {
    *existed = true;
    return -EEXIST;
  }
  *existed = false;

  placement_rule.name = "default";
  placement_rule.storage_class = "STANDARD";
  info.placement_rule = placement_rule;

  sfs::BucketRef bucketref = store->bucket_create(
      b, this->get_info(), zonegroup_id, placement_rule, swift_ver_location,
      pquota_info, attrs, info
  );
  if (!bucketref) {
    lsfs_dout(dpp, 0) << "error creating bucket '" << b << "'" << dendl;
    return -EINVAL;
  }

  bucket_out->reset(new SFSBucket{store, bucketref});

  return 0;
}

int SFSUser::verify_mfa(
    const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp,
    optional_yield y
) {
  return 0;
}

}  // namespace rgw::sal
