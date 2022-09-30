#pragma once

#include "rgw/rgw_sal_sfs.h"

#include <gtest/gtest.h>

template <typename T>
bool compare(const T & origin, const T & dest) {
  return origin == dest;
}

bool compare(const RGWAccessKey & origin, const RGWAccessKey & dest) {
  if (origin.id != dest.id) return false;
  if (origin.key != dest.key) return false;
  if (origin.subuser != dest.subuser) return false;
  return true;
}

bool compare(const RGWSubUser & origin, const RGWSubUser & dest) {
  if (origin.name != dest.name) return false;
  if (origin.perm_mask != dest.perm_mask) return false;
  return true;
}

template <typename T>
bool compareMaps(const T & origin, const T & dest) {
  if (origin.size() != dest.size()) return false;
  for (auto const& [key, val] : origin)
  {
    if (dest.find(key) == dest.end()) return false;
    auto & dest_val = dest.at(key);
    if (!compare(val, dest_val)) return false;
  }
  return true;
}

std::string getCapsString(const RGWUserCaps & caps) {
  auto formatter = std::make_unique<ceph::JSONFormatter>(new ceph::JSONFormatter(true));
  encode_json("caps", caps, formatter.get());
  std::ostringstream oss;
  formatter->flush(oss);
  return oss.str();
}

void compareUsersRGWInfo(const RGWUserInfo & origin, const RGWUserInfo & dest) {
  ASSERT_EQ(origin.user_id.id, dest.user_id.id);
  ASSERT_EQ(origin.user_id.tenant, dest.user_id.tenant);
  ASSERT_EQ(origin.user_id.ns, dest.user_id.ns);
  ASSERT_EQ(origin.display_name, dest.display_name);
  ASSERT_EQ(origin.user_email, dest.user_email);
  ASSERT_TRUE(compareMaps(origin.access_keys, dest.access_keys));
  ASSERT_TRUE(compareMaps(origin.swift_keys, dest.swift_keys));
  ASSERT_TRUE(compareMaps(origin.subusers, dest.subusers));
  ASSERT_EQ(origin.suspended, dest.suspended);
  ASSERT_EQ(origin.max_buckets, dest.max_buckets);
  ASSERT_EQ(origin.op_mask, dest.op_mask);
  ASSERT_EQ(getCapsString(origin.caps), getCapsString(dest.caps));
  ASSERT_EQ(origin.system, dest.system);
  ASSERT_EQ(origin.default_placement.name, dest.default_placement.name);
  ASSERT_EQ(origin.default_placement.storage_class, dest.default_placement.storage_class);
  ASSERT_EQ(origin.placement_tags, dest.placement_tags);
  ASSERT_EQ(origin.quota.bucket_quota.max_size, dest.quota.bucket_quota.max_size);
  ASSERT_EQ(origin.quota.bucket_quota.max_objects, dest.quota.bucket_quota.max_objects);
  ASSERT_EQ(origin.quota.bucket_quota.enabled, dest.quota.bucket_quota.enabled);
  ASSERT_EQ(origin.quota.bucket_quota.check_on_raw, dest.quota.bucket_quota.check_on_raw);
  ASSERT_TRUE(compareMaps(origin.temp_url_keys, dest.temp_url_keys));
  ASSERT_EQ(origin.quota.user_quota.max_size, dest.quota.user_quota.max_size);
  ASSERT_EQ(origin.quota.user_quota.max_objects, dest.quota.user_quota.max_objects);
  ASSERT_EQ(origin.quota.user_quota.enabled, dest.quota.user_quota.enabled);
  ASSERT_EQ(origin.quota.user_quota.check_on_raw, dest.quota.user_quota.check_on_raw);
  ASSERT_EQ(origin.type, dest.type);
  ASSERT_EQ(origin.mfa_ids, dest.mfa_ids);
  ASSERT_EQ(origin.assumed_role_arn, dest.assumed_role_arn);
}

void compareUserAttrs(const rgw::sal::Attrs & origin, const rgw::sal::Attrs & dest) {
  ASSERT_TRUE(compareMaps(origin, dest));
}

void compareUserVersion(const obj_version & origin, const obj_version & dest) {
  ASSERT_EQ(origin.ver, dest.ver);
  ASSERT_EQ(origin.tag, dest.tag);
}
