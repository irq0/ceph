#include "users_conversions.h"

namespace rgw::sal::simplefile::sqlite  {

template <typename BLOB_HOLDER, typename DEST>
void decodeBlob(const BLOB_HOLDER & blob_holder, DEST & dest) {
  bufferlist buffer;
  buffer.append(reinterpret_cast<const char *>(blob_holder.data()), blob_holder.size());
  ceph::decode(dest, buffer);
}

template <typename ORIGIN, typename BLOB_HOLDER>
typename std::enable_if<!std::is_same<ORIGIN, RGWUserCaps>::value &&
                        !std::is_same<ORIGIN, RGWQuotaInfo>::value ,void>::type
encodeBlob(const ORIGIN & origin, BLOB_HOLDER & dest) {
  bufferlist buffer;
  ceph::encode(origin, buffer);	
  dest.reserve(buffer.length());
  std::copy(buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest));
}

// encode of RGWUserCaps and RGWQuotaInfo are in the rgw namespace
template <typename ORIGIN, typename BLOB_HOLDER>
typename std::enable_if<std::is_same<ORIGIN, RGWUserCaps>::value ||
                        std::is_same<ORIGIN, RGWQuotaInfo>::value ,void>::type
encodeBlob(const ORIGIN & origin, BLOB_HOLDER & dest) {
  bufferlist buffer;
  encode(origin, buffer);	
  dest.reserve(buffer.length());
  std::copy(buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest));
}

template <typename SOURCE, typename DEST>
typename std::enable_if<!std::is_same<SOURCE, std::vector<char>>::value,void>::type
assignValue(const SOURCE & source, DEST & dest) {
  dest = source;
}

template <typename SOURCE, typename DEST>
typename std::enable_if<std::is_same<SOURCE, std::vector<char>>::value,void>::type
assignValue(const SOURCE & source, DEST & dest) {
  decodeBlob(source, dest);
}

template <typename OPTIONAL, typename DEST>
void assignOptionalValue (const OPTIONAL & optional_value, DEST & dest) {
  // if value is not set, do nothing
  if (!optional_value) return;
  assignValue(*optional_value, dest);
}

template <typename SOURCE, typename DEST>
void assignDBValue(const SOURCE & source, DEST & dest) {
  dest = source;
}

template <typename DEST>
void assignDBValue(const std::string & source, DEST & dest) {
  if (source.empty()) return;
  dest = source;
}

template <typename SOURCE>
void assignDBValue(const SOURCE & source, std::optional<std::vector<char>> & dest) {
  std::vector<char> blob_vector;
  encodeBlob(source, blob_vector);
  dest = blob_vector;
}

DBOPUserInfo getRGWUser(const DBUser & user) {
  DBOPUserInfo rgw_user;
  rgw_user.uinfo.user_id.id = user.UserID;
  assignOptionalValue(user.Tenant, rgw_user.uinfo.user_id.tenant);
  assignOptionalValue(user.NS, rgw_user.uinfo.user_id.ns);
  assignOptionalValue(user.DisplayName, rgw_user.uinfo.display_name);
  assignOptionalValue(user.UserEmail, rgw_user.uinfo.user_email);
  assignOptionalValue(user.AccessKeys, rgw_user.uinfo.access_keys);
  assignOptionalValue(user.SwiftKeys, rgw_user.uinfo.swift_keys);
  assignOptionalValue(user.SubUsers, rgw_user.uinfo.subusers);
  assignOptionalValue(user.Suspended, rgw_user.uinfo.suspended);
  assignOptionalValue(user.MaxBuckets, rgw_user.uinfo.max_buckets);
  assignOptionalValue(user.OpMask, rgw_user.uinfo.op_mask);
  assignOptionalValue(user.UserCaps, rgw_user.uinfo.caps);
  assignOptionalValue(user.Admin, rgw_user.uinfo.admin);
  assignOptionalValue(user.System, rgw_user.uinfo.system);
  assignOptionalValue(user.PlacementName, rgw_user.uinfo.default_placement.name);
  assignOptionalValue(user.PlacementStorageClass, rgw_user.uinfo.default_placement.storage_class);
  assignOptionalValue(user.PlacementTags, rgw_user.uinfo.placement_tags);
  assignOptionalValue(user.BuckeQuota, rgw_user.uinfo.quota.bucket_quota);
  assignOptionalValue(user.TempURLKeys, rgw_user.uinfo.temp_url_keys);
  assignOptionalValue(user.UserQuota, rgw_user.uinfo.quota.user_quota);
  assignOptionalValue(user.TYPE, rgw_user.uinfo.type);
  assignOptionalValue(user.MfaIDs, rgw_user.uinfo.mfa_ids);
  assignOptionalValue(user.AssumedRoleARN, rgw_user.uinfo.assumed_role_arn);
  assignOptionalValue(user.UserAttrs, rgw_user.user_attrs);
  assignOptionalValue(user.UserVersion, rgw_user.user_version.ver);
  assignOptionalValue(user.UserVersionTag, rgw_user.user_version.tag);

  return rgw_user;
}

DBUser getDBUser(const DBOPUserInfo & user) {
  DBUser db_user;
  db_user.UserID = user.uinfo.user_id.id;
  assignDBValue(user.uinfo.user_id.tenant, db_user.Tenant);
  assignDBValue(user.uinfo.user_id.ns, db_user.NS);
  assignDBValue(user.uinfo.display_name, db_user.DisplayName);
  assignDBValue(user.uinfo.user_email, db_user.UserEmail);
  if (!user.uinfo.access_keys.empty()) {
    auto it = user.uinfo.access_keys.begin();
    const auto& k = it->second;
    assignDBValue(k.id, db_user.AccessKeysID);
    assignDBValue(k.key, db_user.AccessKeysSecret);
  }
  assignDBValue(user.uinfo.access_keys, db_user.AccessKeys);
  assignDBValue(user.uinfo.swift_keys, db_user.SwiftKeys);
  assignDBValue(user.uinfo.subusers, db_user.SubUsers);
  assignDBValue(user.uinfo.suspended, db_user.Suspended);
  assignDBValue(user.uinfo.max_buckets, db_user.MaxBuckets);
  assignDBValue(user.uinfo.op_mask, db_user.OpMask);
  assignDBValue(user.uinfo.caps, db_user.UserCaps);
  assignDBValue(user.uinfo.system, db_user.System);
  assignDBValue(user.uinfo.admin, db_user.Admin);
  assignDBValue(user.uinfo.default_placement.name, db_user.PlacementName);
  assignDBValue(user.uinfo.default_placement.storage_class, db_user.PlacementStorageClass);
  assignDBValue(user.uinfo.placement_tags, db_user.PlacementTags);
  assignDBValue(user.uinfo.quota.bucket_quota, db_user.BuckeQuota);
  assignDBValue(user.uinfo.temp_url_keys, db_user.TempURLKeys);
  assignDBValue(user.uinfo.quota.user_quota, db_user.UserQuota);
  assignDBValue(user.uinfo.type, db_user.TYPE);
  assignDBValue(user.uinfo.mfa_ids, db_user.MfaIDs);
  assignDBValue(user.uinfo.assumed_role_arn, db_user.AssumedRoleARN);
  assignDBValue(user.user_attrs, db_user.UserAttrs);
  assignDBValue(user.user_version.ver, db_user.UserVersion);
  assignDBValue(user.user_version.tag, db_user.UserVersionTag);

  return db_user;
}
}  // namespace rgw::sal::simplefile::sqlite
