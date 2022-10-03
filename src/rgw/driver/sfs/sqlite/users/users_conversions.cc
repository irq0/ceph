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
#include "users_conversions.h"
#include "../conversion_utils.h"

namespace rgw::sal::sfs::sqlite  {

DBOPUserInfo get_rgw_user(const DBUser & user) {
  DBOPUserInfo rgw_user;
  rgw_user.uinfo.user_id.id = user.user_id;
  assign_optional_value(user.tenant, rgw_user.uinfo.user_id.tenant);
  assign_optional_value(user.ns, rgw_user.uinfo.user_id.ns);
  assign_optional_value(user.display_name, rgw_user.uinfo.display_name);
  assign_optional_value(user.user_email, rgw_user.uinfo.user_email);
  assign_optional_value(user.access_keys, rgw_user.uinfo.access_keys);
  assign_optional_value(user.swift_keys, rgw_user.uinfo.swift_keys);
  assign_optional_value(user.sub_users, rgw_user.uinfo.subusers);
  assign_optional_value(user.suspended, rgw_user.uinfo.suspended);
  assign_optional_value(user.max_buckets, rgw_user.uinfo.max_buckets);
  assign_optional_value(user.op_mask, rgw_user.uinfo.op_mask);
  assign_optional_value(user.user_caps, rgw_user.uinfo.caps);
  assign_optional_value(user.admin, rgw_user.uinfo.admin);
  assign_optional_value(user.system, rgw_user.uinfo.system);
  assign_optional_value(user.placement_name, rgw_user.uinfo.default_placement.name);
  assign_optional_value(user.placement_storage_class, rgw_user.uinfo.default_placement.storage_class);
  assign_optional_value(user.placement_tags, rgw_user.uinfo.placement_tags);
  assign_optional_value(user.bucke_quota, rgw_user.uinfo.quota.bucket_quota);
  assign_optional_value(user.temp_url_keys, rgw_user.uinfo.temp_url_keys);
  assign_optional_value(user.user_quota, rgw_user.uinfo.quota.user_quota);
  assign_optional_value(user.type, rgw_user.uinfo.type);
  assign_optional_value(user.mfa_ids, rgw_user.uinfo.mfa_ids);
  assign_optional_value(user.assumed_role_arn, rgw_user.uinfo.assumed_role_arn);
  assign_optional_value(user.user_attrs, rgw_user.user_attrs);
  assign_optional_value(user.user_version, rgw_user.user_version.ver);
  assign_optional_value(user.user_version_tag, rgw_user.user_version.tag);

  return rgw_user;
}

DBUser get_db_user(const DBOPUserInfo & user) {
  DBUser db_user;
  db_user.user_id = user.uinfo.user_id.id;
  assign_db_value(user.uinfo.user_id.tenant, db_user.tenant);
  assign_db_value(user.uinfo.user_id.ns, db_user.ns);
  assign_db_value(user.uinfo.display_name, db_user.display_name);
  assign_db_value(user.uinfo.user_email, db_user.user_email);
  assign_db_value(user.uinfo.access_keys, db_user.access_keys);
  assign_db_value(user.uinfo.swift_keys, db_user.swift_keys);
  assign_db_value(user.uinfo.subusers, db_user.sub_users);
  assign_db_value(user.uinfo.suspended, db_user.suspended);
  assign_db_value(user.uinfo.max_buckets, db_user.max_buckets);
  assign_db_value(user.uinfo.op_mask, db_user.op_mask);
  assign_db_value(user.uinfo.caps, db_user.user_caps);
  assign_db_value(user.uinfo.system, db_user.system);
  assign_db_value(user.uinfo.admin, db_user.admin);
  assign_db_value(user.uinfo.default_placement.name, db_user.placement_name);
  assign_db_value(user.uinfo.default_placement.storage_class, db_user.placement_storage_class);
  assign_db_value(user.uinfo.placement_tags, db_user.placement_tags);
  assign_db_value(user.uinfo.quota.bucket_quota, db_user.bucke_quota);
  assign_db_value(user.uinfo.temp_url_keys, db_user.temp_url_keys);
  assign_db_value(user.uinfo.quota.user_quota, db_user.user_quota);
  assign_db_value(user.uinfo.type, db_user.type);
  assign_db_value(user.uinfo.mfa_ids, db_user.mfa_ids);
  assign_db_value(user.uinfo.assumed_role_arn, db_user.assumed_role_arn);
  assign_db_value(user.user_attrs, db_user.user_attrs);
  assign_db_value(user.user_version.ver, db_user.user_version);
  assign_db_value(user.user_version.tag, db_user.user_version_tag);

  return db_user;
}
}  // namespace rgw::sal::sfs::sqlite
