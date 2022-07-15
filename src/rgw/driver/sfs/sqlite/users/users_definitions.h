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
#pragma once

#include "rgw_common.h"

#include <string>

namespace rgw::sal::sfs::sqlite  {

using BLOB = std::vector<char>;

// user to be mapped in DB
// Optinal values mean they might have (or not) a value defined.
// Blobs are stored as std::vector<char> but we could specialise the encoder and decoder templates
// from sqlite_orm to store blobs in any user defined type.
struct DBUser {
  std::string user_id;
  std::optional<std::string> tenant;
  std::optional<std::string> ns;
  std::optional<std::string> display_name;
  std::optional<std::string> user_email;
  std::optional<std::string> access_keys_id;
  std::optional<std::string> access_keys_secret;
  std::optional<BLOB> access_keys;
  std::optional<BLOB> swift_keys;
  std::optional<BLOB> sub_users;
  std::optional<unsigned char> suspended;
  std::optional<int> max_buckets;
  std::optional<int> op_mask;
  std::optional<BLOB> user_caps;
  std::optional<int> admin;
  std::optional<int> system;
  std::optional<std::string> placement_name;
  std::optional<std::string> placement_storage_class;
  std::optional<BLOB> placement_tags;
  std::optional<BLOB> bucke_quota;
  std::optional<BLOB> temp_url_keys;
  std::optional<BLOB> user_quota;
  std::optional<int> type;
  std::optional<BLOB> mfa_ids;
  std::optional<std::string> assumed_role_arn;
  std::optional<BLOB> user_attrs;
  std::optional<int> user_version;
  std::optional<std::string> user_version_tag;
};

// Struct with information needed by SAL layer
// Becase sqlite_orm doesn't like nested members like, for instance, uinfo.user_id.id
// we need to create this structure that will be returned to the user using the SQLiteUsers API.
// The structure stored and retrieved from the database will be DBUser and the one
// received and returned by the SQLiteUsers API will be DBOPUserInfo.
// SQLiteUsers does the needed conversions.
struct DBOPUserInfo {
  RGWUserInfo uinfo;
  obj_version user_version;
  rgw::sal::Attrs user_attrs;
};

}  // namespace rgw::sal::sfs::sqlite
