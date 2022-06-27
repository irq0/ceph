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
  std::string UserID;
  std::optional<std::string> Tenant;
  std::optional<std::string> NS;
  std::optional<std::string> DisplayName;
  std::optional<std::string> UserEmail;
  std::optional<std::string> AccessKeysID;
  std::optional<std::string> AccessKeysSecret;
  std::optional<BLOB> AccessKeys;
  std::optional<BLOB> SwiftKeys;
  std::optional<BLOB> SubUsers;
  std::optional<unsigned char> Suspended;
  std::optional<int> MaxBuckets;
  std::optional<int> OpMask;
  std::optional<BLOB> UserCaps;
  std::optional<int> Admin;
  std::optional<int> System;
  std::optional<std::string> PlacementName;
  std::optional<std::string> PlacementStorageClass;
  std::optional<BLOB> PlacementTags;
  std::optional<BLOB> BuckeQuota;
  std::optional<BLOB> TempURLKeys;
  std::optional<BLOB> UserQuota;
  std::optional<int> TYPE;
  std::optional<BLOB> MfaIDs;
  std::optional<std::string> AssumedRoleARN;
  std::optional<BLOB> UserAttrs;
  std::optional<int> UserVersion;
  std::optional<std::string> UserVersionTag;
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
