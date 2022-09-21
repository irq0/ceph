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

// bucket to be mapped in DB
// Optional values mean they might have (or not) a value defined.
// Blobs are stored as std::vector<char> but we could specialize the encoder and decoder templates
// from sqlite_orm to store blobs in any user defined type.
struct DBBucket {
  std::string bucket_name;
  std::string bucket_id;
  std::optional<std::string> tenant;
  std::optional<std::string> marker;
  std::optional<int> size;
  std::optional<int> size_rounded;
  std::optional<BLOB> creation_time;
  std::optional<int> count;
  std::optional<std::string> placement_name;
  std::optional<std::string> placement_storage_class;
  std::string owner_id;
  std::optional<uint32_t> flags;
  std::optional<std::string> zone_group;
  std::optional<bool> has_instance_obj;
  std::optional<BLOB> quota;
  std::optional<bool> requester_pays;
  std::optional<bool> has_website;
  std::optional<BLOB> website_conf;
  std::optional<bool> swift_versioning;
  std::optional<std::string> swift_ver_location;
  std::optional<BLOB> mdsearch_config;
  std::optional<std::string> new_bucket_instance_id;
  std::optional<BLOB> object_lock;
  std::optional<BLOB> sync_policy_info_groups;
  std::optional<BLOB> bucket_attrs;
  std::optional<int> bucket_version;
  std::optional<std::string> bucket_version_tag;
  std::optional<BLOB> mtime;
  bool deleted;
};

// Struct with information needed by SAL layer
struct DBOPBucketInfo {
  RGWBucketInfo binfo;
  Attrs battrs;
  bool deleted{false};

  DBOPBucketInfo() = default;

  DBOPBucketInfo(const RGWBucketInfo &info, const Attrs &attrs) : 
    binfo(info),
    battrs(attrs) { }

  DBOPBucketInfo(const DBOPBucketInfo &other) = default;
};

}  // namespace rgw::sal::sfs::sqlite
