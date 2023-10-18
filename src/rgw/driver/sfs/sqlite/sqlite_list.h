/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

#include "dbconn.h"
#include "rgw_sal.h"

namespace rgw::sal::sfs::sqlite {

class SQLiteList {
  DBConnRef conn;

 public:
  explicit SQLiteList(DBConnRef _conn);
  virtual ~SQLiteList() = default;

  /// objects lists committed objects in bucket, with optional prefix
  /// search and pagination (max, start_after_object_name). Optionally
  /// sets out_more_available to indicate callers to call again with
  /// pagination.
  bool objects(
      const std::string& bucket_id, const std::string& prefix,
      const std::string& start_after_object_name, size_t max,
      std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available = nullptr
  ) const;

  /// versions lists committed objects versions in bucket, with
  /// optional prefix search and pagination (max,
  /// start_after_object_name). Optionally sets out_more_available to
  /// indicate callers to call again with pagination.
  bool versions(
      const std::string& bucket_id, const std::string& prefix,
      const std::string& start_after_object_name, size_t max,
      std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available = nullptr
  ) const;
  bool versions_orm(
      const std::string& bucket_id, const std::string& prefix,
      const std::string& start_after_object_name, size_t max,
      std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available = nullptr
  ) const;
  bool versions_sqlitecpp(
      const std::string& bucket_id, const std::string& prefix,
      const std::string& start_after_object_name, size_t max,
      std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available = nullptr
  ) const;

  // roll_up_common_prefixes performs S3 common prefix compression to
  // objects and common_prefixes.
  //
  // Add a (`prefix` -> true) entry in `out_common_prefixes`, for
  // every object that starts with `find_after_prefix` and has a
  // `delimiter` in the string after that. `prefix` is the string 0 to
  // `delimiter` position.
  //
  // Copy all other `objects` to `out_objects`.
  void roll_up_common_prefixes(
      const std::string& find_after_prefix, const std::string& delimiter,
      const std::vector<rgw_bucket_dir_entry>& objects,
      std::map<std::string, bool>& out_common_prefixes,
      std::vector<rgw_bucket_dir_entry>& out_objects
  ) const;
};
}  // namespace rgw::sal::sfs::sqlite
