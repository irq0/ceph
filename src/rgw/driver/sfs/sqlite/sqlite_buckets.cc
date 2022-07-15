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
#include "sqlite_buckets.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteBuckets::SQLiteBuckets(CephContext *cct)
  : SQLiteSchema(cct) {
}

std::vector<DBOPBucketInfo> get_rgw_buckets(const std::vector<DBBucket> & db_buckets) {
  std::vector<DBOPBucketInfo> ret_buckets;
  for (const auto & db_bucket: db_buckets) {
    auto rgw_bucket = get_rgw_bucket(db_bucket);
    ret_buckets.push_back(rgw_bucket);
  }
  return ret_buckets;
}

std::optional<DBOPBucketInfo> SQLiteBuckets::get_bucket(const std::string & bucket_name) const {
  auto storage = get_storage();
  auto bucket = storage.get_pointer<DBBucket>(bucket_name);
  std::optional<DBOPBucketInfo> ret_value;
  if (bucket) {
    ret_value = get_rgw_bucket(*bucket);
  }
  return ret_value;
}

void SQLiteBuckets::store_bucket(const DBOPBucketInfo & bucket) const {
  auto storage = get_storage();
  auto db_bucket = get_db_bucket(bucket);
  storage.replace(db_bucket);
}

void SQLiteBuckets::remove_bucket(const std::string & bucket_name) const {
  auto storage = get_storage();
  storage.remove<DBBucket>(bucket_name);
}

std::vector<std::string> SQLiteBuckets::get_bucket_ids() const {
  auto storage = get_storage();
  return storage.select(&DBBucket::bucket_name);
}

std::vector<std::string> SQLiteBuckets::get_bucket_ids(const std::string & user_id) const {
  auto storage = get_storage();
  return storage.select(&DBBucket::bucket_name, where(c(&DBBucket::owner_id) = user_id));
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_buckets() const {
  auto storage = get_storage();
  return get_rgw_buckets(storage.get_all<DBBucket>());
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_buckets(const std::string & user_id) const {
  auto storage = get_storage();
  return get_rgw_buckets(storage.get_all<DBBucket>(where(c(&DBBucket::owner_id) = user_id)));
}

}  // namespace rgw::sal::sfs::sqlite
