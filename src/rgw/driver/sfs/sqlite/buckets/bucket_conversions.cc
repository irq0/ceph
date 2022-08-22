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
#include "bucket_conversions.h"
#include "../conversion_utils.h"

namespace rgw::sal::sfs::sqlite  {

DBOPBucketInfo get_rgw_bucket(const DBBucket & bucket) {
  DBOPBucketInfo rgw_bucket;

  rgw_bucket.binfo.bucket.name = bucket.bucket_name;
  assign_optional_value(bucket.tenant, rgw_bucket.binfo.bucket.tenant);
  assign_optional_value(bucket.marker, rgw_bucket.binfo.bucket.marker);
  assign_optional_value(bucket.bucket_id, rgw_bucket.binfo.bucket.bucket_id);
  rgw_bucket.binfo.owner.id = bucket.owner_id;
  assign_optional_value(bucket.flags, rgw_bucket.binfo.flags);
  assign_optional_value(bucket.creation_time, rgw_bucket.binfo.creation_time);
  assign_optional_value(bucket.placement_name, rgw_bucket.binfo.placement_rule.name);
  assign_optional_value(bucket.placement_storage_class, rgw_bucket.binfo.placement_rule.storage_class);

  return rgw_bucket;
}

DBBucket get_db_bucket(const DBOPBucketInfo & bucket) {
  DBBucket db_bucket;

  db_bucket.bucket_name = bucket.binfo.bucket.name;
  assign_db_value(bucket.binfo.bucket.tenant, db_bucket.tenant);
  assign_db_value(bucket.binfo.bucket.marker, db_bucket.marker);
  assign_db_value(bucket.binfo.bucket.bucket_id, db_bucket.bucket_id);
  db_bucket.owner_id = bucket.binfo.owner.id;
  assign_db_value(bucket.binfo.flags, db_bucket.flags);
  assign_db_value(bucket.binfo.creation_time, db_bucket.creation_time);
  assign_db_value(bucket.binfo.placement_rule.name, db_bucket.placement_name);
  assign_db_value(bucket.binfo.placement_rule.storage_class, db_bucket.placement_storage_class);

  return db_bucket;
}
}  // namespace rgw::sal::sfs::sqlite
