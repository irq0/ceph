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
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

int SFStore::set_buckets_enabled(const DoutPrefixProvider *dpp,
                                         std::vector<rgw_bucket> &buckets,
                                         bool enabled) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFStore::get_bucket(User *u, const RGWBucketInfo &i,
                                std::unique_ptr<Bucket> *bucket) {
  // TODO implement get_bucket by RGWBucketInfo
  ldout(ctx(), 10) << __func__ << ": TODO get_bucket by RGWBucketInfo" << dendl;
  return -ENOTSUP;
}

int SFStore::get_bucket(const DoutPrefixProvider *dpp, User *u,
                                const rgw_bucket &b,
                                std::unique_ptr<Bucket> *result,
                                optional_yield y) {
  std::lock_guard l(buckets_map_lock);
  auto it = buckets.find(b.name);
  if (it == buckets.end()) {
    return -ENOENT;
  }
  auto bucketref = it->second;

  if (bucketref->get_deleted_flag()) {
    return -ENOENT;
  }
  auto bucket = make_unique<SFSBucket>(this, bucketref, bucketref->to_rgw_bucket_info());
  ldpp_dout(dpp, 10) << __func__ << ": bucket: " << bucket->get_name() << dendl;
  result->reset(bucket.release());
  return 0;
}

int SFStore::get_bucket(const DoutPrefixProvider *dpp, User *u,
                                const std::string &tenant,
                                const std::string &name,
                                std::unique_ptr<Bucket> *bucket,
                                optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": get_bucket by name: " << name << dendl;
  std::lock_guard l(buckets_map_lock);
  auto it = buckets.find(name);
  if (it == buckets.end()) {
    return -ENOENT;
  }
  auto bucketref = it->second;
  if (bucketref->get_deleted_flag()) {
    return -ENOENT;
  }
  auto b = make_unique<SFSBucket>(this, bucketref, bucketref->to_rgw_bucket_info());
  ldpp_dout(dpp, 10) << __func__ << ": bucket: " << b->get_name() << dendl;
  bucket->reset(b.release());
  return 0;
}

} // ns rgw::sal
