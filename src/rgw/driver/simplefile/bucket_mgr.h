// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#ifndef RGW_STORE_SIMPLEFILE_BUCKET_MGR_H
#define RGW_STORE_SIMPLEFILE_BUCKET_MGR_H

#include <memory>
#include "include/Context.h"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "rgw_sal.h"

namespace rgw::sal {

class SimpleFileStore;
class SimpleFileObject;
class SimpleFileBucket;

class BucketMgr {
  CephContext *cct;
  SimpleFileStore *store;
  std::string bucket_name;
  std::list<SimpleFileObject*> new_objects;
  std::map<std::string, std::string> objects_map;
  version_t object_map_version;
  ceph::mutex commit_lock = ceph::make_mutex("commit_lock");

 public:
  BucketMgr(
    CephContext *_cct,
    SimpleFileStore *_store,
    std::string _bucket_name
  )
    : cct(_cct),
      store(_store),
      bucket_name(_bucket_name),
      object_map_version(0) {
    // ensure we schedule the timer the first time, so that it then renews
    // itself.
    std::lock_guard l{commit_lock};
    load_object_map();
  }
  ~BucketMgr() = default;

  void add_object(SimpleFileObject *obj) {
    std::lock_guard l1{commit_lock};
    new_objects.push_back(obj);
    write_object_map();
    load_object_map();
  }

  void new_bucket(const DoutPrefixProvider *dpp, SimpleFileBucket *bucket);
  std::map<std::string, std::string> get_objects();
  size_t size() const {
    return objects_map.size();
  }

 private:
  void write_object_map();
  void load_object_map();

};

using BucketMgrRef = std::shared_ptr<BucketMgr>;

} // ns rgw::sal

#endif // RGW_STORE_SIMPLEFILE_BUCKET_MGR_H