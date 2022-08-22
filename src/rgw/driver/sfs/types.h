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
#ifndef RGW_STORE_SFS_TYPES_H
#define RGW_STORE_SFS_TYPES_H

#include <exception>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "common/ceph_mutex.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/uuid_path.h"

namespace rgw::sal {
class SFStore;
}

namespace rgw::sal::sfs {

struct UnknownObjectException : public std::exception {};

struct Object {
  struct Meta {
    size_t size;
    std::string etag;
    ceph::real_time mtime;
    ceph::real_time set_mtime;
    ceph::real_time delete_at;
    std::map<std::string, bufferlist> attrs;
  };

  std::string name;
  UUIDPath path;
  Meta meta;
  bool deleted;

  Object(const std::string& _name)
      : name(_name), path(UUIDPath::create()), deleted(false) {}

  Object(const std::string& _name, const uuid_d& _uuid, bool _deleted)
      : name(_name), path(_uuid), deleted(_deleted) {}
};

using ObjectRef = std::shared_ptr<Object>;

class Bucket {
  CephContext* cct;
  SFStore* store;
  const std::string name;
  rgw_bucket bucket;
  RGWUserInfo owner;
  ceph::real_time creation_time;

 public:
  std::map<std::string, ObjectRef> objects;
  ceph::mutex obj_map_lock = ceph::make_mutex("obj_map_lock");
  std::map<std::string, ObjectRef> creating;
  std::set<ObjectRef> deleted;

  Bucket(const Bucket&) = default;

 private:
  void _refresh_objects();

 public:
  Bucket(
      CephContext* _cct, SFStore* _store, const RGWBucketInfo& _bucket_info,
      const RGWUserInfo& _owner
  )
      : cct(_cct),
        store(_store),
        name(_bucket_info.bucket.name),
        bucket(_bucket_info.bucket),
        owner(_owner),
        creation_time(_bucket_info.creation_time) {
    _refresh_objects();
  }

  const std::string get_name() const { return name; }

  rgw_bucket& get_bucket() { return bucket; }

  RGWUserInfo& get_owner() { return owner; }

  ceph::real_time get_creation_time() { return creation_time; }

  ObjectRef get_or_create(const std::string& name) {
    std::lock_guard l(obj_map_lock);

    {
      auto it = objects.find(name);
      if (it != objects.end()) {
        return it->second;
      }
    }

    // is object already being created?
    //   this will potentially clash between two competing tasks.
    //   deal with that later.
    {
      auto it = creating.find(name);
      if (it != creating.end()) {
        return it->second;
      }
    }

    // create new object
    ObjectRef obj = std::make_shared<Object>(name);
    creating[name] = obj;
    return obj;
  }

  ObjectRef get(const std::string& name) {
    auto it = objects.find(name);
    if (it == objects.end()) {
      throw UnknownObjectException();
    }
    return objects[name];
  }

  void finish(const DoutPrefixProvider* dpp, const std::string& objname);

  void delete_object(ObjectRef objref) {
    std::lock_guard l(obj_map_lock);

    objref->deleted = true;

    auto it = objects.find(objref->name);
    if (it == objects.end()) {
      // nothing to do.
      return;
    }

    ObjectRef found = it->second;
    if (!found->path.match(objref->path)) {
      // different objects, nothing to do.
      return;
    }

    deleted.insert(objref);
    objects.erase(it);
  }

  inline std::string get_cls_name() { return "sfs::bucket"; }
};

using BucketRef = std::shared_ptr<Bucket>;

using MetaBucketsRef = std::shared_ptr<sqlite::SQLiteBuckets>;

static inline MetaBucketsRef get_meta_buckets(sqlite::DBConnRef conn) {
  return std::make_shared<sqlite::SQLiteBuckets>(conn);
}

}  // namespace rgw::sal::sfs

#endif  // RGW_STORE_SFS_TYPES_H
