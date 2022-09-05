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
  std::string instance;
  uint version_id{0};
  UUIDPath path;
  Meta meta;
  bool deleted;

  Object(const std::string& _name)
      : name(_name), path(UUIDPath::create()), deleted(false) {}

  Object(const std::string& _name, const uuid_d& _uuid, bool _deleted)
      : name(_name), path(_uuid), deleted(_deleted) {}

  Object(const rgw_obj_key& key)
      : name(key.name),
        instance(key.instance),
        path(UUIDPath::create()),
        deleted(false) {}

  std::filesystem::path get_storage_path() const;

  void metadata_init(
      SFStore* store, const std::string& bucket_name, bool new_object,
      bool new_version
  );
  void metadata_change_version_state(SFStore* store, ObjectState state);
  void metadata_finish(SFStore* store);
};

using ObjectRef = std::shared_ptr<Object>;

class Bucket {
  CephContext* cct;
  SFStore* store;
  const std::string name;
  rgw_bucket bucket;
  RGWUserInfo owner;
  ceph::real_time creation_time;
  rgw_placement_rule placement_rule;
  uint32_t flags{0};

 public:
  std::map<std::string, ObjectRef> objects;
  ceph::mutex obj_map_lock = ceph::make_mutex("obj_map_lock");

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
        creation_time(_bucket_info.creation_time),
        placement_rule(_bucket_info.placement_rule),
        flags(_bucket_info.flags) {
    _refresh_objects();
  }

  RGWBucketInfo& to_rgw_bucket_info(RGWBucketInfo& out_info) const {
    out_info.bucket = get_bucket();
    out_info.owner = get_owner().user_id;
    out_info.creation_time = get_creation_time();
    out_info.placement_rule.name = placement_rule.name;
    out_info.placement_rule.storage_class = placement_rule.storage_class;
    out_info.flags = get_flags();
    return out_info;
  }

  RGWBucketInfo to_rgw_bucket_info() const {
    RGWBucketInfo rval;
    to_rgw_bucket_info(rval);
    return rval;
  }

  const std::string get_name() const { return name; }

  rgw_bucket& get_bucket() { return bucket; }

  const rgw_bucket& get_bucket() const { return bucket; }

  RGWUserInfo& get_owner() { return owner; }

  const RGWUserInfo& get_owner() const { return owner; }

  ceph::real_time get_creation_time() const { return creation_time; }

  uint32_t get_flags() const { return flags; }

  ObjectRef get_or_create(const rgw_obj_key& key);

  ObjectRef get(const std::string& name) {
    auto it = objects.find(name);
    if (it == objects.end()) {
      throw UnknownObjectException();
    }
    return objects[name];
  }

  void finish(const DoutPrefixProvider* dpp, const std::string& objname);

  void delete_object(ObjectRef objref);

  inline std::string get_cls_name() { return "sfs::bucket"; }
};

using BucketRef = std::shared_ptr<Bucket>;

using MetaBucketsRef = std::shared_ptr<sqlite::SQLiteBuckets>;

static inline MetaBucketsRef get_meta_buckets(sqlite::DBConnRef conn) {
  return std::make_shared<sqlite::SQLiteBuckets>(conn);
}

}  // namespace rgw::sal::sfs

#endif  // RGW_STORE_SFS_TYPES_H
