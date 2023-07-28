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
#include "rgw/driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw/driver/sfs/uuid_path.h"
#include "rgw_common.h"
#include "rgw_sal.h"

namespace rgw::sal {
class SFStore;
}

namespace rgw::sal::sfs {

struct UnknownObjectException : public std::exception {};

class Object {
 public:
  struct Meta {
    size_t size;
    std::string etag;
    ceph::real_time mtime;
    ceph::real_time set_mtime;
    ceph::real_time delete_at;
  };

  std::string name;
  std::string instance;
  uint version_id{0};
  UUIDPath path;
  bool deleted;

 private:
  Meta meta;
  std::map<std::string, bufferlist> attrs;

 protected:
  Object(const rgw_obj_key& _key, const uuid_d& _uuid);

  Object(const rgw_obj_key& key)
      : name(key.name),
        instance(key.instance),
        path(UUIDPath::create()),
        deleted(false) {}

  static Object* _get_object(
      SFStore* store, const std::string& bucket_id, const rgw_obj_key& key
  );

 public:
  static Object* create_for_immediate_deletion(const sqlite::DBObject& object);
  static Object* create_for_query(
      const std::string& name, const uuid_d& uuid, bool deleted, uint version_id
  );
  static Object* create_for_testing(const std::string& name);
  static Object* create_from_obj_key(const rgw_obj_key& key);
  static Object* create_from_db_version(
      const std::string& object_name, const sqlite::DBVersionedObject& version
  );
  static Object* create_from_db_version(
      const std::string& object_name, const sqlite::DBObjectsListItem& version
  );
  static Object* create_for_multipart(const std::string& name);

  static Object* create_commit_delete_marker(
      const rgw_obj_key& key, SFStore* store, const std::string& bucket_id
  );

  static Object* try_fetch_from_database(
      SFStore* store, const std::string& name, const std::string& bucket_id,
      const std::string& version_id, bool versioning_enabled
  );

  const Meta get_meta() const;
  const Meta get_default_meta() const;
  void update_meta(const Meta& update);

  bool get_attr(const std::string& key, bufferlist& dest) const;
  void set_attr(const std::string& key, bufferlist& value);
  Attrs::size_type del_attr(const std::string& key);
  Attrs get_attrs() const;
  void update_attrs(const Attrs& update);

  std::filesystem::path get_storage_path() const;

  /// Commit all object state to database
  // Including meta and attrs
  // Sets obj version state to COMMITTED
  // For unversioned buckets it set the other versions state to DELETED
  bool metadata_finish(SFStore* store, bool versioning_enabled) const;

  /// Commit attrs to database
  void metadata_flush_attrs(rgw::sal::SFStore* store) const;

  int delete_object_version(rgw::sal::SFStore* store) const;
  void delete_object_metadata(rgw::sal::SFStore* store) const;
  /// Delete object _data_ (e.g payload of PUT operations) from disk.
  // Set all=true to delete all versions, not just this version.
  void delete_object_data(rgw::sal::SFStore* store, bool all) const;
};

using ObjectRef = std::shared_ptr<Object>;

class Bucket {
  CephContext* cct;
  rgw::sal::SFStore* store;
  RGWUserInfo owner;
  RGWBucketInfo info;
  rgw::sal::Attrs attrs;
  bool deleted{false};

 public:
  ceph::mutex multipart_map_lock = ceph::make_mutex("multipart_map_lock");

  Bucket(const Bucket&) = delete;

 private:
  bool _undelete_object(
      const Object& obj, const rgw_obj_key& key,
      const sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
      const sqlite::DBVersionedObject& last_version
  ) const;

  bool _delete_object_non_versioned(
      const Object& obj, const rgw_obj_key& key,
      const sqlite::SQLiteVersionedObjects& sqlite_versioned_objects
  ) const;

  bool _delete_object_version(
      const Object& obj, const rgw_obj_key& key,
      const sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
      const sqlite::DBVersionedObject& version
  ) const;

  std::string _add_delete_marker(
      const Object& obj, const rgw_obj_key& key,
      const sqlite::SQLiteVersionedObjects& sqlite_versioned_objects
  ) const;

 public:
  Bucket(
      CephContext* _cct, rgw::sal::SFStore* _store,
      const RGWBucketInfo& _bucket_info, const RGWUserInfo& _owner,
      const rgw::sal::Attrs& _attrs
  )
      : cct(_cct),
        store(_store),
        owner(_owner),
        info(_bucket_info),
        attrs(_attrs) {}

  const RGWBucketInfo& get_info() const { return info; }

  RGWBucketInfo& get_info() { return info; }

  const rgw::sal::Attrs& get_attrs() const { return attrs; }

  rgw::sal::Attrs& get_attrs() { return attrs; }

  const std::string get_name() const { return info.bucket.name; }

  const std::string get_bucket_id() const { return info.bucket.bucket_id; }

  rgw_bucket& get_bucket() { return info.bucket; }

  const rgw_bucket& get_bucket() const { return info.bucket; }

  RGWUserInfo& get_owner() { return owner; }

  const RGWUserInfo& get_owner() const { return owner; }

  ceph::real_time get_creation_time() const { return info.creation_time; }

  rgw_placement_rule& get_placement_rule() { return info.placement_rule; }

  uint32_t get_flags() const { return info.flags; }

 public:
  /// Create object version for key
  ObjectRef create_version(const rgw_obj_key& key) const;

  /// Get existing object by key. Throws if it doesn't exist.
  ObjectRef get(const rgw_obj_key& key) const;
  /// Get copy of all objects that are committed and not deleted
  std::vector<ObjectRef> get_all() const;

  /// S3 delete object operation: delete version or create tombstone.
  /// If a delete marker was added, it returns the new version id generated for
  /// it. Return indicates if operation succeeded
  bool delete_object(
      const Object& obj, const rgw_obj_key& key, bool versioned_bucket,
      std::string& delete_marker_version_id
  ) const;

  /// Delete a non-existing object. Creates object with toumbstone
  // version in database.
  std::string create_non_existing_object_delete_marker(const rgw_obj_key& key
  ) const;

  std::string gen_multipart_upload_id() {
    auto now = ceph::real_clock::now();
    return ceph::to_iso_8601_no_separators(now, ceph::iso_8601_format::YMDhmsn);
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
