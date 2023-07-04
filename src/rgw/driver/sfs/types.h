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

  bool get_attr(const std::string& name, bufferlist& dest);
  void set_attr(const std::string& name, bufferlist& value);
  Attrs::size_type del_attr(const std::string& name);
  Attrs get_attrs();
  void update_attrs(const Attrs& update);

  std::filesystem::path get_storage_path() const;

  /// Commit all object state to database
  // Including meta and attrs
  // Sets obj version state to COMMITTED
  // For unversioned buckets it set the other versions state to DELETED
  bool metadata_finish(SFStore* store, bool versioning_enabled);

  /// Commit attrs to database
  void metadata_flush_attrs(SFStore* store);

  int delete_object_version(SFStore* store) const;
  void delete_object_metadata(SFStore* store) const;
  /// Delete object _data_ (e.g payload of PUT operations) from disk.
  // Set all=true to delete all versions, not just this version.
  void delete_object_data(SFStore* store, bool all) const;
};

using ObjectRef = std::shared_ptr<Object>;

/**
 * @brief Represents a part's object.
 *
 * Each part of a multipart upload will have an object associated. This is the
 * object the data is being written to. This class keeps track of the part's
 * state, as well as the object the data is being written to.
 */
struct MultipartObject {
  enum State { NONE, PREPARED, INPROGRESS, DONE, ABORTED };

  ceph::mutex lock = ceph::make_mutex("part_lock");
  ObjectRef objref;
  const std::string upload_id;
  ceph::real_time mtime;
  uint64_t offset;
  uint64_t len;
  std::string etag;
  State state;
  bool aborted;

  MultipartObject(ObjectRef obj, const std::string& _upload_id)
      : objref(obj),
        upload_id(_upload_id),
        state(State::NONE),
        aborted(false) {}

  void finish_write(uint64_t _offset, uint64_t _len, const std::string& _etag) {
    std::lock_guard l(lock);
    if (aborted && state != State::ABORTED) {
      _abort(nullptr);
      return;
    }
    ceph_assert(state != State::DONE);
    state = State::DONE;
    offset = _offset;
    len = _len;
    etag = _etag;
    mtime = ceph::real_clock::now();
  }

  void abort(const DoutPrefixProvider* dpp);

  inline std::string get_cls_name() { return "sfs::multipart_object"; }

 private:
  void _abort(const DoutPrefixProvider* dpp);
};

using MultipartObjectRef = std::shared_ptr<MultipartObject>;

/**
 * @brief Represents a current multipart upload.
 *
 * The MultipartUpload class keeps track of all state pertaining to a given
 * multipart upload, from its start up until it is completed by the user.
 *
 * Each part the user uploads is written to a part file, and all files are
 * aggregated upon completion into one single file representing the final
 * object. Part files are afterwards deleted.
 *
 * Unlike other backends, we do not rely on a meta object to track multipart
 * state.
 *
 * This class expects to have an 'upload_id' provided, which should either be
 * user-specified or generated by the initial caller otherwise. Also expects an
 * sfs::Object to be provided, as it should be the caller's responsibility to
 * deal with object creation/allocation, including version management.
 *
 * At the moment, on-going multipart state is not persisted. Should rgw crash or
 * be stopped, multipart metadata is lost. This is to be addressed in follow up
 * changes. Keep in mind we do not reclaim disk space for a broken multipart.
 * This too shall be addressed.
 */
struct MultipartUpload {
  enum State { NONE, INIT, INPROGRESS, AGGREGATING, DONE, ABORTED };

  const std::string upload_id;
  ACLOwner owner;
  ceph::real_time mtime;
  rgw_placement_rule dest_placement;
  rgw::sal::Attrs attrs;

  State state;
  ceph::mutex parts_map_lock = ceph::make_mutex("parts_map_lock");
  std::map<uint32_t, MultipartObjectRef> parts;
  ObjectRef objref;
  const std::string meta_str;

  MultipartUpload(
      ObjectRef _objref, const std::string& _upload_id, ACLOwner& _owner,
      ceph::real_time _mtime
  )
      : upload_id(_upload_id),
        owner(_owner),
        mtime(_mtime),
        state(State::NONE),
        objref(_objref),
        meta_str("_meta." + _objref->name + "." + _upload_id) {}

  const std::string& get_meta_str() const { return meta_str; }

  const std::string& get_obj_name() const { return objref->name; }

  const std::string& get_upload_id() const { return upload_id; }

  const ACLOwner& get_owner() const { return owner; }

  ceph::real_time& get_mtime() { return mtime; }

  void init(rgw_placement_rule& placement, rgw::sal::Attrs& attrs) {
    std::lock_guard l(parts_map_lock);
    ceph_assert(state == State::NONE);
    state = State::INIT;
    dest_placement = placement;
    this->attrs = attrs;
  }

  MultipartObjectRef get_part(uint64_t part_num) {
    std::lock_guard l(parts_map_lock);

    ceph_assert(state == State::INIT || state == State::INPROGRESS);
    state = State::INPROGRESS;

    auto it = parts.find(part_num);
    if (it != parts.end()) {
      return it->second;
    }
    std::string part_obj_name =
        objref->name + "." + upload_id + ".part." + std::to_string(part_num);
    auto part_obj =
        std::shared_ptr<Object>(Object::create_for_multipart(part_obj_name));
    auto part = std::make_shared<MultipartObject>(part_obj, upload_id);
    parts[part_num] = part;
    return part;
  }

  void aggregate() {
    std::lock_guard l(parts_map_lock);
    ceph_assert(state == State::INPROGRESS);
    state = State::AGGREGATING;
  }

  void finish() {
    std::lock_guard l(parts_map_lock);
    ceph_assert(state == State::AGGREGATING);
    state = State::DONE;
    // drop parts
    parts.clear();
  }

  std::map<uint32_t, MultipartObjectRef> get_parts() {
    std::lock_guard l(parts_map_lock);
    // this is doing a copy.
    return parts;
  }

  void abort(const DoutPrefixProvider* dpp);

  inline std::string get_cls_name() { return "sfs::multipart_upload"; }
};

using MultipartUploadRef = std::shared_ptr<MultipartUpload>;

class Bucket {
  CephContext* cct;
  SFStore* store;
  RGWUserInfo owner;
  RGWBucketInfo info;
  rgw::sal::Attrs attrs;
  bool deleted{false};

 public:
  ceph::mutex multipart_map_lock = ceph::make_mutex("multipart_map_lock");
  std::map<std::string, MultipartUploadRef> multiparts;

  Bucket(const Bucket&) = delete;

 private:
  void _undelete_object(
      ObjectRef objref, const rgw_obj_key& key,
      sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
      sqlite::DBVersionedObject& last_version
  );

  bool _delete_object_non_versioned(
      ObjectRef objref, const rgw_obj_key& key,
      sqlite::SQLiteVersionedObjects& sqlite_versioned_objects
  );

  bool _delete_object_version(
      ObjectRef objref, const rgw_obj_key& key,
      sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
      sqlite::DBVersionedObject& version
  );

  std::string _add_delete_marker(
      ObjectRef objref, const rgw_obj_key& key,
      sqlite::SQLiteVersionedObjects& sqlite_versioned_objects
  );

 public:
  Bucket(
      CephContext* _cct, SFStore* _store, const RGWBucketInfo& _bucket_info,
      const RGWUserInfo& _owner, const rgw::sal::Attrs& _attrs
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
  ObjectRef create_version(const rgw_obj_key& key);

  /// Get existing object by key. Throws if it doesn't exist.
  ObjectRef get(const rgw_obj_key& key);
  /// Get copy of all objects that are committed and not deleted
  std::vector<ObjectRef> get_all();

  /// S3 delete object operation: delete version or create tombstone.
  /// If a delete marker was added, it returns the new version id generated for
  /// it. Return indicates if operation succeeded
  bool delete_object(
      ObjectRef objref, const rgw_obj_key& key, bool versioned_bucket,
      std::string& delete_marker_version_id
  );

  /// Delete a non-existing object. Creates object with toumbstone
  // version in database.
  std::string create_non_existing_object_delete_marker(const rgw_obj_key& key);

  MultipartUploadRef get_multipart(
      const std::string& upload_id, const std::string& oid, ACLOwner owner,
      ceph::real_time mtime
  ) {
    std::lock_guard l(multipart_map_lock);

    auto it = multiparts.find(upload_id);
    if (it != multiparts.end()) {
      auto mp = it->second;
      ceph_assert(mp->objref);
      ceph_assert(mp->objref->name == oid);
      return mp;
    }

    ObjectRef obj = std::shared_ptr<Object>(Object::create_from_obj_key(oid));
    MultipartUploadRef mp =
        std::make_shared<MultipartUpload>(obj, upload_id, owner, mtime);
    multiparts[upload_id] = mp;
    return mp;
  }

  void finish_multipart(const std::string& upload_id, ObjectRef objref) {
    std::lock_guard l2(multipart_map_lock);

    auto it = multiparts.find(upload_id);
    ceph_assert(it != multiparts.end());
    auto mp = it->second;
    mp->finish();
    multiparts.erase(it);

    objref->metadata_finish(store, get_info().versioning_enabled());
  }

  std::string gen_multipart_upload_id() {
    auto now = ceph::real_clock::now();
    return ceph::to_iso_8601_no_separators(now, ceph::iso_8601_format::YMDhmsn);
  }

  std::map<std::string, MultipartUploadRef> get_multiparts() {
    std::lock_guard l(multipart_map_lock);
    // this should be doing a copy
    return multiparts;
  }

  void abort_multipart(
      const DoutPrefixProvider* dpp, const std::string& upload_id
  ) {
    std::lock_guard l(multipart_map_lock);
    auto it = multiparts.find(upload_id);
    if (it == multiparts.end()) {
      return;
    }
    multiparts.erase(it);
    it->second->abort(dpp);
  }

  void abort_multiparts(const DoutPrefixProvider* dpp) {
    std::lock_guard l(multipart_map_lock);
    for (const auto& [mpid, mp] : multiparts) {
      mp->abort(dpp);
    }
    multiparts.clear();
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
