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

#include "rgw/driver/sfs/types.h"

#include <filesystem>
#include <memory>
#include <string>
#include <system_error>

#include "rgw/driver/sfs/object_state.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw/driver/sfs/types.h"
#include "rgw/rgw_sal_sfs.h"
#include "rgw_obj_types.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw_sfs

namespace rgw::sal::sfs {

std::string generate_new_version_id(CephContext* ceph_context) {
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];
  gen_rand_alphanumeric_no_underscore(ceph_context, buf, OBJ_INSTANCE_LEN);
  return std::string(buf);
}

Object::Object(const rgw_obj_key& _key, const uuid_d& _uuid)
    : name(_key.name), instance(_key.instance), path(_uuid), deleted(false) {}

void Object::delete_version_data(
    SFStore* store, const uuid_d& uuid, uint version_id
) {
  Object* result = new Object(rgw_obj_key(), uuid);
  result->version_id = version_id;
  result->delete_object_data(store);
}

Object* Object::create_for_testing(const std::string& name) {
  Object* result = new Object(name, UUIDPath::create().get_uuid());
  return result;
}

Object* Object::create_from_obj_key(const rgw_obj_key& key) {
  Object* result = new Object(key);
  return result;
}

Object* Object::create_from_db_version(
    const std::string& object_name, const sqlite::DBVersionedObject& version
) {
  Object* result = new Object(
      rgw_obj_key(object_name, version.version_id), version.object_id
  );
  result->deleted = (version.version_type == VersionType::DELETE_MARKER);
  result->version_id = version.id;
  result->meta = {
      .size = version.size,
      .etag = version.etag,
      .mtime = version.mtime,
      .delete_at = version.delete_time};
  result->attrs = version.attrs;
  return result;
}

Object* Object::create_commit_delete_marker(
    const rgw_obj_key& key, SFStore* store, const std::string& bucket_id
) {
  Object* result = new Object(key);
  result->deleted = true;

  sqlite::DBObject oinfo;
  oinfo.uuid = result->path.get_uuid();
  oinfo.bucket_id = bucket_id;
  oinfo.name = result->name;

  sqlite::SQLiteObjects dbobjs(store->db_conn);
  dbobjs.store_object(oinfo);
  return result;
}

Object* Object::try_fetch_from_database(
    SFStore* store, const std::string& name, const std::string& bucket_id,
    const std::string& version_id, bool versioning_enabled
) {
  auto version_id_query = version_id;
  if (!versioning_enabled && version_id == "null") {
    // non versioned bucket and versionId = null --> ignore versionId
    version_id_query = "";
  }
  sqlite::SQLiteVersionedObjects objs_versions(store->db_conn);
  // if version_id is empty it will get the last version for that object
  auto version = objs_versions.get_committed_versioned_object(
      bucket_id, name, version_id_query
  );
  if (!version.has_value()) {
    return nullptr;
  }

  // don't return the version_id if versioning is not enabled
  // and the user didn't ask for a specific version.
  // Returning the version_id when no required breaks s3tests related to tags
  if (!versioning_enabled && version_id.empty()) {
    version->version_id.clear();
  }
  auto result =
      new Object(rgw_obj_key(name, version->version_id), version->object_id);
  result->deleted = (version->version_type == VersionType::DELETE_MARKER);
  result->version_id = version->id;
  result->meta = {
      .size = version->size,
      .etag = version->etag,
      .mtime = version->mtime,
      .delete_at = version->delete_time};
  result->attrs = version->attrs;

  return result;
}

std::filesystem::path Object::get_storage_path() const {
  std::string filename = std::to_string(version_id);
  filename.append(".v");
  return path.to_path() / filename;
}

const Object::Meta Object::get_meta() const {
  return Object::Meta(meta);
}

const Object::Meta Object::get_default_meta() const {
  return Object::Meta();
}

void Object::update_meta(const Meta& update) {
  meta = update;
}

bool Object::get_attr(const std::string& key, bufferlist& dest) const {
  auto iter = attrs.find(key);
  if (iter != attrs.end()) {
    dest = iter->second;
    return true;
  }
  return false;
}

void Object::set_attr(const std::string& key, bufferlist& value) {
  attrs[key] = value;
}

Attrs::size_type Object::del_attr(const std::string& key) {
  return attrs.erase(key);
}

Attrs Object::get_attrs() const {
  return attrs;
}

void Object::update_attrs(const Attrs& update) {
  attrs = update;
}

void Object::metadata_flush_attrs(SFStore* store) const {
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(versioned_object.has_value());
  versioned_object->attrs = get_attrs();
  db_versioned_objs.store_versioned_object(*versioned_object);
}

bool Object::metadata_finish(SFStore* store, bool versioning_enabled) const {
  sqlite::SQLiteObjects dbobjs(store->db_conn);
  auto db_object = dbobjs.get_object(path.get_uuid());
  ceph_assert(db_object.has_value());
  db_object->name = name;
  dbobjs.store_object(*db_object);

  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  // get the object, even if it was deleted.
  // 2 threads could be creating and deleting the object in parallel.
  // last one finishing wins
  auto db_versioned_object =
      db_versioned_objs.get_versioned_object(version_id, false);
  ceph_assert(db_versioned_object.has_value());
  // TODO calculate checksum. Is it already calculated while writing?
  db_versioned_object->size = meta.size;
  db_versioned_object->create_time = meta.mtime;
  db_versioned_object->delete_time = meta.delete_at;
  db_versioned_object->mtime = meta.mtime;
  db_versioned_object->object_state = ObjectState::COMMITTED;
  db_versioned_object->commit_time = ceph::real_clock::now();
  db_versioned_object->etag = meta.etag;
  db_versioned_object->attrs = get_attrs();
  if (versioning_enabled) {
    return db_versioned_objs.store_versioned_object_if_state(
        *db_versioned_object, {ObjectState::OPEN}
    );

  } else {
    return db_versioned_objs
        .store_versioned_object_delete_committed_transact_if_state(
            *db_versioned_object, {ObjectState::OPEN}
        );
  }
}

int Object::delete_object_version(SFStore* store) const {
  // remove metadata
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  db_versioned_objs.remove_versioned_object(version_id);
  return 0;
}

void Object::delete_object_metadata(SFStore* store) const {
  // remove metadata
  sqlite::SQLiteObjects db_objs(store->db_conn);
  db_objs.remove_object(path.get_uuid());
}

void Object::delete_object_data(SFStore* store) const {
  // remove object version data
  std::filesystem::remove(store->get_data_path() / get_storage_path());
  auto folder_path = store->get_data_path() / path.to_path();
  // try to delete the parent folder
  // it won't be deleted if it's not empty.
  std::error_code delete_folder_error;
  // use the overload that throws no exception
  std::filesystem::remove(folder_path, delete_folder_error);
}

std::unique_ptr<Object> Bucket::create_version(const rgw_obj_key& key) const {
  // even if a specific version was not asked we generate one
  // non-versioned bucket objects will also have a version_id
  auto version_id = key.instance;
  if (version_id.empty()) {
    version_id = generate_new_version_id(store->ceph_context());
  }
  std::unique_ptr<Object> result;
  sqlite::SQLiteVersionedObjects objs_versions(store->db_conn);
  // create objects in a transaction.
  // That way threads trying to create the same object in parallel will be
  // synchronised by the database without using extra mutexes.
  auto new_version = objs_versions.create_new_versioned_object_transact(
      info.bucket.bucket_id, key.name, version_id
  );
  if (new_version.has_value()) {
    result.reset(Object::create_from_db_version(key.name, *new_version));
  }
  return result;
}

std::unique_ptr<Object> Bucket::get(const rgw_obj_key& key) const {
  auto maybe_result = Object::try_fetch_from_database(
      store, key.name, info.bucket.bucket_id, key.instance,
      get_info().versioning_enabled()
  );

  if (maybe_result == nullptr) {
    throw UnknownObjectException();
  }

  return std::unique_ptr<Object>(maybe_result);
}

bool Bucket::delete_object(
    const Object& obj, const rgw_obj_key& key, bool versioned_bucket,
    std::string& out_delete_marker_version_id
) const {
  out_delete_marker_version_id = "";
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);

  if (!versioned_bucket) {
    return _delete_object_non_versioned(obj, key, db_versioned_objs);
  } else {
    if (key.instance.empty()) {
      out_delete_marker_version_id =
          _add_delete_marker(obj, key, db_versioned_objs);
      return true;
    } else {
      // we have a version id (instance)
      auto version_to_delete =
          db_versioned_objs.get_versioned_object(key.instance);
      if (version_to_delete.has_value()) {
        if (version_to_delete->version_type == VersionType::DELETE_MARKER) {
          _undelete_object(key, db_versioned_objs, *version_to_delete);
          return true;
        } else {
          return _delete_object_version(db_versioned_objs, *version_to_delete);
        }
      }
      return false;
    }
  }
}

std::string Bucket::create_non_existing_object_delete_marker(
    const rgw_obj_key& key
) const {
  auto obj = std::unique_ptr<Object>(
      Object::create_commit_delete_marker(key, store, info.bucket.bucket_id)
  );
  // create the delete marker
  // generate a new version id
  auto new_version_id = generate_new_version_id(store->ceph_context());
  sqlite::DBVersionedObject version_info;
  version_info.object_id = obj->path.get_uuid();
  version_info.object_state = ObjectState::COMMITTED;
  version_info.version_type = VersionType::DELETE_MARKER;
  version_info.version_id = new_version_id;
  version_info.delete_time = ceph::real_clock::now();
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  obj->version_id = db_versioned_objs.insert_versioned_object(version_info);

  return new_version_id;
}

bool Bucket::_undelete_object(
    const rgw_obj_key& key,
    const sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
    const sqlite::DBVersionedObject& last_version
) const {
  if (!last_version.version_id.empty()) {
    // versioned object
    // only remove the delete marker if the requested version id is the last one
    if (!key.instance.empty() && (key.instance == last_version.version_id)) {
      // remove the delete marker and get the previous version in a transaction
      sqlite_versioned_objects.delete_version(last_version.id);
    }
  }
  return true;
}

bool Bucket::_delete_object_non_versioned(
    const Object& obj, const rgw_obj_key& /*key*/,
    const sqlite::SQLiteVersionedObjects& db_versioned_objs
) const {
  auto version_to_delete =
      db_versioned_objs.get_last_versioned_object(obj.path.get_uuid());
  if (!version_to_delete.has_value()) {
    // if there is no latest version to delete, assume that we are
    // already deleted and do nothing
    return true;
  }
  return _delete_object_version(db_versioned_objs, *version_to_delete);
}

bool Bucket::_delete_object_version(
    const sqlite::SQLiteVersionedObjects& db_versioned_objs,
    const sqlite::DBVersionedObject& version
) const {
  auto now = ceph::real_clock::now();
  sqlite::DBVersionedObject to_delete(version);
  to_delete.delete_time = now;
  to_delete.mtime = now;
  to_delete.object_state = ObjectState::DELETED;
  const bool ret = db_versioned_objs.store_versioned_object_if_state(
      to_delete, {ObjectState::OPEN, ObjectState::COMMITTED}
  );
  return ret;
}

std::string Bucket::_add_delete_marker(
    const Object& obj, const rgw_obj_key& /*key*/,
    const sqlite::SQLiteVersionedObjects& db_versioned_objs
) const {
  std::string delete_marker_id = generate_new_version_id(store->ceph_context());
  bool added;
  db_versioned_objs.add_delete_marker_transact(
      obj.path.get_uuid(), delete_marker_id, added
  );
  if (added) {
    return delete_marker_id;
  }
  return "";
}
}  // namespace rgw::sal::sfs
