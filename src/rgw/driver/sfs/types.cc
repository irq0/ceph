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

#include <memory>
#include <string>

#include "rgw/driver/sfs/object_state.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw/driver/sfs/types.h"
#include "rgw/rgw_sal_sfs.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw
namespace rgw::sal::sfs {

std::string generate_new_version_id(CephContext* ceph_context) {
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];
  gen_rand_alphanumeric_no_underscore(ceph_context, buf, OBJ_INSTANCE_LEN);
  return std::string(buf);
}

Object::Object(const rgw_obj_key& _key, const uuid_d& _uuid)
    : name(_key.name), instance(_key.instance), path(_uuid), deleted(false) {}

Object* Object::create_for_immediate_deletion(const sqlite::DBObject& object) {
  Object* result = new Object(object.name, object.uuid);
  result->deleted = true;
  return result;
}

Object* Object::create_for_query(
    const std::string& name, const uuid_d& uuid, bool deleted, uint version_id
) {
  Object* result = new Object(name, uuid);
  result->deleted = deleted;
  result->version_id = version_id;
  return result;
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

Object* Object::create_from_db_version(
    const std::string& object_name, const sqlite::DBObjectsListItem& version
) {
  Object* result = new Object(
      rgw_obj_key(object_name, sqlite::get_version_id(version)),
      sqlite::get_uuid(version)
  );
  result->deleted =
      (sqlite::get_version_type(version) == VersionType::DELETE_MARKER);
  result->version_id = sqlite::get_id(version);
  result->meta = {
      .size = sqlite::get_size(version),
      .etag = sqlite::get_etag(version),
      .mtime = sqlite::get_mtime(version),
      .delete_at = sqlite::get_delete_time(version)};
  result->attrs = sqlite::get_attrs(version);
  return result;
}

Object* Object::create_for_multipart(const std::string& name) {
  Object* result = new Object(name, UUIDPath::create().get_uuid());
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
  return path.to_path() / std::to_string(version_id);
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

bool Object::get_attr(const std::string& name, bufferlist& dest) {
  auto iter = attrs.find(name);
  if (iter != attrs.end()) {
    dest = iter->second;
    return true;
  }
  return false;
}

void Object::set_attr(const std::string& name, bufferlist& value) {
  attrs[name] = value;
}

Attrs::size_type Object::del_attr(const std::string& name) {
  return attrs.erase(name);
}

Attrs Object::get_attrs() {
  return attrs;
}

void Object::update_attrs(const Attrs& update) {
  attrs = update;
}

void Object::metadata_flush_attrs(SFStore* store) {
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(versioned_object.has_value());
  versioned_object->attrs = get_attrs();
  db_versioned_objs.store_versioned_object(*versioned_object);
}

bool Object::metadata_finish(SFStore* store, bool versioning_enabled) {
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
        .store_versioned_object_delete_rest_transact_if_state(
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

void Object::delete_object_data(SFStore* store, bool all) const {
  if (all) {
    // remove object folder
    std::filesystem::remove(store->get_data_path() / path.to_path());
  } else {
    // remove object data
    std::filesystem::remove(store->get_data_path() / get_storage_path());
  }
}

void MultipartObject::_abort(const DoutPrefixProvider* dpp) {
  // assumes being called while holding the lock.
  ceph_assert(aborted);
  state = State::ABORTED;
  auto path = objref->path.to_path();
  if (std::filesystem::exists(path)) {
    // destroy part's contents
    if (dpp) {
      lsfs_dout(dpp, 10) << "remove part contents at " << path << dendl;
    }
    std::filesystem::remove(path);
  }
  objref.reset();
}

void MultipartObject::abort(const DoutPrefixProvider* dpp) {
  std::lock_guard l(lock);
  lsfs_dout(dpp, 10) << "abort part for upload id: " << upload_id
                     << ", state: " << state << dendl;
  if (state == State::ABORTED) {
    return;
  }

  aborted = true;
  if (state == State::INPROGRESS) {
    lsfs_dout(dpp, 10) << "part upload in progress, wait to abort." << dendl;
    return;
  }
  _abort(dpp);
}

void MultipartUpload::abort(const DoutPrefixProvider* dpp) {
  std::lock_guard l(parts_map_lock);
  lsfs_dout(dpp, 10) << "aborting multipart upload id: " << upload_id
                     << ", object: " << objref->name
                     << ", num parts: " << parts.size() << dendl;

  state = State::ABORTED;
  for (const auto& [id, part] : parts) {
    part->abort(dpp);
  }
  parts.clear();
  objref.reset();
}

ObjectRef Bucket::create_version(const rgw_obj_key& key) {
  // even if a specific version was not asked we generate one
  // non-versioned bucket objects will also have a version_id
  auto version_id = key.instance;
  if (version_id.empty()) {
    version_id = generate_new_version_id(store->ceph_context());
  }
  ObjectRef result;
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

ObjectRef Bucket::get(const rgw_obj_key& key) {
  auto maybe_result = Object::try_fetch_from_database(
      store, key.name, info.bucket.bucket_id, key.instance,
      get_info().versioning_enabled()
  );

  if (maybe_result == nullptr) {
    throw UnknownObjectException();
  }

  return std::shared_ptr<Object>(maybe_result);
}

std::vector<ObjectRef> Bucket::get_all() {
  std::vector<ObjectRef> result;
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  // get the list of objects and its last version (filters deleted versions)
  // if an object has all versions deleted it is also filtered
  auto objects =
      db_versioned_objs.list_last_versioned_objects(info.bucket.bucket_id);
  for (const auto& db_obj : objects) {
    if (sqlite::get_object_state(db_obj) == ObjectState::COMMITTED) {
      result.push_back(std::shared_ptr<Object>(
          Object::create_from_db_version(sqlite::get_name(db_obj), db_obj)
      ));
    }
  }
  return result;
}

bool Bucket::delete_object(
    ObjectRef objref, const rgw_obj_key& key, bool versioned_bucket,
    std::string& delete_marker_version_id
) {
  delete_marker_version_id = "";
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);

  if (!versioned_bucket) {
    return _delete_object_non_versioned(objref, key, db_versioned_objs);
  } else {
    if (key.instance.empty()) {
      delete_marker_version_id =
          _add_delete_marker(objref, key, db_versioned_objs);
      return true;
    } else {
      // we have a version id (instance)
      auto version_to_delete =
          db_versioned_objs.get_versioned_object(key.instance);
      if (version_to_delete.has_value()) {
        if (version_to_delete->version_type == VersionType::DELETE_MARKER) {
          _undelete_object(objref, key, db_versioned_objs, *version_to_delete);
          return true;
        } else {
          return _delete_object_version(
              objref, key, db_versioned_objs, *version_to_delete
          );
        }
      }
      return false;
    }
  }
}

std::string Bucket::create_non_existing_object_delete_marker(
    const rgw_obj_key& key
) {
  auto obj = std::shared_ptr<Object>(
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

void Bucket::_undelete_object(
    ObjectRef objref, const rgw_obj_key& key,
    sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
    sqlite::DBVersionedObject& last_version
) {
  if (!last_version.version_id.empty()) {
    // versioned object
    // only remove the delete marker if the requested version id is the last one
    if (!key.instance.empty() && (key.instance == last_version.version_id)) {
      // remove the delete marker and get the previous version in a transaction
      auto previous_version =
          sqlite_versioned_objects.delete_version_and_get_previous_transact(
              last_version.id
          );
      if (previous_version.has_value()) {
        objref->instance = previous_version->version_id;
        objref->deleted = false;
      } else {
        // all versions were removed for this object
      }
    }
  }
}

bool Bucket::_delete_object_non_versioned(
    ObjectRef objref, const rgw_obj_key& key,
    sqlite::SQLiteVersionedObjects& db_versioned_objs
) {
  auto version_to_delete =
      db_versioned_objs.get_last_versioned_object(objref->path.get_uuid());
  return _delete_object_version(
      objref, key, db_versioned_objs, *version_to_delete
  );
}

bool Bucket::_delete_object_version(
    ObjectRef objref, const rgw_obj_key& key,
    sqlite::SQLiteVersionedObjects& db_versioned_objs,
    sqlite::DBVersionedObject& version
) {
  auto now = ceph::real_clock::now();
  version.delete_time = now;
  version.mtime = now;
  version.object_state = ObjectState::DELETED;
  const bool ret = db_versioned_objs.store_versioned_object_if_state(
      version, {ObjectState::OPEN, ObjectState::COMMITTED}
  );
  objref->deleted = true;
  return ret;
}

std::string Bucket::_add_delete_marker(
    ObjectRef objref, const rgw_obj_key& key,
    sqlite::SQLiteVersionedObjects& db_versioned_objs
) {
  std::string delete_marker_id = generate_new_version_id(store->ceph_context());
  bool added;
  auto version_id = db_versioned_objs.add_delete_marker_transact(
      objref->path.get_uuid(), delete_marker_id, added
  );
  if (added) {
    objref->deleted = true;
    objref->instance = delete_marker_id;
    objref->version_id = version_id;
    return delete_marker_id;
  }
  return "";
}
}  // namespace rgw::sal::sfs
