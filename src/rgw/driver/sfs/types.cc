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
#include <system_error>

#include "conversion_utils.h"
#include "dbconn.h"
#include "driver/sfs/uuid_path.h"
#include "objects/object_definitions.h"
#include "rgw/driver/sfs/object_state.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw/driver/sfs/types.h"
#include "rgw/rgw_sal_sfs.h"
#include "rgw_sal_sfs.h"
#include "versioned_object/versioned_object_definitions.h"

namespace rgw::sal::sfs {

Object::Object(const std::string& _name, const uuid_d& _uuid)
    : name(_name), path(_uuid), deleted(false) {}

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

Object* Object::create_for_multipart(const std::string& name) {
  Object* result = new Object(name, UUIDPath::create().get_uuid());
  return result;
}

Object* Object::create_commit_delete_marker(
    const rgw_obj_key& key, SFStore* store, const std::string& bucket_id
) {
  Object* result = new Object(key);
  result->deleted = true;

  sqlite::DBOPObjectInfo oinfo;
  oinfo.uuid = result->path.get_uuid();
  oinfo.bucket_id = bucket_id;
  oinfo.name = result->name;

  sqlite::SQLiteObjects dbobjs(store->db_conn);
  dbobjs.store_object(oinfo);
  return result;
}

Object* Object::create_commit_new_object(
    const rgw_obj_key& key, SFStore* store, const std::string& bucket_id,
    const std::string* version_id
) {
  Object* result = new Object(key);

  if (version_id != nullptr) {
    result->instance = *version_id;
  }

  sqlite::DBOPObjectInfo oinfo;
  oinfo.uuid = result->path.get_uuid();
  oinfo.bucket_id = bucket_id;
  oinfo.name = result->name;

  // TODO(https://github.com/aquarist-labs/s3gw/issues/378) make
  // object and version insert a transaction
  sqlite::SQLiteObjects dbobjs(store->db_conn);
  dbobjs.store_object(oinfo);

  sqlite::DBOPVersionedObjectInfo version_info;
  version_info.object_id = result->path.get_uuid();
  version_info.object_state = ObjectState::OPEN;
  version_info.version_id = result->instance;
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  result->version_id = db_versioned_objs.insert_versioned_object(version_info);
  return result;
}

Object* Object::try_create_with_last_version_fetch_from_database(
    SFStore* store, const std::string& name, const std::string& bucket_id
) {
  sqlite::SQLiteObjects objs(store->db_conn);
  auto obj = objs.get_object(bucket_id, name);
  if (!obj) {
    return nullptr;
  }

  sqlite::SQLiteVersionedObjects objs_versions(store->db_conn);
  auto last_version = objs_versions.get_last_versioned_object(obj->uuid);
  if (!last_version.has_value()) {
    return nullptr;
  }

  Object* result = new Object(name, obj->uuid);
  result->deleted = (last_version->object_state == ObjectState::DELETED);
  result->version_id = last_version->id;
  result->meta = {
      .size = obj->size,
      .etag = obj->etag,
      .mtime = obj->mtime,
      .set_mtime = obj->set_mtime,
      .delete_at = obj->delete_at};
  result->attrs = last_version->attrs;
  result->instance = last_version->version_id;

  return result;
}

Object* Object::try_create_fetch_from_database(
    SFStore* store, const std::string& name, const std::string& bucket_id,
    const std::string& version_id
) {
  sqlite::SQLiteObjects objs(store->db_conn);
  auto obj = objs.get_object(bucket_id, name);
  if (!obj) {
    return nullptr;
  }

  sqlite::SQLiteVersionedObjects objs_versions(store->db_conn);
  auto version = objs_versions.get_versioned_object(version_id);
  if (!version.has_value()) {
    return nullptr;
  }

  Object* result = new Object(name, obj->uuid);
  result->deleted = (version->object_state == ObjectState::DELETED);
  result->version_id = version->id;
  result->meta = {
      .size = obj->size,
      .etag = obj->etag,
      .mtime = obj->mtime,
      .set_mtime = obj->set_mtime,
      .delete_at = obj->delete_at};
  result->attrs = version->attrs;
  result->instance = version->version_id;
  return result;
}

std::filesystem::path Object::get_storage_path(
    const UUIDPath& path, uint version_id
) {
  return path.to_path() / std::to_string(version_id);
}

std::filesystem::path Object::get_storage_path() const {
  return get_storage_path(path, version_id);
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

void Object::update_commit_new_version(
    SFStore* store, const std::string& new_version
) {
  sqlite::DBOPVersionedObjectInfo version_info;
  version_info.object_id = path.get_uuid();
  version_info.object_state = ObjectState::OPEN;
  version_info.version_id = new_version;
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  version_id = db_versioned_objs.insert_versioned_object(version_info);
  instance = new_version;
}

void Object::metadata_change_version_state(SFStore* store, ObjectState state) {
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(versioned_object.has_value());
  versioned_object->object_state = state;
  if (state == ObjectState::DELETED) {
    deleted = true;
    versioned_object->deletion_time = ceph::real_clock::now();
  }
  db_versioned_objs.store_versioned_object(*versioned_object);
}

void Object::metadata_flush_attrs(SFStore* store) {
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(versioned_object.has_value());
  versioned_object->attrs = get_attrs();
  db_versioned_objs.store_versioned_object(*versioned_object);
}

void Object::metadata_finish(SFStore* store) {
  sqlite::SQLiteObjects dbobjs(store->db_conn);
  auto db_object = dbobjs.get_object(path.get_uuid());
  ceph_assert(db_object.has_value());
  db_object->name = name;
  db_object->size = meta.size;
  db_object->etag = meta.etag;
  db_object->mtime = meta.mtime;
  db_object->set_mtime = meta.set_mtime;
  db_object->delete_at = meta.delete_at;
  dbobjs.store_object(*db_object);

  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto db_versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(db_versioned_object.has_value());
  // TODO calculate checksum. Is it already calculated while writing?
  db_versioned_object->size = meta.size;
  db_versioned_object->creation_time = meta.mtime;
  db_versioned_object->object_state = ObjectState::COMMITTED;
  db_versioned_object->etag = meta.etag;
  db_versioned_object->attrs = get_attrs();
  db_versioned_objs.store_versioned_object(*db_versioned_object);
}

ObjectDeleter::ObjectDeleter(
    const std::filesystem::path _data_path, sqlite::DBConnRef _dbconn,
    const uuid_d& _uuid
)
    : data_path(_data_path), dbconn(_dbconn), uuid(_uuid) {}

void ObjectDeleter::delete_version(uint version_id) const {
  sqlite::SQLiteVersionedObjects db_versioned_objs(dbconn);
  db_versioned_objs.remove_versioned_object(version_id);
}

void ObjectDeleter::delete_version_data(std::vector<uint> versions) const {
  for (const auto& version : versions) {
    std::filesystem::remove(
        data_path / Object::get_storage_path(uuid, version)
    );
  }
}

void ObjectDeleter::delete_data_directory() const {
  std::filesystem::remove(data_path / uuid.to_path());
}

std::vector<uint> ObjectDeleter::delete_all() const {
  auto storage = dbconn->get_storage();
  auto transaction = storage.transaction_guard();
  // TODO(irq0) Replace with 'delete .. returning' when sqlite_orm
  // supports that
  std::vector<uint> result = storage.select(
      &sqlite::DBVersionedObject::id,
      sqlite_orm::where(
          sqlite_orm::c(&sqlite::DBVersionedObject::object_id) =
              uuid.get_uuid().to_string()
      )
  );
  storage.remove_all<sqlite::DBVersionedObject>(sqlite_orm::where(
      sqlite_orm::c(&sqlite::DBVersionedObject::object_id) =
          uuid.get_uuid().to_string()
  ));
  storage.remove<sqlite::DBObject>(uuid.get_uuid().to_string());
  transaction.commit();
  return result;
}

std::optional<rgw::sal::Attrs> ObjectAttr::get(
    sqlite::DBConnRef _dbconn, const VersionedObjectHandle& vo
) {
  auto storage = _dbconn->get_storage();
  auto rows = storage.select(
      &sqlite::DBVersionedObject::attrs,
      sqlite_orm::where(sqlite_orm::and_(
          sqlite_orm::is_equal(
              &sqlite::DBVersionedObject::object_id, vo.object_id().to_string()
          ),
          sqlite_orm::is_equal(&sqlite::DBVersionedObject::id, vo.version_id())
      ))
  );

  if (rows.size() != 1 || !rows[0].has_value()) {
    return std::nullopt;
  }

  rgw::sal::Attrs result;
  sqlite::decode_blob(rows[0].value(), result);
  return result;
}

std::optional<ObjectState> ObjectAttr::state(
    sqlite::DBConnRef _dbconn, const VersionedObjectHandle& vo
) {
  auto storage = _dbconn->get_storage();
  auto rows = storage.select(
      &sqlite::DBVersionedObject::object_state,
      sqlite_orm::where(sqlite_orm::and_(
          sqlite_orm::is_equal(
              &sqlite::DBVersionedObject::object_id, vo.object_id().to_string()
          ),
          sqlite_orm::is_equal(&sqlite::DBVersionedObject::id, vo.version_id())
      ))
  );

  if (rows.size() != 1) {
    return std::nullopt;
  }
  return static_cast<ObjectState>(rows[0]);
}

ObjectAttr::ObjectAttr(sqlite::DBConnRef _dbconn) : dbconn(_dbconn) {}

VersionedObjectHandle* VersionedObjectHandle::resolve(
    const sqlite::DBConnRef _dbconn, const rgw_obj_key& key
) {
  auto storage = _dbconn->get_storage();

  auto rows = storage.select(
      sqlite_orm::columns(
          &sqlite::DBVersionedObject::object_id, &sqlite::DBVersionedObject::id
      ),
      sqlite_orm::join<sqlite::DBObject>(
          sqlite_orm::using_(&sqlite::DBVersionedObject::object_id)
      ),
      sqlite_orm::where(sqlite_orm::and_(
          sqlite_orm::is_equal(&sqlite::DBObject::name, key.name),
          sqlite_orm::is_equal(
              &sqlite::DBVersionedObject::version_id, key.instance
          )
      ))
  );
  if (rows.size() != 1) {
    return nullptr;
  }

  uuid_d id;
  id.parse(std::get<0>(rows[0]).c_str());
  return new VersionedObjectHandle(key, std::get<1>(rows[0]), UUIDPath(id));
}

std::ostream& operator<<(
    std::ostream& out, const rgw::sal::sfs::VersionedObjectHandle& vo
) {
  return out << "VO("
             << "key:" << vo.m_key << ", id:" << vo.object_id()
             << ", version_id:" << vo.m_version_id << ", path:" << vo.path()
             << ")";
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

ObjectRef Bucket::get_or_create(const rgw_obj_key& key) {
  const bool wants_specific_version = !key.instance.empty();
  ObjectRef result;

  auto maybe_result = Object::try_create_with_last_version_fetch_from_database(
      store, key.name, info.bucket.bucket_id
  );

  if (maybe_result == nullptr) {  // new object
    result.reset(Object::create_commit_new_object(
        key, store, info.bucket.bucket_id, &key.instance
    ));
    return result;
  }

  // an object exists with at least 1 version
  if (wants_specific_version && maybe_result->instance == key.instance) {
    // requested version happens to be the last version
    result.reset(maybe_result);
  } else if (wants_specific_version && maybe_result->instance != key.instance) {
    // requested version is not last

    auto specific_version_object = Object::try_create_fetch_from_database(
        store, key.name, info.bucket.bucket_id, key.instance
    );

    if (specific_version_object == nullptr) {
      // requested version does not exist -> create it from last
      // version object
      result.reset(maybe_result);
      result->update_commit_new_version(store, key.instance);
    } else {
      // requested version does exist -> return it
      result.reset(specific_version_object);
    }
  } else {
    // no specific version requested - return last
    result.reset(maybe_result);
  }

  ceph_assert(result);
  return result;
}

ObjectRef Bucket::get(const std::string& name) {
  auto maybe_result = Object::try_create_with_last_version_fetch_from_database(
      store, name, info.bucket.bucket_id
  );

  if (maybe_result == nullptr) {
    throw UnknownObjectException();
  }
  return std::shared_ptr<Object>(maybe_result);
}

std::vector<ObjectRef> Bucket::get_all() {
  std::vector<ObjectRef> result;
  sqlite::SQLiteObjects dbobjs(store->db_conn);
  for (const auto& db_obj : dbobjs.get_objects(info.bucket.bucket_id)) {
    result.push_back(get(db_obj.name));
  }
  return result;
}

void Bucket::delete_object(ObjectRef objref, const rgw_obj_key& key) {
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  // get the last available version to make a copy changing the object state to DELETED
  auto last_version =
      db_versioned_objs.get_last_versioned_object(objref->path.get_uuid());
  ceph_assert(last_version.has_value());
  if (last_version->object_state == ObjectState::DELETED) {
    _undelete_object(objref, key, db_versioned_objs, *last_version);
  } else {
    last_version->object_state = ObjectState::DELETED;
    last_version->deletion_time = ceph::real_clock::now();

    if (last_version->version_id != "") {
// generate a new version id
#define OBJ_INSTANCE_LEN 32
      char buf[OBJ_INSTANCE_LEN + 1];
      gen_rand_alphanumeric_no_underscore(
          store->ceph_context(), buf, OBJ_INSTANCE_LEN
      );
      last_version->version_id = std::string(buf);
      objref->instance = last_version->version_id;
      // insert a new deleted version
      db_versioned_objs.insert_versioned_object(*last_version);
    } else {
      db_versioned_objs.store_versioned_object(*last_version);
    }
    objref->deleted = true;
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
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];
  gen_rand_alphanumeric_no_underscore(
      store->ceph_context(), buf, OBJ_INSTANCE_LEN
  );
  auto new_version_id = std::string(buf);
  sqlite::DBOPVersionedObjectInfo version_info;
  version_info.object_id = obj->path.get_uuid();
  version_info.object_state = ObjectState::DELETED;
  version_info.version_id = new_version_id;
  version_info.deletion_time = ceph::real_clock::now();
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  obj->version_id = db_versioned_objs.insert_versioned_object(version_info);

  return new_version_id;
}

void Bucket::_undelete_object(
    ObjectRef objref, const rgw_obj_key& key,
    sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
    sqlite::DBOPVersionedObjectInfo& last_version
) {
  if (!last_version.version_id.empty()) {
    // versioned object
    // only remove the delete marker if the requested version id is the last one
    if (!key.instance.empty() && (key.instance == last_version.version_id)) {
      // remove the delete marker
      sqlite_versioned_objects.remove_versioned_object(last_version.id);
      // get the previous id
      auto previous_version =
          sqlite_versioned_objects.get_last_versioned_object(
              objref->path.get_uuid()
          );
      if (previous_version.has_value()) {
        objref->instance = previous_version->version_id;
        objref->deleted = false;
      } else {
        // all versions were removed for this object
      }
    }
  } else {
    // non-versioned object
    // just remove the delete marker in the version and store
    last_version.object_state = ObjectState::COMMITTED;
    last_version.deletion_time = ceph::real_clock::now();
    sqlite_versioned_objects.store_versioned_object(last_version);
    objref->deleted = false;
  }
}

}  // namespace rgw::sal::sfs
