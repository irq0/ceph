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
#include "sqlite_versioned_objects.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

std::vector<DBOPVersionedObjectInfo> get_rgw_versioned_objects(
  const std::vector<DBVersionedObject> & db_versioned_objects ) {
  std::vector<DBOPVersionedObjectInfo> ret_objs;
  for (const auto & db_obj : db_versioned_objects) {
    auto rgw_obj = get_rgw_versioned_object(db_obj);
    ret_objs.push_back(rgw_obj);
  }
  return ret_objs;
}

SQLiteVersionedObjects::SQLiteVersionedObjects(DBConnRef _conn)
  : conn(_conn) { }

std::optional<DBOPVersionedObjectInfo> SQLiteVersionedObjects::get_versioned_object(uint id) const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto object = storage.get_pointer<DBVersionedObject>(id);
  std::optional<DBOPVersionedObjectInfo> ret_value;
  if (object) {
    ret_value = get_rgw_versioned_object(*object);
  }
  return ret_value;
}

std::optional<DBOPVersionedObjectInfo> SQLiteVersionedObjects::get_versioned_object(const std::string & version_id) const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto versioned_objects = storage.get_all<DBVersionedObject>(where(c(&DBVersionedObject::version_id) = version_id));
  ceph_assert(versioned_objects.size() <= 1);
  std::optional<DBOPVersionedObjectInfo> ret_value;
  if (versioned_objects.size()) {
    ret_value = get_rgw_versioned_object(versioned_objects[0]);
  }
  return ret_value;
}

uint SQLiteVersionedObjects::insert_versioned_object(const DBOPVersionedObjectInfo & object) const {
  std::unique_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto db_object = get_db_versioned_object(object);
  return storage.insert(db_object);
}

void SQLiteVersionedObjects::store_versioned_object(const DBOPVersionedObjectInfo & object) const {
  std::unique_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto db_object = get_db_versioned_object(object);
  storage.update(db_object);
}

void SQLiteVersionedObjects::remove_versioned_object(uint id) const {
  std::unique_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  storage.remove<DBVersionedObject>(id);
}

std::vector<uint> SQLiteVersionedObjects::get_versioned_object_ids() const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  return storage.select(&DBVersionedObject::id);
}

std::vector<uint> SQLiteVersionedObjects::get_versioned_object_ids(const uuid_d & object_id) const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto uuid = object_id.to_string();
  return storage.select(&DBVersionedObject::id, where(c(&DBVersionedObject::object_id) = uuid));
}

std::vector<DBOPVersionedObjectInfo> SQLiteVersionedObjects::get_versioned_objects(const uuid_d & object_id) const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto uuid = object_id.to_string();
  auto versioned_objects = storage.get_all<DBVersionedObject>(where(c(&DBVersionedObject::object_id) = uuid));
  return get_rgw_versioned_objects(versioned_objects);
}

std::optional<DBOPVersionedObjectInfo>
SQLiteVersionedObjects::get_last_versioned_object(
  const uuid_d & object_id
) const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto last_version_id = storage.max(&DBVersionedObject::id, where(c(&DBVersionedObject::object_id) = object_id.to_string()));
  std::optional<DBOPVersionedObjectInfo> ret_value;
  if (last_version_id) {
    auto last_version = storage.get_pointer<DBVersionedObject>(*last_version_id);
    if (last_version) {
      ret_value = get_rgw_versioned_object(*last_version);
    }
  }
  return ret_value;
}

}  // namespace rgw::sal::sfs::sqlite
