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

void SQLiteVersionedObjects::store_versioned_object(const DBOPVersionedObjectInfo & object) const {
  std::unique_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto db_object = get_db_versioned_object(object);
  storage.insert(db_object);
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
