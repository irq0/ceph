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
#include "sqlite_objects.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

std::vector<uuid_d> get_rgw_uuids( const std::vector<std::string> & uuids) {
  std::vector<uuid_d> ret_ids;
  for (auto const & uuid: uuids) {
    uuid_d rgw_uuid;
    rgw_uuid.parse(uuid.c_str());
    ret_ids.push_back(rgw_uuid);
  }
  return ret_ids;
}

SQLiteObjects::SQLiteObjects(CephContext *cct)
  : SQLiteSchema(cct) {
}

std::optional<DBOPObjectInfo> SQLiteObjects::get_object(const uuid_d & uuid) const {
  auto storage = get_storage();
  auto object = storage.get_pointer<DBObject>(uuid.to_string());
  std::optional<DBOPObjectInfo> ret_value;
  if (object) {
    ret_value = get_rgw_object(*object);
  }
  return ret_value;
}

std::optional<DBOPObjectInfo> SQLiteObjects::get_object(const std::string & bucket_name,
                                                        const std::string & object_name) const {
  auto storage = get_storage();
  auto objects = storage.get_all<DBObject>(where(is_equal(&DBObject::bucket_name, bucket_name) and is_equal(&DBObject::name, object_name)));
  std::optional<DBOPObjectInfo> ret_value;
  // value must be unique
  if (objects.size() == 1) {
    ret_value = get_rgw_object(objects[0]);
  }
  return ret_value;
}

void SQLiteObjects::store_object(const DBOPObjectInfo & object) const {
  auto storage = get_storage();
  auto db_object = get_db_object(object);
  storage.replace(db_object);
}

void SQLiteObjects::remove_object(const uuid_d & uuid) const {
  auto storage = get_storage();
  storage.remove<DBObject>(uuid.to_string());
}

std::vector<uuid_d> SQLiteObjects::get_object_ids() const {
  auto storage = get_storage();
  return get_rgw_uuids(storage.select(&DBObject::object_id));
}

std::vector<uuid_d> SQLiteObjects::get_object_ids(const std::string & bucket_name) const {
  auto storage = get_storage();
  return get_rgw_uuids(storage.select(&DBObject::object_id, where(c(&DBObject::bucket_name) = bucket_name)));
}

}  // namespace rgw::sal::sfs::sqlite
