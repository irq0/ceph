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
#pragma once

#include "dbconn.h"
#include "versioned_object/versioned_object_definitions.h"

namespace rgw::sal::sfs::sqlite {

class SQLiteVersionedObjects {
  DBConnRef conn;

 public:
  explicit SQLiteVersionedObjects(DBConnRef _conn);
  virtual ~SQLiteVersionedObjects() = default;

  SQLiteVersionedObjects(const SQLiteVersionedObjects&) = delete;
  SQLiteVersionedObjects& operator=(const SQLiteVersionedObjects&) = delete;

  std::optional<DBVersionedObject> get_versioned_object(
      uint id, bool filter_deleted = true
  ) const;
  std::optional<DBVersionedObject> get_versioned_object(
      const std::string& version_id, bool filter_deleted = true
  ) const;
  std::optional<DBVersionedObject> get_committed_versioned_object(
      const std::string& bucket_id, const std::string& object_name,
      const std::string& version_id
  ) const;
  DBObjectsListItems list_last_versioned_objects(const std::string& bucket_id
  ) const;

  uint insert_versioned_object(const DBVersionedObject& object) const;
  void store_versioned_object(const DBVersionedObject& object) const;
  bool store_versioned_object_if_state(
      const DBVersionedObject& object, std::vector<ObjectState> allowed_states
  ) const;
  void remove_versioned_object(uint id) const;
  bool store_versioned_object_delete_committed_transact_if_state(
      const DBVersionedObject& object, std::vector<ObjectState> allowed_states
  ) const;

  std::vector<uint> get_versioned_object_ids(bool filter_deleted = true) const;
  std::vector<uint> get_versioned_object_ids(
      const uuid_d& object_id, bool filter_deleted = true
  ) const;
  std::vector<DBVersionedObject> get_versioned_objects(
      const uuid_d& object_id, bool filter_deleted = true
  ) const;

  std::optional<DBVersionedObject> get_last_versioned_object(
      const uuid_d& object_id, bool filter_deleted = true
  ) const;

  std::optional<DBVersionedObject> delete_version_and_get_previous_transact(
      uint id
  ) const;

  std::optional<DBVersionedObject> create_new_versioned_object_transact(
      const std::string& bucket_id, const std::string& object_name,
      const std::string& version_id
  ) const;

  uint add_delete_marker_transact(
      const uuid_d& object_id, const std::string& delete_marker_id, bool& added
  ) const;

 private:
  std::optional<DBVersionedObject>
  get_committed_versioned_object_specific_version(
      const std::string& bucket_id, const std::string& object_name,
      const std::string& version_id
  ) const;

  std::optional<DBVersionedObject> get_committed_versioned_object_last_version(
      const std::string& bucket_id, const std::string& object_name
  ) const;
};

}  // namespace rgw::sal::sfs::sqlite
