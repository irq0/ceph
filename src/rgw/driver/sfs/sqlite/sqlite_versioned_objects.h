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
#include "versioned_object/versioned_object_conversions.h"

namespace rgw::sal::sfs::sqlite  {

class SQLiteVersionedObjects {
  DBConnRef conn;

 public:
  explicit SQLiteVersionedObjects(DBConnRef _conn);
  virtual ~SQLiteVersionedObjects() = default;

  SQLiteVersionedObjects(const SQLiteVersionedObjects&) = delete;
  SQLiteVersionedObjects& operator=(const SQLiteVersionedObjects&) = delete;

  std::optional<DBOPVersionedObjectInfo> get_versioned_object(uint id) const;

  void store_versioned_object(const DBOPVersionedObjectInfo & object) const;
  void remove_versioned_object(uint id) const;

  std::vector<uint> get_versioned_object_ids() const;
  std::vector<uint> get_versioned_object_ids(const uuid_d & object_id) const;

  std::optional<DBOPVersionedObjectInfo> get_last_versioned_object(const uuid_d & object_id) const;
};

}  // namespace rgw::sal::sfs::sqlite
