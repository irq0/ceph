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

#include "sqlite_schema.h"
#include "objects/object_conversions.h"

namespace rgw::sal::sfs::sqlite  {

class SQLiteObjects : public SQLiteSchema {
 public:
  explicit SQLiteObjects(CephContext *cct);
  virtual ~SQLiteObjects() = default;

  SQLiteObjects(const SQLiteObjects&) = delete;
  SQLiteObjects& operator=(const SQLiteObjects&) = delete;

  std::optional<DBOPObjectInfo> get_object(const uuid_d & uuid) const;
  std::optional<DBOPObjectInfo> get_object(const std::string & bucket_name, const std::string & object_name) const;

  void store_object(const DBOPObjectInfo & object) const;
  void remove_object(const uuid_d & uuid) const;

  std::vector<uuid_d> get_object_ids() const;
  std::vector<uuid_d> get_object_ids(const std::string & bucket_name) const;
};

}  // namespace rgw::sal::sfs::sqlite
