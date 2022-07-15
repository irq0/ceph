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
#include "sqlite_schema.h"

#include <filesystem>

namespace rgw::sal::sfs::sqlite  {

SQLiteSchema::SQLiteSchema(CephContext *cct)
    : ceph_context_(cct) {
    sync();
}

std::string SQLiteSchema::getDBPath() const {
    auto rgw_sfs_path = ceph_context_->_conf.get_val<std::string>("rgw_sfs_data_path");
    auto db_path = std::filesystem::path(rgw_sfs_path) / std::string(SCHEMA_DB_NAME);
    return db_path.string();
}

void SQLiteSchema::sync() const {
    get_storage().sync_schema();
}

}  // namespace rgw::sal::sfs::sqlite
