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
#include "dbconn.h"

#include <sqlite3.h>

#include <filesystem>
#include <system_error>

#include "common/dout.h"

namespace fs = std::filesystem;
namespace orm = sqlite_orm;

namespace rgw::sal::sfs::sqlite {

static std::string get_temporary_db_path(CephContext* ctt) {
  auto rgw_sfs_path = ctt->_conf.get_val<std::string>("rgw_sfs_data_path");
  auto tmp_db_name = std::string(SCHEMA_DB_NAME) + "_tmp";
  auto db_path = std::filesystem::path(rgw_sfs_path) / std::string(tmp_db_name);
  return db_path.string();
}

void DBConn::check_metadata_is_compatible() const {
  // create a copy of the actual metadata
  fs::copy(getDBPath(cct), get_temporary_db_path(cct));
  try {
    fs::copy(getDBPath(cct) + "-wal", get_temporary_db_path(cct) + "-wal");
  } catch (const std::filesystem::filesystem_error& e) {
    if (e.code() != std::errc::no_such_file_or_directory) {
      throw e;
    }
  }

  // try to sync the storage based on the temporary db
  // in case something goes wrong show possible errors and return
  auto test_storage = _make_storage(get_temporary_db_path(cct));
  test_storage.open_forever();
  test_storage.busy_timeout(5000);
  bool sync_error = false;
  std::string result_message;
  try {
    auto sync_res = test_storage.sync_schema();
    std::vector<std::string> non_compatible_tables;
    for (auto const& [table_name, sync_result] : sync_res) {
      if (sync_result == orm::sync_schema_result::dropped_and_recreated) {
        // this result is aggressive as it drops the table and
        // recreates it.
        // Data loss is expected and we should warn the user and
        // stop the final sync in the real database.
        result_message +=
            "Table: [" + table_name + "] is no longer compatible. ";
        non_compatible_tables.push_back(table_name);
      }
    }
    if (non_compatible_tables.size() > 0) {
      sync_error = true;
      result_message = "Tables: [ ";
      for (auto const& table : non_compatible_tables) {
        result_message += table + " ";
      }
      result_message += "] are no longer compatible.";
    }
  } catch (std::exception& e) {
    // check for any other errors (foreign keys constrains, etc...)
    result_message =
        "Metadata database might be corrupted or is no longer compatible";
    sync_error = true;
  }
  // remove the temporary db
  fs::remove(get_temporary_db_path(cct));

  try {
    fs::remove(get_temporary_db_path(cct) + "-wal");
  } catch (const std::filesystem::filesystem_error& e) {
    if (e.code() != std::errc::no_such_file_or_directory) {
      throw e;
    }
  }

  // if there was a sync issue, throw an exception
  if (sync_error) {
    throw sqlite_sync_exception(
        "ERROR ACCESSING SFS METADATA. " + result_message
    );
  }
}

static int get_version(
    CephContext* cct, rgw::sal::sfs::sqlite::Storage& storage
) {
  try {
    return storage.pragma.user_version();
  } catch (const std::system_error& e) {
    lsubdout(cct, rgw, -1) << "error opening db: " << e.code().message() << " ("
                           << e.code().value() << "), " << e.what() << dendl;
    throw e;
  }
}

static int upgrade_metadata_from_v1(sqlite3* db, std::string* errmsg) {
  auto rc = sqlite3_exec(
      db,
      fmt::format(
          "CREATE TABLE '{}' ("
          "'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
          "'bucket_id' TEXT NOT NULL,"
          "'upload_id' TEXT NOT NULL,"
          "'state' INTEGER NOT NULL ,"
          "'state_change_time' INTEGER NOT NULL,"
          "'object_name' TEXT NOT NULL,"
          "'object_uuid' TEXT NOT NULL,"
          "'meta_str' TEXT NOT NULL,"
          "'owner_id' TEXT NOT NULL,"
          "'owner_display_name' TEXT NOT NULL,"
          "'mtime' INTEGER NOT NULL,"
          "'attrs' BLOB NOT NULL,"
          "'placement_name' TEXT NOT NULL,"
          "'placement_storage_class' TEXT NOT NULL,"
          "UNIQUE(upload_id),"
          "UNIQUE(bucket_id, upload_id),"
          "UNIQUE(object_uuid),"
          "FOREIGN KEY('bucket_id') REFERENCES '{}' ('bucket_id')"
          ")",
          MULTIPARTS_TABLE, BUCKETS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error creating '{}' table: {}", MULTIPARTS_TABLE, sqlite3_errmsg(db)
      );
    }
    return -1;
  }

  rc = sqlite3_exec(
      db,
      fmt::format(
          "CREATE TABLE '{}' ("
          "'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
          "'upload_id' TEXT NOT NULL,"
          "'part_num' INTEGER NOT NULL,"
          "'len' INTEGER NOT NULL,"
          "'etag' TEXT,"
          "'mtime' INTEGER,"
          "UNIQUE(upload_id, part_num),"
          "FOREIGN KEY('upload_id') REFERENCES '{}'('upload_id')"
          ")",
          MULTIPARTS_PARTS_TABLE, MULTIPARTS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error creating '{}' table: {}", MULTIPARTS_PARTS_TABLE,
          sqlite3_errmsg(db)
      );
    }
    return -1;
  }
  return 0;
}

static int upgrade_metadata_from_v2(sqlite3* db, std::string* errmsg) {
  auto rc = sqlite3_exec(
      db,
      fmt::format(
          "ALTER TABLE {} RENAME COLUMN len TO size", MULTIPARTS_PARTS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error updating '{}' table: {}", MULTIPARTS_PARTS_TABLE,
          sqlite3_errmsg(db)
      );
    }
    return -1;
  }
  return 0;
}

static void upgrade_metadata(
    CephContext* cct, rgw::sal::sfs::sqlite::Storage& storage, sqlite3* db
) {
  while (true) {
    int cur_version = get_version(cct, storage);
    ceph_assert(cur_version <= SFS_METADATA_VERSION);
    ceph_assert(cur_version >= SFS_METADATA_MIN_VERSION);
    if (cur_version == SFS_METADATA_VERSION) {
      break;
    }

    std::string errmsg;
    int rc = 0;
    if (cur_version == 1) {
      rc = upgrade_metadata_from_v1(db, &errmsg);
    } else if (cur_version == 2) {
      rc = upgrade_metadata_from_v2(db, &errmsg);
    }

    if (rc < 0) {
      auto err = fmt::format(
          "Error upgrading from version {}: {}", cur_version, errmsg
      );
      lsubdout(cct, rgw, -1) << err << dendl;
      throw sqlite_sync_exception(err);
    }

    lsubdout(cct, rgw, 1)
        << fmt::format(
               "upgraded metadata from version {} to version {}", cur_version,
               cur_version + 1
           )
        << dendl;
    storage.pragma.user_version(cur_version + 1);
  }
}

void DBConn::maybe_upgrade_metadata() {
  int db_version = get_version(cct, storage);
  lsubdout(cct, rgw, 10) << "db user version: " << db_version << dendl;

  if (db_version == 0) {
    // must have just been created, set version!
    storage.pragma.user_version(SFS_METADATA_VERSION);
  } else if (db_version < SFS_METADATA_VERSION && db_version >= SFS_METADATA_MIN_VERSION) {
    // perform schema update
    upgrade_metadata(cct, storage, sqlite_db);
  } else if (db_version < SFS_METADATA_MIN_VERSION) {
    throw sqlite_sync_exception(
        "Existing metadata too far behind! Unable to upgrade schema!"
    );
  } else if (db_version > SFS_METADATA_VERSION) {
    // we won't be able to read a database in the future.
    throw sqlite_sync_exception(
        "Existing metadata too far ahead! Please upgrade!"
    );
  }
}

}  // namespace rgw::sal::sfs::sqlite
