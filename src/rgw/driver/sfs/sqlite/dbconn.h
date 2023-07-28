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

#include <sqlite3.h>

#include <filesystem>
#include <ios>
#include <memory>

#include "buckets/bucket_definitions.h"
#include "buckets/multipart_definitions.h"
#include "common/ceph_mutex.h"
#include "common/dout.h"
#include "lifecycle/lifecycle_definitions.h"
#include "objects/object_definitions.h"
#include "sqlite_orm.h"
#include "users/users_definitions.h"
#include "versioned_object/versioned_object_definitions.h"

namespace rgw::sal::sfs::sqlite {

/// current db version.
constexpr int SFS_METADATA_VERSION = 3;
/// minimum required version to upgrade db.
constexpr int SFS_METADATA_MIN_VERSION = 1;

constexpr std::string_view SCHEMA_DB_NAME = "s3gw.db";

constexpr std::string_view USERS_TABLE = "users";
constexpr std::string_view BUCKETS_TABLE = "buckets";
constexpr std::string_view OBJECTS_TABLE = "objects";
constexpr std::string_view VERSIONED_OBJECTS_TABLE = "versioned_objects";
constexpr std::string_view ACCESS_KEYS = "access_keys";
constexpr std::string_view LC_HEAD_TABLE = "lc_head";
constexpr std::string_view LC_ENTRIES_TABLE = "lc_entries";
constexpr std::string_view MULTIPARTS_TABLE = "multiparts";
constexpr std::string_view MULTIPARTS_PARTS_TABLE = "multiparts_parts";

class sqlite_sync_exception : public std::exception {
  std::string _message;

 public:
  explicit sqlite_sync_exception(const std::string& message)
      : _message(message) {}

  const char* what() const noexcept override { return _message.c_str(); }
};

inline auto _make_storage(const std::string& path) {
  return sqlite_orm::make_storage(
      path,
      sqlite_orm::make_unique_index(
          "versioned_object_objid_vid_unique", &DBVersionedObject::object_id,
          &DBVersionedObject::version_id
      ),
      sqlite_orm::make_index("bucket_ownerid_idx", &DBBucket::owner_id),
      sqlite_orm::make_index("bucket_name_idx", &DBBucket::bucket_name),
      sqlite_orm::make_index("objects_bucketid_idx", &DBObject::bucket_id),
      sqlite_orm::make_index(
          "vobjs_versionid_idx", &DBVersionedObject::version_id
      ),
      sqlite_orm::make_index(
          "vobjs_object_id_idx", &DBVersionedObject::object_id
      ),
      sqlite_orm::make_table(
          std::string(USERS_TABLE),
          sqlite_orm::make_column(
              "user_id", &DBUser::user_id, sqlite_orm::primary_key()
          ),
          sqlite_orm::make_column("tenant", &DBUser::tenant),
          sqlite_orm::make_column("ns", &DBUser::ns),
          sqlite_orm::make_column("display_name", &DBUser::display_name),
          sqlite_orm::make_column("user_email", &DBUser::user_email),
          sqlite_orm::make_column("access_keys", &DBUser::access_keys),
          sqlite_orm::make_column("swift_keys", &DBUser::swift_keys),
          sqlite_orm::make_column("sub_users", &DBUser::sub_users),
          sqlite_orm::make_column("suspended", &DBUser::suspended),
          sqlite_orm::make_column("max_buckets", &DBUser::max_buckets),
          sqlite_orm::make_column("op_mask", &DBUser::op_mask),
          sqlite_orm::make_column("user_caps", &DBUser::user_caps),
          sqlite_orm::make_column("admin", &DBUser::admin),
          sqlite_orm::make_column("system", &DBUser::system),
          sqlite_orm::make_column("placement_name", &DBUser::placement_name),
          sqlite_orm::make_column(
              "placement_storage_class", &DBUser::placement_storage_class
          ),
          sqlite_orm::make_column("placement_tags", &DBUser::placement_tags),
          sqlite_orm::make_column("bucket_quota", &DBUser::bucket_quota),
          sqlite_orm::make_column("temp_url_keys", &DBUser::temp_url_keys),
          sqlite_orm::make_column("user_quota", &DBUser::user_quota),
          sqlite_orm::make_column("type", &DBUser::type),
          sqlite_orm::make_column("mfa_ids", &DBUser::mfa_ids),
          sqlite_orm::make_column(
              "assumed_role_arn", &DBUser::assumed_role_arn
          ),
          sqlite_orm::make_column("user_attrs", &DBUser::user_attrs),
          sqlite_orm::make_column("user_version", &DBUser::user_version),
          sqlite_orm::make_column("user_version_tag", &DBUser::user_version_tag)
      ),
      sqlite_orm::make_table(
          std::string(BUCKETS_TABLE),
          sqlite_orm::make_column(
              "bucket_id", &DBBucket::bucket_id, sqlite_orm::primary_key()
          ),
          sqlite_orm::make_column("bucket_name", &DBBucket::bucket_name),
          sqlite_orm::make_column("tenant", &DBBucket::tenant),
          sqlite_orm::make_column("marker", &DBBucket::marker),
          sqlite_orm::make_column("owner_id", &DBBucket::owner_id),
          sqlite_orm::make_column("flags", &DBBucket::flags),
          sqlite_orm::make_column("zone_group", &DBBucket::zone_group),
          sqlite_orm::make_column("quota", &DBBucket::quota),
          sqlite_orm::make_column("creation_time", &DBBucket::creation_time),
          sqlite_orm::make_column("placement_name", &DBBucket::placement_name),
          sqlite_orm::make_column(
              "placement_storage_class", &DBBucket::placement_storage_class
          ),
          sqlite_orm::make_column("deleted", &DBBucket::deleted),
          sqlite_orm::make_column("bucket_attrs", &DBBucket::bucket_attrs),
          sqlite_orm::make_column("object_lock", &DBBucket::object_lock),
          sqlite_orm::foreign_key(&DBBucket::owner_id)
              .references(&DBUser::user_id)
      ),
      sqlite_orm::make_table(
          std::string(OBJECTS_TABLE),
          sqlite_orm::make_column(
              "uuid", &DBObject::uuid, sqlite_orm::primary_key()
          ),
          sqlite_orm::make_column("bucket_id", &DBObject::bucket_id),
          sqlite_orm::make_column("name", &DBObject::name),
          sqlite_orm::foreign_key(&DBObject::bucket_id)
              .references(&DBBucket::bucket_id)
      ),
      sqlite_orm::make_table(
          std::string(VERSIONED_OBJECTS_TABLE),
          sqlite_orm::make_column(
              "id", &DBVersionedObject::id, sqlite_orm::autoincrement(),
              sqlite_orm::primary_key()
          ),
          sqlite_orm::make_column("object_id", &DBVersionedObject::object_id),
          sqlite_orm::make_column("checksum", &DBVersionedObject::checksum),
          sqlite_orm::make_column("size", &DBVersionedObject::size),
          sqlite_orm::make_column(
              "create_time", &DBVersionedObject::create_time
          ),
          sqlite_orm::make_column(
              "delete_time", &DBVersionedObject::delete_time
          ),
          sqlite_orm::make_column(
              "commit_time", &DBVersionedObject::commit_time
          ),
          sqlite_orm::make_column("mtime", &DBVersionedObject::mtime),
          sqlite_orm::make_column(
              "object_state", &DBVersionedObject::object_state
          ),
          sqlite_orm::make_column("version_id", &DBVersionedObject::version_id),
          sqlite_orm::make_column("etag", &DBVersionedObject::etag),
          sqlite_orm::make_column("attrs", &DBVersionedObject::attrs),
          sqlite_orm::make_column(
              "version_type", &DBVersionedObject::version_type
          ),
          sqlite_orm::foreign_key(&DBVersionedObject::object_id)
              .references(&DBObject::uuid)
      ),
      sqlite_orm::make_table(
          std::string(ACCESS_KEYS),
          sqlite_orm::make_column(
              "id", &DBAccessKey::id, sqlite_orm::autoincrement(),
              sqlite_orm::primary_key()
          ),
          sqlite_orm::make_column("access_key", &DBAccessKey::access_key),
          sqlite_orm::make_column("user_id", &DBAccessKey::user_id),
          sqlite_orm::foreign_key(&DBAccessKey::user_id)
              .references(&DBUser::user_id)
      ),
      sqlite_orm::make_table(
          std::string(LC_HEAD_TABLE),
          sqlite_orm::make_column(
              "lc_index", &DBOPLCHead::lc_index, sqlite_orm::primary_key()
          ),
          sqlite_orm::make_column("marker", &DBOPLCHead::marker),
          sqlite_orm::make_column("start_date", &DBOPLCHead::start_date)
      ),
      sqlite_orm::make_table(
          std::string(LC_ENTRIES_TABLE),
          sqlite_orm::make_column("lc_index", &DBOPLCEntry::lc_index),
          sqlite_orm::make_column("bucket_name", &DBOPLCEntry::bucket_name),
          sqlite_orm::make_column("start_time", &DBOPLCEntry::start_time),
          sqlite_orm::make_column("status", &DBOPLCEntry::status),
          sqlite_orm::primary_key(
              &DBOPLCEntry::lc_index, &DBOPLCEntry::bucket_name
          )
      ),
      sqlite_orm::make_table(
          std::string(MULTIPARTS_TABLE),
          sqlite_orm::make_column(
              "id", &DBMultipart::id, sqlite_orm::primary_key(),
              sqlite_orm::autoincrement()
          ),
          sqlite_orm::make_column("bucket_id", &DBMultipart::bucket_id),
          sqlite_orm::make_column("upload_id", &DBMultipart::upload_id),
          sqlite_orm::make_column("state", &DBMultipart::state),
          sqlite_orm::make_column(
              "state_change_time", &DBMultipart::state_change_time
          ),
          sqlite_orm::make_column("object_name", &DBMultipart::object_name),
          sqlite_orm::make_column("object_uuid", &DBMultipart::object_uuid),
          sqlite_orm::make_column("meta_str", &DBMultipart::meta_str),
          sqlite_orm::make_column("owner_id", &DBMultipart::owner_id),
          sqlite_orm::make_column(
              "owner_display_name", &DBMultipart::owner_display_name
          ),
          sqlite_orm::make_column("mtime", &DBMultipart::mtime),
          sqlite_orm::make_column("attrs", &DBMultipart::attrs),
          sqlite_orm::make_column(
              "placement_name", &DBMultipart::placement_name
          ),
          sqlite_orm::make_column(
              "placement_storage_class", &DBMultipart::placement_storage_class
          ),
          sqlite_orm::unique(&DBMultipart::upload_id),
          sqlite_orm::unique(&DBMultipart::bucket_id, &DBMultipart::upload_id),
          sqlite_orm::unique(&DBMultipart::object_uuid),
          sqlite_orm::foreign_key(&DBMultipart::bucket_id)
              .references(&DBBucket::bucket_id)
      ),
      sqlite_orm::make_table(
          std::string(MULTIPARTS_PARTS_TABLE),
          sqlite_orm::make_column(
              "id", &DBMultipartPart::id, sqlite_orm::primary_key(),
              sqlite_orm::autoincrement()
          ),
          sqlite_orm::make_column("upload_id", &DBMultipartPart::upload_id),
          sqlite_orm::make_column("part_num", &DBMultipartPart::part_num),
          sqlite_orm::make_column("size", &DBMultipartPart::size),
          sqlite_orm::make_column("etag", &DBMultipartPart::etag),
          sqlite_orm::make_column("mtime", &DBMultipartPart::mtime),
          sqlite_orm::unique(
              &DBMultipartPart::upload_id, &DBMultipartPart::part_num
          ),
          sqlite_orm::foreign_key(&DBMultipartPart::upload_id)
              .references(&DBMultipart::upload_id)
      )
  );
}

using Storage = decltype(_make_storage(""));

static void sqlite_error_callback(void* ctx, int error_code, const char* msg) {
  const auto cct = static_cast<CephContext*>(ctx);
  lderr(cct) << "[SQLITE] (" << error_code << ") " << msg << dendl;
}

static int sqlite_profile_callback(
    unsigned int reason, void* ctx, void* vstatement, void* runtime_ns
) {
  const auto cct = static_cast<CephContext*>(ctx);

  if (reason == SQLITE_TRACE_PROFILE) {
    const int64_t runtime = *static_cast<int64_t*>(runtime_ns) / 1000 / 1000;
    sqlite3_stmt* statement = static_cast<sqlite3_stmt*>(vstatement);
    const std::unique_ptr<char, void (*)(char*)> sql(
        sqlite3_expanded_sql(statement),
        [](char* p) { sqlite3_free(static_cast<void*>(p)); }
    );
    const sqlite3* db = sqlite3_db_handle(statement);

    if (sql) {
      lderr(cct) << "[SQLITE PROFILE] " << std::hex << db << " " << runtime
                 << "ms "
                 << "\"" << sql.get() << "\"" << dendl;
    } else {
      lderr(cct) << "[SQLITE PROFILE] " << std::hex << db << " " << runtime
                 << "ms "
                 << "\"" << sqlite3_sql(statement) << "\"" << dendl;
    }
  }

  return 0;
}

class DBConn {
 private:
  Storage storage;

 public:
  sqlite3* sqlite_db;
  CephContext* const cct;
  const bool profile_enabled;

  DBConn(CephContext* _cct)
      : storage(_make_storage(getDBPath(_cct))),
        cct(_cct),
        profile_enabled(_cct->_conf.get_val<bool>("rgw_sfs_sqlite_profile")) {
    sqlite3_config(SQLITE_CONFIG_LOG, &sqlite_error_callback, cct);
    storage.on_open = [this](sqlite3* db) {
      sqlite_db = db;

      sqlite3_extended_result_codes(db, 1);
      sqlite3_busy_timeout(db, 10000);
      sqlite3_exec(
          db,
          "PRAGMA journal_mode=WAL;"
          "PRAGMA synchronous=normal;"
          "PRAGMA temp_store = memory;"
          "PRAGMA mmap_size = 30000000000;",
          0, 0, 0
      );
      if (this->profile_enabled) {
        sqlite3_trace_v2(
            db, SQLITE_TRACE_PROFILE, &sqlite_profile_callback, this->cct
        );
      }
    };
    storage.open_forever();
    storage.busy_timeout(5000);
    maybe_upgrade_metadata();
    check_metadata_is_compatible();
    storage.sync_schema();
  }
  virtual ~DBConn() = default;

  DBConn(const DBConn&) = delete;
  DBConn& operator=(const DBConn&) = delete;

  inline auto get_storage() const { return storage; }

  static std::string getDBPath(CephContext* cct) {
    auto rgw_sfs_path = cct->_conf.get_val<std::string>("rgw_sfs_data_path");
    auto db_path =
        std::filesystem::path(rgw_sfs_path) / std::string(SCHEMA_DB_NAME);
    return db_path.string();
  }

  void check_metadata_is_compatible() const;
  void maybe_upgrade_metadata();
};

using DBConnRef = std::shared_ptr<DBConn>;

}  // namespace rgw::sal::sfs::sqlite
