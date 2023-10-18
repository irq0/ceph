/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "sqlite_list.h"

#include <limits>

#include "SQLiteCpp/Database.h"
#include "SQLiteCpp/Statement.h"
#include "rgw/driver/sfs/sqlite/conversion_utils.h"
#include "rgw/driver/sfs/sqlite/objects/object_definitions.h"
#include "rgw/driver/sfs/sqlite/versioned_object/versioned_object_definitions.h"
#include "rgw/driver/sfs/version_type.h"
#include "rgw_obj_types.h"
#include "sqlite_orm.h"

using namespace sqlite_orm;
namespace rgw::sal::sfs::sqlite {

SQLiteList::SQLiteList(DBConnRef _conn) : conn(_conn) {}

bool SQLiteList::objects(
    const std::string& bucket_id, const std::string& prefix,
    const std::string& start_after_object_name, size_t max,
    std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available
) const {
  ceph_assert(!bucket_id.empty());

  // more available logic: request one more than max. if we get that
  // much set out_more_available, but return only up to max
  ceph_assert(max < std::numeric_limits<size_t>::max());
  const size_t query_limit = max + 1;

  // ListBucket does not care about versions/instances. don't populate
  // key.instance
  auto storage = conn->get_storage();
  auto rows = storage.select(
      columns(
          &DBObject::name, &DBVersionedObject::mtime, &DBVersionedObject::etag,
          sum(&DBVersionedObject::size)
      ),
      inner_join<DBVersionedObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED) and
          is_equal(&DBObject::bucket_id, bucket_id) and
          greater_than(&DBObject::name, start_after_object_name) and
          prefix_to_like(&DBObject::name, prefix)
      ),
      group_by(&DBVersionedObject::object_id),
      having(is_equal(
          sqlite_orm::max(&DBVersionedObject::version_type),
          VersionType::REGULAR
      )),
      order_by(&DBObject::name), limit(query_limit)
  );
  ceph_assert(rows.size() <= static_cast<size_t>(query_limit));
  const size_t return_limit = std::min(max, rows.size());
  out.reserve(return_limit);
  for (size_t i = 0; i < return_limit; i++) {
    const auto& row = rows[i];
    rgw_bucket_dir_entry e;
    e.key.name = std::get<0>(row);
    e.meta.mtime = std::get<1>(row);
    e.meta.etag = std::get<2>(row);
    e.meta.size = static_cast<uint64_t>(*std::get<3>(row));
    e.meta.accounted_size = e.meta.size;
    out.emplace_back(e);
  }
  if (out_more_available) {
    *out_more_available = rows.size() == query_limit;
  }
  return true;
}

static uint16_t to_dentry_flag(VersionType vt, bool latest) {
  uint16_t result = rgw_bucket_dir_entry::FLAG_VER;
  if (latest) {
    result |= rgw_bucket_dir_entry::FLAG_CURRENT;
  }
  if (vt == VersionType::DELETE_MARKER) {
    result |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
  }
  return result;
}

bool SQLiteList::versions_orm(
    const std::string& bucket_id, const std::string& prefix,
    const std::string& start_after_object_name, size_t max,
    std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available
) const {
  ceph_assert(!bucket_id.empty());

  // more available logic: request one more than max. if we get that
  // much set out_more_available, but return only up to max
  ceph_assert(max < std::numeric_limits<size_t>::max());
  const uint32_t query_limit = max + 1;

  auto storage = conn->get_storage();
  auto rows = storage.select(
      columns(
          &DBObject::name, &DBVersionedObject::version_id,
          &DBVersionedObject::mtime, &DBVersionedObject::etag,
          &DBVersionedObject::size, &DBVersionedObject::version_type,
          is_equal(
              // IsLatest logic
              // - Use the id as secondary condition if multiple version
              // with same max(commit_time) exists
              sqlite_orm::select(
                  &DBVersionedObject::id, from<DBVersionedObject>(),
                  where(
                      is_equal(
                          &DBObject::uuid, &DBVersionedObject::object_id
                      ) and
                      is_equal(
                          &DBVersionedObject::object_state,
                          ObjectState::COMMITTED
                      )
                  ),
                  multi_order_by(
                      order_by(&DBVersionedObject::commit_time).desc(),
                      order_by(&DBVersionedObject::id).desc()
                  ),
                  limit(1)
              ),
              &DBVersionedObject::id
          )
      ),
      inner_join<DBVersionedObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED) and
          is_equal(&DBObject::bucket_id, bucket_id) and
          greater_than(&DBObject::name, start_after_object_name) and
          prefix_to_like(&DBObject::name, prefix)
      ),
      // Sort:
      // names a-Z
      // first delete markers, then versions - (See: LC CurrentExpiration)
      // newest to oldest version
      multi_order_by(
          order_by(&DBObject::name).asc(),
          order_by(&DBVersionedObject::commit_time).desc(),
          order_by(&DBVersionedObject::id).desc()
      ),
      limit(query_limit)
  );

  ceph_assert(rows.size() <= static_cast<size_t>(query_limit));
  const size_t return_limit = std::min(max, rows.size());
  out.reserve(return_limit);
  for (size_t i = 0; i < return_limit; i++) {
    const auto& row = rows[i];
    rgw_bucket_dir_entry e;
    e.key.name = std::get<0>(row);
    e.key.instance = std::get<1>(row);
    e.meta.mtime = std::get<2>(row);
    e.meta.etag = std::get<3>(row);
    e.meta.size = std::get<4>(row);
    e.meta.accounted_size = e.meta.size;
    e.flags = to_dentry_flag(std::get<5>(row), std::get<6>(row));
    out.emplace_back(e);
  }
  if (out_more_available) {
    *out_more_available = rows.size() == query_limit;
  }
  return true;

}
bool SQLiteList::versions_sqlitecpp(
    const std::string& bucket_id, const std::string& prefix,
    const std::string& start_after_object_name, size_t max,
    std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available
) const {
  ceph_assert(!bucket_id.empty());

  // more available logic: request one more than max. if we get that
  // much set out_more_available, but return only up to max
  ceph_assert(max < std::numeric_limits<size_t>::max());
  const uint32_t query_limit = max + 1;

  SQLite::Database db = conn->get_sqlitecpp();
  SQLite::Statement query{
    db, R"sql(
      SELECT
         o.name, vo.version_id, vo.mtime, vo.etag, vo.size, vo.version_type,
         (vo.id = ( SELECT id FROM versioned_objects
           WHERE object_id = o.uuid
           AND object_state = ?
           ORDER BY commit_time desc, id desc
           LIMIT 1
         )) AS is_latest
      FROM objects as o
      INNER JOIN versioned_objects as vo
      ON (o.uuid = vo.object_id)
      WHERE vo.object_state = ?
      AND o.bucket_id = ?
      AND o.name > ?
      AND o.name LIKE ? ESCAPE '\'
      ORDER BY o.name ASC,
        vo.commit_time DESC,
        vo.id DESC
      LIMIT ?)sql"};

  query.bind(1, static_cast<int>(ObjectState::COMMITTED));
  query.bind(2, static_cast<int>(ObjectState::COMMITTED));
  query.bind(3, bucket_id);
  query.bind(4, start_after_object_name);
  query.bind(6, query_limit);
  std::string like_expr;
  like_expr.reserve(prefix.length() + 10);
  for (const char c : prefix) {
    switch (c) {
      case '%':
      case '_':
        like_expr.push_back('\\');
      default:
        like_expr.push_back(c);
    }
  }
  like_expr.push_back('%');
  query.bind(5, like_expr);

  out.reserve(max);
  while (query.executeStep() && out.size() < max) {
    rgw_bucket_dir_entry e;
    e.key.name = query.getColumn(0).getString();
    e.key.instance = query.getColumn(1).getString();
    e.meta.mtime =
        ceph::real_time(std::chrono::nanoseconds(query.getColumn(2).getInt64())
        );
    e.meta.etag = query.getColumn(3).getString();
    e.meta.size = query.getColumn(4).getInt64();
    e.meta.accounted_size = e.meta.size;
    e.flags = to_dentry_flag(
        static_cast<VersionType>(query.getColumn(5).getInt()),
        query.getColumn(6).getInt()
    );
    out.emplace_back(e);
  }
  if (out_more_available) {
    *out_more_available = !query.isDone();
  }
  return true;
}

bool SQLiteList::versions(
    const std::string& bucket_id, const std::string& prefix,
    const std::string& start_after_object_name, size_t max,
    std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available
) const {
  return
      SQLiteList::versions_sqlitecpp(bucket_id, prefix, start_after_object_name, max, out, out_more_available);
}

void SQLiteList::roll_up_common_prefixes(
    const std::string& find_after_prefix, const std::string& delimiter,
    const std::vector<rgw_bucket_dir_entry>& objects,
    std::map<std::string, bool>& out_common_prefixes,
    std::vector<rgw_bucket_dir_entry>& out_objects
) const {
  const size_t find_after_pos = find_after_prefix.length();
  const size_t delim_len = delimiter.length();
  if (delimiter.empty()) {
    out_objects = objects;
    return;
  }
  const std::string* prefix{nullptr};  // Last added prefix
  for (size_t i = 0; i < objects.size(); i++) {
    const std::string& name = objects[i].key.name;
    // Same prefix -> skip
    if (prefix != nullptr && name.starts_with(*prefix)) {
      continue;
    }
    if (name.starts_with(find_after_prefix)) {
      // Found delim -> add, remember prefix
      auto delim_pos = name.find(delimiter, find_after_pos);
      if (delim_pos != name.npos) {
        prefix = &out_common_prefixes
                      .emplace(name.substr(0, delim_pos + delim_len), true)
                      .first->first;
        continue;
      }
    }
    // Not found -> next
    out_objects.push_back(objects[i]);
  }
}

}  // namespace rgw::sal::sfs::sqlite
