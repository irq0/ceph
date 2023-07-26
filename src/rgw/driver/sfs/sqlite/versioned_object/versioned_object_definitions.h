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

#include <ranges>
#include <string>

#include "common/iso_8601.h"
#include "rgw/driver/sfs/object_state.h"
#include "rgw/driver/sfs/sqlite/bindings/blob.h"
#include "rgw/driver/sfs/sqlite/bindings/enum.h"
#include "rgw/driver/sfs/sqlite/bindings/real_time.h"
#include "rgw/driver/sfs/version_type.h"
#include "rgw/rgw_common.h"
#include "rgw_common.h"

#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

namespace rgw::sal::sfs::sqlite {

struct DBVersionedObject {
  uint id;
  uuid_d object_id;
  std::string checksum;
  size_t size;
  ceph::real_time create_time;
  ceph::real_time delete_time;
  ceph::real_time commit_time;
  ceph::real_time mtime;
  rgw::sal::sfs::ObjectState object_state;
  std::string version_id;
  std::string etag;
  rgw::sal::Attrs attrs;
  VersionType version_type = rgw::sal::sfs::VersionType::REGULAR;
};

using DBObjectsListItem = std::tuple<
    decltype(DBObject::uuid), decltype(DBObject::name),
    decltype(DBVersionedObject::version_id),
    std::unique_ptr<decltype(DBVersionedObject::commit_time)>,
    std::unique_ptr<decltype(DBVersionedObject::id)>,
    decltype(DBVersionedObject::size), decltype(DBVersionedObject::etag),
    decltype(DBVersionedObject::mtime),
    decltype(DBVersionedObject::delete_time),
    decltype(DBVersionedObject::attrs),
    decltype(DBVersionedObject::version_type),
    decltype(DBVersionedObject::object_state)>;

using DBObjectsListItems = std::vector<DBObjectsListItem>;

/// DBObjectsListItem helpers
inline decltype(DBObject::uuid) get_uuid(const DBObjectsListItem& item) {
  return std::get<0>(item);
}

inline decltype(DBObject::name) get_name(const DBObjectsListItem& item) {
  return std::get<1>(item);
}

inline decltype(DBVersionedObject::version_id) get_version_id(
    const DBObjectsListItem& item
) {
  return std::get<2>(item);
}

inline decltype(DBVersionedObject::id) get_id(const DBObjectsListItem& item) {
  return *(std::get<4>(item));
}

inline decltype(DBVersionedObject::size) get_size(const DBObjectsListItem& item
) {
  return std::get<5>(item);
}

inline decltype(DBVersionedObject::etag) get_etag(const DBObjectsListItem& item
) {
  return std::get<6>(item);
}

inline decltype(DBVersionedObject::mtime) get_mtime(
    const DBObjectsListItem& item
) {
  return std::get<7>(item);
}

inline decltype(DBVersionedObject::delete_time) get_delete_time(
    const DBObjectsListItem& item
) {
  return std::get<8>(item);
}

inline decltype(DBVersionedObject::attrs) get_attrs(
    const DBObjectsListItem& item
) {
  return std::get<9>(item);
}

inline decltype(DBVersionedObject::version_type) get_version_type(
    const DBObjectsListItem& item
) {
  return std::get<10>(item);
}

inline decltype(DBVersionedObject::object_state) get_object_state(
    const DBObjectsListItem& item
) {
  return std::get<11>(item);
}

}  // namespace rgw::sal::sfs::sqlite

inline std::ostream& operator<<(
    std::ostream& out, const rgw::sal::sfs::sqlite::DBVersionedObject& o
) {
  fmt::print(
      out,
      "DBVersionedObject("
      "id:{} oid:{} vid:{} state:{} size:{} del:{} creat:{} "
      "com:{} mtime:{} etag:{} attr_keys:{})",
      o.id, o.object_id.to_string(), o.version_id,
      rgw::sal::sfs::str_object_state(o.object_state), o.size,
      to_iso_8601(o.delete_time), to_iso_8601(o.create_time),
      to_iso_8601(o.commit_time), to_iso_8601(o.mtime), o.etag,
      fmt::join(std::views::keys(o.attrs), ", ")
  );
  return out;
}

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<rgw::sal::sfs::sqlite::DBVersionedObject>
    : fmt::ostream_formatter {};
#endif
