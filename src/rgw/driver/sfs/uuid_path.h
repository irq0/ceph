// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#ifndef RGW_STORE_SFS_UUID_PATH_H
#define RGW_STORE_SFS_UUID_PATH_H

#include <filesystem>
#include "include/ceph_assert.h"
#include "include/uuid.h"

namespace rgw::sal::sfs {

class UUIDPath {

  uuid_d uuid;
  std::string first;
  std::string second;
  std::string fname;

 public:
  UUIDPath(uuid_d &_uuid) : uuid(_uuid) {
    std::string uuidstr = _uuid.to_string();
    first = uuidstr.substr(0, 2);
    second = uuidstr.substr(2, 2);
    fname = uuidstr.substr(4);
  }

  std::filesystem::path to_path() const {
    ceph_assert(!uuid.uuid.is_nil());
    return
      std::filesystem::path(first) /
      std::filesystem::path(second) /
      std::filesystem::path(fname);
  }

  bool match(const UUIDPath &other) {
    return uuid == other.uuid;
  }

  static UUIDPath create() {
    uuid_d uuid;
    uuid.generate_random();
    return UUIDPath(uuid);
  }
};

}

inline std::ostream& operator<<(
  std::ostream& out, const rgw::sal::sfs::UUIDPath& p
) {
  return out << p.to_path();
}

#endif // RGW_STORE_SFS_UUID_PATH_H