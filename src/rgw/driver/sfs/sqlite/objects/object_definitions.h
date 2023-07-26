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

#include <string>

#include "rgw/driver/sfs/sqlite/bindings/uuid_d.h"
#include "rgw_common.h"

namespace rgw::sal::sfs::sqlite {

struct DBObject {
  uuid_d uuid;
  std::string bucket_id;
  std::string name;
};

}  // namespace rgw::sal::sfs::sqlite

inline std::ostream& operator<<(
    std::ostream& out, const rgw::sal::sfs::sqlite::DBObject& o
) {
  return out << "DBObject("
             << "uuid:" << o.uuid << " bucket_id:" << o.bucket_id
             << " name:" << o.name << ")";
}

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<rgw::sal::sfs::sqlite::DBObject>
    : fmt::ostream_formatter {};
#endif
