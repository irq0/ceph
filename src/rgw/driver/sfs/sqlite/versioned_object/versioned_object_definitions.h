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

#include "rgw/driver/sfs/object_state.h"
#include "rgw/rgw_common.h"
#include "rgw_common.h"

namespace rgw::sal::sfs::sqlite {

using BLOB = std::vector<char>;

struct DBVersionedObject {
  uint id;
  std::string object_id;
  std::string checksum;
  BLOB deletion_time;
  size_t size;
  BLOB creation_time;
  uint object_state;
};

struct DBOPVersionedObjectInfo {
  uint id;
  uuid_d object_id;
  std::string checksum;
  ceph::real_time deletion_time;
  size_t size;
  ceph::real_time creation_time;
  ObjectState object_state;
};

}  // namespace rgw::sal::sfs::sqlite
