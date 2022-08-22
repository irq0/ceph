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

#include "rgw_common.h"

#include <string>

namespace rgw::sal::sfs::sqlite  {

using BLOB = std::vector<char>;

struct DBObject {
  std::string object_id;
  std::string bucket_name;
  std::string name;
  std::optional<size_t> size;
  std::optional<std::string> etag;
  std::optional<BLOB> mtime;
  std::optional<BLOB> set_mtime;
  std::optional<BLOB> delete_at_time;
  std::optional<BLOB> attrs;
  std::optional<BLOB> acls;
};

struct DBOPObjectInfo {
  uuid_d uuid;
  std::string bucket_name;
  std::string name;
  size_t size;
  std::string etag;
  ceph::real_time mtime;
  ceph::real_time set_mtime;
  ceph::real_time delete_at;
  rgw::sal::Attrs attrs;
  RGWAccessControlPolicy acls;
};

}  // namespace rgw::sal::sfs::sqlite
