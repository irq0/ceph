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
#include "object_conversions.h"
#include "../conversion_utils.h"

namespace rgw::sal::sfs::sqlite  {

DBOPObjectInfo get_rgw_object(const DBObject & object) {
  DBOPObjectInfo rgw_object;
  rgw_object.uuid.parse(object.object_id.c_str());
  rgw_object.bucket_id = object.bucket_id;
  rgw_object.name = object.name;
  assign_optional_value(object.size, rgw_object.size);
  assign_optional_value(object.etag, rgw_object.etag);
  assign_optional_value(object.mtime, rgw_object.mtime);
  assign_optional_value(object.set_mtime, rgw_object.set_mtime);
  assign_optional_value(object.delete_at_time, rgw_object.delete_at);
  assign_optional_value(object.attrs, rgw_object.attrs);
  assign_optional_value(object.acls, rgw_object.acls);
  return rgw_object;
}

DBObject get_db_object(const DBOPObjectInfo & object) {
  DBObject db_object;
  db_object.object_id = object.uuid.to_string();
  db_object.bucket_id = object.bucket_id;
  db_object.name = object.name;
  assign_db_value(object.size, db_object.size);
  assign_db_value(object.etag, db_object.etag);
  assign_db_value(object.mtime, db_object.mtime);
  assign_db_value(object.set_mtime, db_object.set_mtime);
  assign_db_value(object.delete_at, db_object.delete_at_time);
  assign_db_value(object.attrs, db_object.attrs);
  assign_db_value(object.acls, db_object.acls);
  return db_object;
}
}  // namespace rgw::sal::sfs::sqlite
