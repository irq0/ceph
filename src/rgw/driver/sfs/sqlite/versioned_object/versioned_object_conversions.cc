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
#include "versioned_object_conversions.h"
#include "../conversion_utils.h"

namespace rgw::sal::sfs::sqlite  {

ObjectState get_object_state(uint state) {
  if (state > static_cast<uint>(ObjectState::LAST_VALUE)) {
    throw(std::runtime_error("incorrect state found (" + std::to_string(state) + ")"));
  }
  return static_cast<ObjectState>(state);
}

uint get_uint_object_state(ObjectState state) {
  return static_cast<uint>(state);
}

DBOPVersionedObjectInfo get_rgw_versioned_object(const DBVersionedObject & object) {
  DBOPVersionedObjectInfo rgw_object;
  rgw_object.id = object.id;
  rgw_object.object_id.parse(object.object_id.c_str());
  rgw_object.checksum = object.checksum;
  decode_blob(object.deletion_time, rgw_object.deletion_time);
  rgw_object.size = object.size;
  decode_blob(object.creation_time, rgw_object.creation_time);
  rgw_object.object_state = get_object_state(object.object_state);
  rgw_object.version_id = object.version_id;
  rgw_object.etag = object.etag;
  return rgw_object;
}

DBVersionedObject get_db_versioned_object(const DBOPVersionedObjectInfo & object) {
  DBVersionedObject db_object;
  db_object.id = object.id;
  db_object.object_id = object.object_id.to_string();
  db_object.checksum = object.checksum;
  encode_blob(object.deletion_time, db_object.deletion_time);
  db_object.size = object.size;
  encode_blob(object.creation_time, db_object.creation_time);
  db_object.object_state = get_uint_object_state(object.object_state);
  db_object.version_id = object.version_id;
  db_object.etag = object.etag;
  return db_object;
}
}  // namespace rgw::sal::sfs::sqlite
