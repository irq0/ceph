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

#include "versioned_object_definitions.h"

namespace rgw::sal::sfs::sqlite  {

// Functions that convert DB type to RGW type (and vice-versa)
DBOPVersionedObjectInfo get_rgw_versioned_object(const DBVersionedObject & object);
DBVersionedObject get_db_versioned_object(const DBOPVersionedObjectInfo & object);

}  // namespace rgw::sal::sfs::sqlite
