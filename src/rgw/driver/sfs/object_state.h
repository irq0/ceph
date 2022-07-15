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
#ifndef RGW_SFS_OBJECT_STATE_H
#define RGW_SFS_OBJECT_STATE_H

namespace rgw::sal {

enum class ObjectState {
    OPEN = 0,
    WRITING,
    COMMITTED,
    LOCKED,
    DELETED,
    LAST_VALUE = DELETED
};

}  // namespace rgw::sal

#endif  // RGW_SFS_OBJECT_STATE_H
