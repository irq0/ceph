// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "ObjectStore.h"

#include <ctype.h>

#include <sstream>

#include "FileStore.h"
#include "common/Formatter.h"
#include "common/safe_io.h"

using std::string;

std::unique_ptr<ObjectStore> ObjectStore::create(CephContext* cct,
                                                 const string& data,
                                                 const string& journal,
                                                 osflagbits_t flags) {
  return std::make_unique<FileStore>(cct, data, journal, flags);
}

int ObjectStore::probe_block_device_fsid(CephContext* cct, const string& path,
                                         uuid_d* fsid) {
  int r;

  // okay, try FileStore (journal).
  r = FileStore::get_block_device_fsid(cct, path, fsid);
  if (r == 0) {
    lgeneric_dout(cct, 0) << __func__ << " " << path << " is filestore, "
                          << *fsid << dendl;
    return r;
  }
  return -EINVAL;
}
