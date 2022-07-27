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

#include "rgw/driver/sfs/types.h"

#include <memory>
#include <string>

#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/rgw_sal_sfs.h"

namespace rgw::sal::sfs {

void Bucket::finish(const DoutPrefixProvider* dpp, const std::string& objname) {
  std::lock_guard l(obj_map_lock);

  auto it = creating.find(objname);
  if (it == creating.end()) {
    return;
  }

  // finished creating the object
  ceph_assert(objects.count(objname) == 0);
  objects[objname] = creating[objname];
  creating.erase(objname);

  ObjectRef ref = objects[objname];
  sqlite::DBOPObjectInfo oinfo;
  oinfo.uuid = ref->path.get_uuid();
  oinfo.bucket_name = name;
  oinfo.name = objname;
  oinfo.size = ref->meta.size;
  oinfo.etag = ref->meta.etag;
  oinfo.mtime = ref->meta.mtime;
  oinfo.set_mtime = ref->meta.set_mtime;
  oinfo.delete_at = ref->meta.delete_at;
  oinfo.attrs = ref->meta.attrs;

  sqlite::SQLiteObjects dbobjs(store->db_conn);
  dbobjs.store_object(oinfo);
}

void Bucket::_refresh_objects() {
  sqlite::SQLiteObjects objs(store->db_conn);
  auto existing = objs.get_objects(name);
  for (const auto& obj : existing) {
    ObjectRef ref = std::make_shared<Object>(obj.name, obj.uuid, false);
    ref->meta.size = obj.size;
    ref->meta.etag = obj.etag;
    ref->meta.mtime = obj.mtime;
    ref->meta.set_mtime = obj.set_mtime;
    ref->meta.delete_at = obj.delete_at;
    ref->meta.attrs = obj.attrs;
    objects[obj.name] = ref;
  }
}

}  // namespace rgw::sal::sfs
