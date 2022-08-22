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

#include "rgw/driver/sfs/object_state.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw_sal_sfs.h"

namespace rgw::sal::sfs {

std::filesystem::path Object::get_storage_path() const {
  return path.to_path() / std::to_string(version_id);
}

void Object::metadata_init(
    SFStore* store, const std::string& bucket_name, bool new_object,
    bool new_version
) {
  sqlite::DBOPObjectInfo oinfo;
  oinfo.uuid = path.get_uuid();
  oinfo.bucket_name = bucket_name;
  oinfo.name = name;

  if (new_object) {
    sqlite::SQLiteObjects dbobjs(store->db_conn);
    dbobjs.store_object(oinfo);
  }

  if (new_version) {
    sqlite::DBOPVersionedObjectInfo version_info;
    version_info.object_id = path.get_uuid();
    version_info.object_state = ObjectState::OPEN;
    version_info.version_id = instance;
    sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
    version_id = db_versioned_objs.insert_versioned_object(version_info);
  }
}

void Object::metadata_change_version_state(SFStore* store, ObjectState state) {
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(versioned_object.has_value());
  versioned_object->object_state = state;
  if (state == ObjectState::DELETED) {
    deleted = true;
    versioned_object->deletion_time = ceph::real_clock::now();
  }
  db_versioned_objs.store_versioned_object(*versioned_object);
}

void Object::metadata_finish(SFStore* store) {
  sqlite::SQLiteObjects dbobjs(store->db_conn);
  auto db_object = dbobjs.get_object(path.get_uuid());
  ceph_assert(db_object.has_value());
  db_object->size = meta.size;
  db_object->etag = meta.etag;
  db_object->mtime = meta.mtime;
  db_object->set_mtime = meta.set_mtime;
  db_object->delete_at = meta.delete_at;
  db_object->attrs = meta.attrs;
  dbobjs.store_object(*db_object);

  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto db_versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(db_versioned_object.has_value());
  // TODO calculate checksum. Is it already calculated while writing?
  db_versioned_object->size = meta.size;
  db_versioned_object->creation_time = meta.mtime;
  db_versioned_object->object_state = ObjectState::COMMITTED;
  db_versioned_objs.store_versioned_object(*db_versioned_object);
}

ObjectRef Bucket::get_or_create(const rgw_obj_key& key) {
  std::lock_guard l(obj_map_lock);
  ObjectRef obj = nullptr;
  auto new_object = true;
  auto create_new_version = true;
  auto it = objects.find(key.name);
  if (it != objects.end()) {
    obj = it->second;
    new_object = false;
    if (key.instance.empty() || key.instance == obj->instance) {
      create_new_version = false;
    } else {
      obj->instance = key.instance;
    }
  } else {
    obj = std::make_shared<Object>(key);
    objects[key.name] = obj;
  }
  obj->metadata_init(store, name, new_object, create_new_version);

  return obj;
}

void Bucket::finish(const DoutPrefixProvider* dpp, const std::string& objname) {
  std::lock_guard l(obj_map_lock);

  // finished creating the object

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

void Bucket::delete_object(ObjectRef objref) {
  std::lock_guard l(obj_map_lock);

  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  // get the last available version to make a copy changing the object state to DELETED
  auto last_version =
      db_versioned_objs.get_last_versioned_object(objref->path.get_uuid());
  ceph_assert(last_version.has_value());
  last_version->object_state = ObjectState::DELETED;
  last_version->deletion_time = ceph::real_clock::now();

  if (last_version->version_id != "") {
// generate a new version id
#define OBJ_INSTANCE_LEN 32
    char buf[OBJ_INSTANCE_LEN + 1];
    gen_rand_alphanumeric_no_underscore(
        store->ceph_context(), buf, OBJ_INSTANCE_LEN
    );
    last_version->version_id = std::string(buf);
    objref->instance = last_version->version_id;
    // insert a new deleted version
    db_versioned_objs.insert_versioned_object(*last_version);
  } else {
    db_versioned_objs.store_versioned_object(*last_version);
  }
  objref->deleted = true;
}

void Bucket::_refresh_objects() {
  sqlite::SQLiteObjects objs(store->db_conn);
  auto existing = objs.get_objects(name);
  for (const auto& obj : existing) {
    sqlite::SQLiteVersionedObjects objs_versions(store->db_conn);
    auto last_version = objs_versions.get_last_versioned_object(obj.uuid);
    ceph_assert(last_version.has_value());
    ObjectRef ref = std::make_shared<Object>(obj.name, obj.uuid, false);
    ref->meta.size = obj.size;
    ref->meta.etag = obj.etag;
    ref->meta.mtime = obj.mtime;
    ref->meta.set_mtime = obj.set_mtime;
    ref->meta.delete_at = obj.delete_at;
    ref->meta.attrs = obj.attrs;
    ref->version_id = last_version->id;
    ref->instance = last_version->version_id;
    ref->deleted = (last_version->object_state == ObjectState::DELETED);

    objects[obj.name] = ref;
  }
}

}  // namespace rgw::sal::sfs
