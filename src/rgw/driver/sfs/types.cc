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

#include <memory>
#include <string>

#include "rgw/rgw_sal_sfs.h"
#include "rgw/driver/sfs/types.h"
#include "rgw/driver/sfs/object_state.h"

namespace rgw::sal::sfs {

std::filesystem::path Object::get_storage_path() const {
  return path.to_path() / std::to_string(version_id);
}

void Object::metadata_init(SFStore *store, const std::string & bucket_id,
                           bool new_object, bool new_version) {
  sqlite::DBOPObjectInfo oinfo;
  oinfo.uuid = path.get_uuid();
  oinfo.bucket_id = bucket_id;
  oinfo.name = name;

  // This should probably be done in one single exclusive access to the
  // database, lest another operation happen in between and affect the version
  // we obtain. We might have to consider to create a mechanism of sorts to lock
  // the connection for exclusive write access for multiple operations.
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

void Object::metadata_change_version_state(SFStore *store, ObjectState state) {
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

void Object::metadata_flush_attrs(SFStore *store){
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(versioned_object.has_value());
  versioned_object->attrs = meta.attrs;
  db_versioned_objs.store_versioned_object(*versioned_object);
}

void Object::metadata_finish(SFStore *store) {
  sqlite::SQLiteObjects dbobjs(store->db_conn);
  auto db_object = dbobjs.get_object(path.get_uuid());
  ceph_assert(db_object.has_value());
  db_object->name = name;
  db_object->size = meta.size;
  db_object->etag = meta.etag;
  db_object->mtime = meta.mtime;
  db_object->set_mtime = meta.set_mtime;
  db_object->delete_at = meta.delete_at;
  dbobjs.store_object(*db_object);

  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto db_versioned_object = db_versioned_objs.get_versioned_object(version_id);
  ceph_assert(db_versioned_object.has_value());
  // TODO calculate checksum. Is it already calculated while writing?
  db_versioned_object->size = meta.size;
  db_versioned_object->creation_time = meta.mtime;
  db_versioned_object->object_state = ObjectState::COMMITTED;
  db_versioned_object->etag = meta.etag;
  db_versioned_object->attrs = meta.attrs;
  db_versioned_objs.store_versioned_object(*db_versioned_object);
}

int Object::delete_object_version(SFStore *store) {
  // remove object data
  std::filesystem::remove(store->get_data_path() / get_storage_path());

  // remove metadata
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  db_versioned_objs.remove_versioned_object(version_id);
  return 0;
}

void Object::delete_object(SFStore *store) {
  // remove object folder
  std::filesystem::remove(store->get_data_path() / path.to_path());
  // remove metadata
  sqlite::SQLiteObjects db_objs(store->db_conn);
  db_objs.remove_object(path.get_uuid());
}

bool Object::get_attr(const std::string &name, bufferlist& dest){
  auto iter = meta.attrs.find(name);
  if (iter != meta.attrs.end()) {
    dest = iter->second;
    return true;
  }
  return false;
}

void MultipartObject::_abort(const DoutPrefixProvider *dpp) {
  // assumes being called while holding the lock.
  ceph_assert(aborted);
  state = State::ABORTED;
  auto path = objref->path.to_path();
  if (std::filesystem::exists(path)) {
    // destroy part's contents
    if (dpp) {
      lsfs_dout(dpp, 10) << "remove part contents at " << path << dendl;
    }
    std::filesystem::remove(path);
  }
  objref.reset();
}

void MultipartObject::abort(const DoutPrefixProvider *dpp) {
  std::lock_guard l(lock);
  lsfs_dout(dpp, 10) << "abort part for upload id: " << upload_id
                     << ", state: " << state << dendl;
  if (state == State::ABORTED) {
    return;
  }

  aborted = true;
  if (state == State::INPROGRESS) {
    lsfs_dout(dpp, 10) << "part upload in progress, wait to abort." << dendl;
    return;
  }
  _abort(dpp);
}

void MultipartUpload::abort(const DoutPrefixProvider *dpp) {
  std::lock_guard l(parts_map_lock);
  lsfs_dout(dpp, 10) << "aborting multipart upload id: " << upload_id
                     << ", object: " << objref->name
                     << ", num parts: " << parts.size()
                     << dendl;

  state = State::ABORTED;
  for (const auto &[id, part] : parts) {
    part->abort(dpp);
  }
  parts.clear();
  objref.reset();
}

ObjectRef Bucket::get_or_create(const rgw_obj_key &key) {
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
  obj->metadata_init(store, info.bucket.bucket_id, new_object, create_new_version);

  return obj;
}

void Bucket::finish(const DoutPrefixProvider *dpp, const std::string &objname) {
  std::lock_guard l(obj_map_lock);


  // finished creating the object

  ObjectRef ref = objects[objname];
  _finish_object(ref);
}

void Bucket::_finish_object(ObjectRef ref) {
  sqlite::DBOPObjectInfo oinfo;
  oinfo.uuid = ref->path.get_uuid();
  oinfo.bucket_id = info.bucket.bucket_id;
  oinfo.name = ref->name;
  oinfo.size = ref->meta.size;
  oinfo.etag = ref->meta.etag;
  oinfo.mtime = ref->meta.mtime;
  oinfo.set_mtime = ref->meta.set_mtime;
  oinfo.delete_at = ref->meta.delete_at;
  ref->deleted = false;

  sqlite::SQLiteObjects dbobjs(store->db_conn);
  dbobjs.store_object(oinfo);
}

void Bucket::delete_object(ObjectRef objref, const rgw_obj_key & key) {
  std::lock_guard l(obj_map_lock);

  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  // get the last available version to make a copy changing the object state to DELETED
  auto last_version = db_versioned_objs.get_last_versioned_object(objref->path.get_uuid());
  ceph_assert(last_version.has_value());
  if (last_version->object_state == ObjectState::DELETED) {
    _undelete_object(objref, key, db_versioned_objs, *last_version);
  } else {
    last_version->object_state = ObjectState::DELETED;
    last_version->deletion_time = ceph::real_clock::now();

    if (last_version->version_id != "") {
      // generate a new version id
      #define OBJ_INSTANCE_LEN 32
      char buf[OBJ_INSTANCE_LEN + 1];
      gen_rand_alphanumeric_no_underscore(store->ceph_context(), buf, OBJ_INSTANCE_LEN);
      last_version->version_id = std::string(buf);
      objref->instance = last_version->version_id;
      // insert a new deleted version
      db_versioned_objs.insert_versioned_object(*last_version);
    } else {
      db_versioned_objs.store_versioned_object(*last_version);
    }
    objref->deleted = true;
  }
}

std::string Bucket::create_non_existing_object_delete_marker(
                                                      const rgw_obj_key & key) {
  std::lock_guard l(obj_map_lock);

  auto obj = std::make_shared<Object>(key);
  obj->deleted = true;
  objects[key.name] = obj;
  obj->metadata_init(store, info.bucket.bucket_id, true, false);
  // create the delete marker
  // generate a new version id
  #define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];
  gen_rand_alphanumeric_no_underscore(store->ceph_context(), buf, OBJ_INSTANCE_LEN);
  auto new_version_id = std::string(buf);
  sqlite::DBOPVersionedObjectInfo version_info;
  version_info.object_id = obj->path.get_uuid();
  version_info.object_state = ObjectState::DELETED;
  version_info.version_id = new_version_id;
  version_info.deletion_time = ceph::real_clock::now();
  sqlite::SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  obj->version_id = db_versioned_objs.insert_versioned_object(version_info);

  return new_version_id;
}

void Bucket::_undelete_object(ObjectRef objref, const rgw_obj_key & key,
                              sqlite::SQLiteVersionedObjects & sqlite_versioned_objects,
                              sqlite::DBOPVersionedObjectInfo & last_version) {
  if (!last_version.version_id.empty()) {
    // versioned object
    // only remove the delete marker if the requested version id is the last one
    if (!key.instance.empty() && (key.instance == last_version.version_id)) {
      // remove the delete marker
      sqlite_versioned_objects.remove_versioned_object(last_version.id);
      // get the previous id
      auto previous_version = sqlite_versioned_objects.get_last_versioned_object(objref->path.get_uuid());
      if (previous_version.has_value()) {
        objref->instance = previous_version->version_id;
        objref->deleted = false;
      } else {
        // all versions were removed for this object
        objects.erase(key.name);
      }
    }
  } else {
    // non-versioned object
    // just remove the delete marker in the version and store
    last_version.object_state = ObjectState::COMMITTED;
    last_version.deletion_time = ceph::real_clock::now();
    sqlite_versioned_objects.store_versioned_object(last_version);
    objref->deleted = false;
  }
}

void Bucket::_refresh_objects() {
  sqlite::SQLiteObjects objs(store->db_conn);
  auto existing = objs.get_objects(info.bucket.bucket_id);
  for (const auto &obj : existing) {
    sqlite::SQLiteVersionedObjects objs_versions(store->db_conn);
    auto last_version = objs_versions.get_last_versioned_object(obj.uuid);
    if (last_version.has_value()) {
      ObjectRef ref = std::make_shared<Object>(obj.name, obj.uuid, false);
      ref->meta.size = obj.size;
      ref->meta.etag = obj.etag;
      ref->meta.mtime = obj.mtime;
      ref->meta.set_mtime = obj.set_mtime;
      ref->meta.delete_at = obj.delete_at;
      ref->meta.attrs = last_version->attrs;
      ref->version_id = last_version->id;
      ref->instance = last_version->version_id;
      ref->deleted = (last_version->object_state == ObjectState::DELETED);

      objects[obj.name] = ref;
    }
  }
}

} // ns rgw::sal::sfs
