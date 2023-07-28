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
#include "sfs_gc.h"

#include <filesystem>

#include "driver/sfs/types.h"
#include "multipart_types.h"
#include "rgw/driver/sfs/sqlite/sqlite_multipart.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"

namespace rgw::sal::sfs {

SFSGC::SFSGC(CephContext* _cctx, SFStore* _store) : cct(_cctx), store(_store) {
  worker = std::make_unique<GCWorker>(this, cct, this);
}

SFSGC::~SFSGC() {
  down_flag = true;
  if (worker->is_started()) {
    worker->stop();
    worker->join();
  }
}

int SFSGC::process() {
  // This is the method that does the garbage collection.

  // set the maximum number of objects we can delete in this iteration
  max_objects = cct->_conf->rgw_gc_max_objs;
  lsfs_dout(this, 10) << "garbage collection: processing with max_objects = "
                      << max_objects << dendl;

  // For now, delete only the objects with deleted bucket.
  process_deleted_buckets();
  return 0;
}

bool SFSGC::going_down() {
  return down_flag;
}

/*
 * The constructor must have finished before the worker thread can be created,
 * because otherwise the logging will dereference an invalid pointer, since the
 * SFSGC instance is a prefix provider for the logging in the worker thread
 */
void SFSGC::initialize() {
  worker->create("rgw_gc");
  down_flag = false;
}

bool SFSGC::suspended() {
  return suspend_flag;
}

void SFSGC::suspend() {
  suspend_flag = true;
}

void SFSGC::resume() {
  suspend_flag = false;
}

std::ostream& SFSGC::gen_prefix(std::ostream& out) const {
  return out << "garbage collection: ";
}

void SFSGC::process_deleted_buckets() {
  // permanently delete removed buckets and their objects and versions
  sqlite::SQLiteBuckets db_buckets(store->db_conn);
  auto deleted_buckets = db_buckets.get_deleted_buckets_ids();
  lsfs_dout(this, 10) << "deleted buckets found = " << deleted_buckets.size()
                      << dendl;
  for (auto const& bucket_id : deleted_buckets) {
    if (max_objects <= 0) {
      break;
    }
    delete_bucket(bucket_id);
  }
}

void SFSGC::delete_objects(const std::string& bucket_id) {
  sqlite::SQLiteObjects db_objs(store->db_conn);
  auto objects = db_objs.get_objects(bucket_id);
  for (auto const& object : objects) {
    if (max_objects <= 0) {
      break;
    }
    auto obj_instance =
        std::unique_ptr<Object>(Object::create_for_immediate_deletion(object));
    delete_object(*obj_instance.get());
  }
}

void SFSGC::delete_versioned_objects(const Object& object) {
  sqlite::SQLiteVersionedObjects db_ver_objs(store->db_conn);
  // get all versions. Including deleted ones
  auto versions =
      db_ver_objs.get_versioned_objects(object.path.get_uuid(), false);
  for (auto const& version : versions) {
    if (max_objects <= 0) {
      break;
    }

    Object to_be_deleted(object);
    to_be_deleted.version_id = version.id;
    delete_versioned_object(to_be_deleted);
  }
}

void SFSGC::delete_bucket(const std::string& bucket_id) {
  // delete the multiparts on the bucket first
  delete_multiparts(bucket_id);
  // then delete the objects of the bucket
  delete_objects(bucket_id);
  if (max_objects > 0) {
    sqlite::SQLiteBuckets db_buckets(store->db_conn);
    db_buckets.remove_bucket(bucket_id);
    lsfs_dout(this, 30) << "Deleted bucket: " << bucket_id << dendl;
    --max_objects;
  }
}

void SFSGC::delete_multiparts(const std::string& bucket_id) {
  sqlite::SQLiteMultipart db_mp(store->db_conn);
  int ret = db_mp.abort_multiparts_by_bucket_id(bucket_id);
  ceph_assert(ret >= 0);

  // now we can delete both the multipart uploads' parts, and the uploads themselves.

  // grab all multiparts
  auto mps = db_mp.list_multiparts_by_bucket_id(
      bucket_id, "", "", "", 10000, nullptr, true
  );
  if (mps.empty()) {
    lsfs_dout(this, 30) << "No multiparts to remove for bucket id " << bucket_id
                        << dendl;
    return;
  }

  for (const auto& mp : mps) {
    // grab all parts destination files
    auto parts = db_mp.get_parts(mp.upload_id);
    for (const auto& part : parts) {
      // delete on-disk part file
      MultipartPartPath pp(mp.object_uuid, part.part_num);
      auto p = pp.to_path();
      if (!std::filesystem::exists(p)) {
        continue;
      }
      std::filesystem::remove(p);
    }

    // then delete all parts from the db.
    db_mp.remove_parts(mp.upload_id);
  }

  // and then delete all multiparts from the db.
  db_mp.remove_multiparts_by_bucket_id(bucket_id);
}

void SFSGC::delete_object(const Object& object) {
  // delete its versions first
  delete_versioned_objects(object);
  if (max_objects > 0) {
    object.delete_object_metadata(store);
    object.delete_object_data(store, true);
    lsfs_dout(this, 30) << "Deleted object: " << object.path.get_uuid()
                        << dendl;
    --max_objects;
  }
}

void SFSGC::delete_versioned_object(const Object& object) {
  object.delete_object_version(store);
  object.delete_object_data(store, false);
  lsfs_dout(this, 30) << "Deleted version: (" << object.path.get_uuid() << ","
                      << object.version_id << ")" << dendl;
  --max_objects;
}

SFSGC::GCWorker::GCWorker(
    const DoutPrefixProvider* _dpp, CephContext* _cct, SFSGC* _gc
)
    : dpp(_dpp), cct(_cct), gc(_gc) {}

void* SFSGC::GCWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    lsfs_dout(dpp, 2) << "start" << dendl;

    if (!gc->suspended()) {
      int r = gc->process();
      if (r < 0) {
        lsfs_dout(
            dpp, 0
        ) << "ERROR: garbage collection process() returned error r="
          << r << dendl;
      }
      lsfs_dout(dpp, 2) << "stop" << dendl;
    }

    if (gc->going_down()) break;

    utime_t end = ceph_clock_now();
    end -= start;
    int secs = cct->_conf->rgw_gc_processor_period;
    secs -= end.sec();
    if (secs <= 0) {
      // in case the GC iteration took more time than the period
      secs = cct->_conf->rgw_gc_processor_period;
      ;
    }

    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(secs));
  } while (!gc->going_down());

  return nullptr;
}

void SFSGC::GCWorker::stop() {
  std::lock_guard l{lock};
  cond.notify_all();
}

}  //  namespace rgw::sal::sfs
