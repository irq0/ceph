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

#include "driver/sfs/bucket_mgr.h"

#include <filesystem>
#include <memory>

#include "driver/sfs/object.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

void BucketMgr::new_bucket(const DoutPrefixProvider* dpp, SFSBucket* bucket) {
  ceph_assert(bucket->get_name() == bucket_name);
  std::lock_guard l{commit_lock};
  write_object_map();
  load_object_map();
}

std::map<std::string, std::string> BucketMgr::get_objects() {
  std::lock_guard l{commit_lock};
  std::map<std::string, std::string> ret(objects_map);
  return ret;
}

void BucketMgr::write_object_map() {
  version_t new_version = object_map_version + 1;
  std::map<std::string, std::string> new_map(objects_map);

  ldout(cct, 10) << "bucket_mgr::write_object_map > new objects: "
                 << new_objects.size() << dendl;
  for (const auto& obj : new_objects) {
    ldout(cct, 10) << "bucket_mgr::write_object_map > object name: "
                   << obj->get_name() << ", key: " << obj->get_key() << dendl;
    std::string hash = store->hash_rgw_obj_key(obj->get_key());
    new_map.insert(make_pair(obj->get_name(), hash));
  }

  for (auto const& obj : rm_objects) {
    ldout(cct, 10) << "bucket_mgr::write_object_map > remove object name: "
                   << obj->get_name() << ", key: " << obj->get_key() << dendl;
    new_map.erase(obj->get_name());
  }

  bufferlist bl;
  ceph::encode(new_version, bl);
  ceph::encode(new_map, bl);

  auto path = store->bucket_path(bucket_name);
  auto obj_map_path = path / "_objects.map";
  auto new_map_path = path / ("_objects.map.v" + std::to_string(new_version));
  // ensure we did not go back in time.
  ceph_assert(!std::filesystem::exists(new_map_path));

  ldout(cct, 10) << "version " << new_version << " with " << objects_map.size()
                 << " objects to " << new_map_path << dendl;
  bl.write_file(new_map_path.c_str());
  std::filesystem::remove(obj_map_path);
  std::filesystem::create_symlink(new_map_path, obj_map_path);
  new_objects.clear();
}

void BucketMgr::load_object_map() {
  ldout(cct, 10) << "load objects map for bucket " << bucket_name << dendl;
  auto path = store->bucket_path(bucket_name);
  auto obj_map_path = path / "_objects.map";

  if (!std::filesystem::exists(obj_map_path)) {
    ldout(cct, 10) << "object map does not exist for bucket " << bucket_name
                   << ": maybe has not been initted." << dendl;
    return;
  }

  bufferlist bl;
  std::string err;
  bl.read_file(obj_map_path.c_str(), &err);
  if (!err.empty()) {
    ldout(cct, 0) << "unable to load object map for bucket " << bucket_name
                  << ": " << err << dendl;
    ceph_abort("unable to load object map for bucket");
  }

  auto it = bl.cbegin();
  ceph::decode(object_map_version, it);
  ceph::decode(objects_map, it);

  ldout(cct, 10) << "loaded object map version " << object_map_version
                 << " with " << objects_map.size() << " objects" << dendl;
}

}  // namespace rgw::sal