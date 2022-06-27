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
#include "driver/sfs/zone.h"

#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

ZoneGroup& SFSZone::get_zonegroup() {
  return *zonegroup;
}

const std::string& SFSZone::get_id() {
  return cur_zone_id.id;
}

const std::string& SFSZone::get_name() const {
  return zone_params->get_name();
}

bool SFSZone::is_writeable() {
  return true;
}

bool SFSZone::get_redirect_endpoint(std::string* endpoint) {
  return false;
}

bool SFSZone::has_zonegroup_api(const std::string& api) const {
  return false;
}

const std::string& SFSZone::get_current_period_id() {
  return current_period->get_id();
}

const RGWAccessKey& SFSZone::get_system_key() {
  return zone_params->system_key;
}

const std::string& SFSZone::get_realm_name() {
  return realm->get_name();
}

const std::string& SFSZone::get_realm_id() {
  return realm->get_id();
}

RGWBucketSyncPolicyHandlerRef SFSZone::get_sync_policy_handler() {
  return nullptr;
}

SFSZone::SFSZone(SFStore* _store) : store(_store) {
  realm = new RGWRealm();
  zonegroup = new SFSZoneGroup(store, std::make_unique<RGWZoneGroup>());
  zone_public_config = new RGWZone();
  zone_params = new RGWZoneParams();
  current_period = new RGWPeriod();
  cur_zone_id = rgw_zone_id(zone_params->get_id());
  RGWZonePlacementInfo info;
  RGWZoneStorageClasses sc;
  sc.set_storage_class("STANDARD", nullptr, nullptr);
  info.storage_classes = sc;
  zone_params->placement_pools["default"] = info;
}

}  // namespace rgw::sal
