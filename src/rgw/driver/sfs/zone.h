// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#ifndef RGW_STORE_SFS_ZONE_H
#define RGW_STORE_SFS_ZONE_H

#include "rgw_sal.h"
#include "rgw_sal_store.h"

namespace rgw::sal {

class SFStore;

class SFSZoneGroup : public StoreZoneGroup {
  SFStore* store;
  std::unique_ptr<RGWZoneGroup> group;
  std::string empty;

 public:
  SFSZoneGroup(SFStore* _store, std::unique_ptr<RGWZoneGroup> _group)
      : store(_store), group(std::move(_group)) {}
  virtual ~SFSZoneGroup() = default;

  virtual const std::string& get_id() const override { return group->get_id(); }
  virtual const std::string& get_name() const override {
    return group->get_name();
  }
  virtual int equals(const std::string& other_zonegroup) const override {
    return group->equals(other_zonegroup);
  }
  virtual const std::string& get_endpoint() const override { return empty; }
  virtual bool placement_target_exists(std::string& target) const override {
    return !!group->placement_targets.count(target);
  }
  virtual bool is_master_zonegroup() const override {
    return group->is_master_zonegroup();
  }
  virtual const std::string& get_api_name() const override {
    return group->api_name;
  }
  virtual int get_placement_target_names(std::set<std::string>& names
  ) const override {
    for (const auto& target : group->placement_targets) {
      names.emplace(target.second.name);
    }
    return 0;
  }
  virtual const std::string& get_default_placement_name() const override {
    return group->default_placement.name;
  }
  virtual int get_hostnames(std::list<std::string>& names) const override {
    names = group->hostnames;
    return 0;
  }
  virtual int get_s3website_hostnames(std::list<std::string>& names
  ) const override {
    names = group->hostnames_s3website;
    return 0;
  }
  virtual int get_zone_count() const override { return 1; }
  virtual int get_placement_tier(
      const rgw_placement_rule& rule, std::unique_ptr<PlacementTier>* tier
  ) override {
    return -1;
  }

  virtual int get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone)
      override {
    return -1;
  }
  virtual int get_zone_by_name(
      const std::string& name, std::unique_ptr<Zone>* zone
  ) override {
    return -1;
  }
  virtual int list_zones(std::list<std::string>& zone_ids) override {
    zone_ids.clear();
    return 0;
  }
  virtual std::unique_ptr<ZoneGroup> clone() override {
    std::unique_ptr<RGWZoneGroup> zg =
        std::make_unique<RGWZoneGroup>(*group.get());
    return std::make_unique<SFSZoneGroup>(store, std::move(zg));
  }
};

class SFSZone : public StoreZone {
 protected:
  SFStore* store;
  RGWRealm* realm{nullptr};
  SFSZoneGroup* zonegroup{nullptr};
  RGWZone* zone_public_config{nullptr};
  RGWZoneParams* zone_params{nullptr};
  RGWPeriod* current_period{nullptr};
  rgw_zone_id cur_zone_id;

 public:
  SFSZone(const SFSZone&) = delete;
  SFSZone& operator=(const SFSZone&) = delete;
  SFSZone(SFStore* _store);
  ~SFSZone() {
    delete realm;
    delete zonegroup;
    delete zone_public_config;
    delete zone_params;
    delete current_period;
  }

  virtual std::unique_ptr<Zone> clone() override {
    return std::make_unique<SFSZone>(store);
  }
  virtual ZoneGroup& get_zonegroup() override;
  virtual const std::string& get_id() override;
  virtual const std::string& get_name() const override;
  virtual bool is_writeable() override;
  virtual bool get_redirect_endpoint(std::string* endpoint) override;
  virtual bool has_zonegroup_api(const std::string& api) const override;
  virtual const std::string& get_current_period_id() override;
  virtual const RGWAccessKey& get_system_key() override;
  virtual const std::string& get_realm_name() override;
  virtual const std::string& get_realm_id() override;
  virtual const std::string_view get_tier_type() override { return "rgw"; }
  virtual RGWBucketSyncPolicyHandlerRef get_sync_policy_handler() override;

  const RGWZoneParams& get_params() { return *zone_params; }
};

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_ZONE_H
