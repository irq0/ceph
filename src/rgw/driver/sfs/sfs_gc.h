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

#include "rgw/driver/sfs/types.h"
#include "rgw_sal.h"
#include "rgw_sal_sfs.h"

namespace rgw::sal::sfs {

class SFSGC : public DoutPrefixProvider {
  CephContext* cct = nullptr;
  SFStore* store = nullptr;
  std::atomic<bool> down_flag = {false};
  long int max_objects;

  class GCWorker : public Thread {
    const DoutPrefixProvider* dpp = nullptr;
    CephContext* cct = nullptr;
    SFSGC* gc = nullptr;
    ceph::mutex lock = ceph::make_mutex("GCWorker");
    ceph::condition_variable cond;

    std::string get_cls_name() const { return "GCWorker"; }

   public:
    GCWorker(const DoutPrefixProvider* _dpp, CephContext* _cct, SFSGC* _gc);
    void* entry() override;
    void stop();
  };

  GCWorker* worker = nullptr;

 public:
  SFSGC() = default;
  ~SFSGC();

  void initialize(CephContext* _cct, SFStore* _store);
  void finalize();

  int process();

  bool going_down();
  void start_processor();
  void stop_processor();

  CephContext* get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const;

  std::ostream& gen_prefix(std::ostream& out) const;

  std::string get_cls_name() const { return "SFSGC"; }

 private:
  void process_deleted_buckets();

  void delete_objects(const std::string& bucket_id);
  void delete_versioned_objects(const std::shared_ptr<Object>& object);

  void delete_bucket(const std::string& bucket_id);
  void delete_object(const std::shared_ptr<Object>& object);
  void delete_versioned_object(const std::shared_ptr<Object>& object, uint id);
};

}  //  namespace rgw::sal::sfs
