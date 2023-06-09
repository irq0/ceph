// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#ifndef RGW_S3GW_TELEMETRY_H
#define RGW_S3GW_TELEMETRY_H

#include <boost/beast/http/status.hpp>
#include <chrono>
#include <mutex>
#include <random>
#include <string>

#include "common/Formatter.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "rgw_sal_sfs.h"
#include "rgw_status_page.h"

/// S3GWTelemetry periodically sends s3gw version and system
/// information to a Longhorn update responder. Makes the the replied
/// available versions information available.
class S3GWTelemetry {
 public:
  /// Status stores the timestamps of the last update process
  struct Status {
    ceph::real_time last_attempt;
    ceph::real_time last_success;
  };

  /// Version stores (partial) version information parsed from JSON
  /// replies. See
  /// https://github.com/longhorn/upgrade-responder/blob/master/upgraderesponder/service.go
  /// type Version
  struct Version {
    std::string name;
    ceph::real_time release_date;

    void decode_json(JSONObj* obj);
  };

 private:
  class State {
   private:
    ceph::mutex m_mutex;
    /// parsed version information from the last successful update
    std::vector<Version> m_versions;
    /// update interval from the last successful update
    std::chrono::milliseconds m_update_interval;
    /// Keeps update status information
    Status m_status;

   public:
    State()
        : m_mutex(make_mutex("S3GWTelemetry::State")),
          m_versions(),
          m_update_interval(std::chrono::minutes(10)),
          m_status({{}, {}}) {}
    Status status() const {
      std::lock_guard<std::mutex> lock(mutex);
      return m_status;
    }
    std::vector<Version> versions() const {
      std::lock_guard<std::mutex> lock(mutex);
      return m_versions;
    }
    std::chrono::milliseconds update_interval() const {
      std::lock_guard<std::mutex> lock(mutex);
      return m_update_interval;
    };
    void update_attempt(const ceph::real_time next_attempt) {
      std::lock_guard<std::mutex> lock(mutex);
      m_status.last_attempt = next_attempt;
    }
    void update_success(
        const ceph::real_time next_success,
        const std::vector<Version>& next_versions,
        std::chrono::milliseconds next_update_interval
    ) {
      std::lock_guard<std::mutex> lock(mutex);
      m_status.last_success = next_success;
      m_versions = next_versions;
      m_update_interval = next_update_interval;
    }
  } m_state;

  CephContext* const m_cct;
  const rgw::sal::SFStore* const m_sfs;

  bool m_shutdown;
  std::thread m_updater;
  ceph::condition_variable m_updater_cvar;
  ceph::mutex m_updater_mutex;

  void updater_main();
  void append_sfs_telemetry(JSONFormatter* f) const;

  /// Make POST to upgrade responder URL. Stores the raw response body data in response
  bool post_to_update_responder(const std::string& body, bufferlist& response)
      const;

 protected:
  /// Send request, parse response, update internal state on success
  void do_update();

  /// Wake up service thread
  void wake_up();

 public:
  S3GWTelemetry(CephContext* cct, const rgw::sal::SFStore* sfs);
  virtual ~S3GWTelemetry() = default;

  /// Start service thread. After call will peridodically send and
  /// receive update information.
  void start();

  /// Stop telemetry service thread.
  void stop();

  /// Schedule update immediately. Only call after start(), before stop()
  void update();

  /// Return available versions. Populated after successful update. Otherweise empty.
  std::vector<Version> available_versions() const;

  /// Return status information regarding sending / receiving update.
  Status status() const;

  /// Parse "CheckUpgradeResponse"
  /// (https://github.com/longhorn/upgrade-responder/blob/master/upgraderesponder/service.go)
  bool parse_upgrade_response(
      bufferlist& response,
      std::chrono::milliseconds& out_request_interval_minutes,
      std::vector<Version>& out_versions
  ) const;

  /// Create "CheckUpgradeRequest"
  /// (https://github.com/longhorn/upgrade-responder/blob/master/upgraderesponder/service.go)
  void create_update_responder_request(JSONFormatter* f) const;
};

#endif  // RGW_S3GW_TELEMETRY_H
