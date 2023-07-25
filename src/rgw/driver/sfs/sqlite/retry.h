// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
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
 */
#pragma once

#include <functional>
#include <optional>
#include <system_error>
#include <thread>

#include "errors.h"
#include "rgw_perf_counters.h"

using namespace std::chrono_literals;
namespace rgw::sal::sfs::sqlite {

/// RetrySQLite is a utility to retry non-critically failed
// sqlite_orm. run() executes function passed with the constructor
// returning the result OR an empty optional if non could be obtained
// after retrying. Catches all non-critical exceptions and makes them
// available via failed_error(). Critical exceptions are passed on.
template <typename Return>
class RetrySQLite {
 public:
  using Func = std::function<Return(void)>;

 private:
  const Func m_fn;
  const int m_max_retries{10};
  bool m_successful{false};
  int m_retries{0};
  std::error_code m_failed_error{};

 public:
  RetrySQLite(Func&& fn) : m_fn(std::forward<Func>(fn)) {}
  RetrySQLite(RetrySQLite&&) = delete;
  RetrySQLite(const RetrySQLite&) = delete;
  RetrySQLite& operator=(const RetrySQLite&) = delete;
  RetrySQLite& operator=(RetrySQLite&&) = delete;

  /// run runs fn with up to m_max_retries retries. It may throw
  /// critical-exceptions. Non-critical errors are made available via
  /// failed_error(). Returns empty if fn did not succeed after
  /// retrying.
  std::optional<Return> run() {
    if (perfcounter) {
      perfcounter->inc(l_rgw_sfs_sqlite_retry_total, 1);
    }
    for (int retry = 0; retry < m_max_retries; retry++) {
      try {
        Return result = m_fn();
        m_successful = true;
        m_failed_error = std::error_code{};
        m_retries = retry;
        return result;
      } catch (const std::system_error& ex) {
        m_failed_error = ex.code();
        if (critical_error(ex.code().value())) {
          // Rethrow, expect a higher layer to shut us down
          throw ex;
        }
        std::this_thread::sleep_for(10ms * retry);
        m_retries = retry;
        if (perfcounter) {
          perfcounter->inc(l_rgw_sfs_sqlite_retry_retried_count, 1);
        }
      }
    }
    m_successful = false;
    if (perfcounter) {
      perfcounter->inc(l_rgw_sfs_sqlite_retry_failed_count, 1);
    }
    return std::nullopt;
  };

  /// successful returns true if fn finished successful, possilby
  /// after retries
  bool successful() { return m_successful; };
  /// failed_error returns the non-critical error code of the last
  /// failed attempt to run fn
  std::error_code failed_error() { return m_failed_error; };
  /// retries returns the number of retries to failure or success
  int retries() { return m_retries; };
};

}  // namespace rgw::sal::sfs::sqlite
