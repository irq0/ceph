// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <common/dout.h>
#include <common/dout_fmt.h>

#include <atomic>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/io_context.hpp>

#include "common/async/completion.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"

namespace rgw::limiter {
namespace async = ceph::async;

class StaticLimiter;

class ConcurrencyLimiter {
 public:
  struct Sample {
    std::chrono::nanoseconds rtt{0};
    int64_t inflight = 0;
    bool dropped = false;
  };

  virtual ~ConcurrencyLimiter() = default;

  // current ceiling of cuncurrent in-flight requests
  virtual int64_t limit() const = 0;

  // per completion feedback: adaptive filters use this to update
  // their internal estimates
  virtual void sample(const Sample&) = 0;
};

class StaticLimiter : public ConcurrencyLimiter, public md_config_obs_t {
 private:
  CephContext* cct;
  std::atomic<int64_t> max_requests;

 public:
  explicit StaticLimiter(CephContext* cct)
      : max_requests(
            cct->_conf.get_val<int64_t>("rgw_max_concurrent_requests")) {
    if (max_requests <= 0) {
      max_requests = std::numeric_limits<int64_t>::max();
    }
    cct->_conf.add_observer(this);
  }
  ~StaticLimiter() override {}

  int64_t limit() const override { return max_requests.load(); }

  void sample(const Sample&) {};

  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {std::string{"rgw_max_concurrent_requests"}};
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override
  {
    if (changed.count("rgw_max_concurrent_requests")) {
      auto new_max = conf.get_val<int64_t>("rgw_max_concurrent_requests");
      max_requests = new_max > 0 ? new_max : std::numeric_limits<int64_t>::max();
    }
  }
};

static std::unique_ptr<ConcurrencyLimiter> create_by_name(
    CephContext* cct, std::string_view name) {
  if (name == "static") {
    return std::make_unique<StaticLimiter>(cct);
  }
  return nullptr;
}

enum class Metric {
  metrics_start = 94000,
  qlen,
  throttle,
  outstanding,
  long_rtt,
  short_rtt,
  queue_size,
  metrics_stop,
};

template <typename Num>
class ExpAvg {
 private:
  double value = 0.0;
  double sum = 0.0;
  const int window;
  const int warmup_window;
  int count = 0;

 public:
  ExpAvg(int _window, int _warmup_window) :
      window(_window),
      warmup_window(_warmup_window) {
  }

  Num add(Num sample) {
    if (count < warmup_window) {
      count++;
      sum += sample;
      value = sum / count;
    } else {
      const double factor = 2.00 / (window + 1);
      value = value * (1-factor) + sample * factor;
    }
    return value;
  }

  Num get() const {
    return value;
  }
};

}
