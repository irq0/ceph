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

#include <common/ceph_mutex.h>
#include <common/ceph_time.h>
#include <common/dout.h>
#include <common/dout_fmt.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <cmath>
#include <mutex>

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

  void handle_conf_change(
      const ConfigProxy& conf, const std::set<std::string>& changed) override {
    if (changed.count("rgw_max_concurrent_requests")) {
      auto new_max = conf.get_val<int64_t>("rgw_max_concurrent_requests");
      max_requests =
          new_max > 0 ? new_max : std::numeric_limits<int64_t>::max();
    }
  }
};

class Gradient2 : public ConcurrencyLimiter, public md_config_obs_t {
 protected:
  enum class Metric {
    metrics_start = 94000,
    long_rtt,
    last_rtt,
    gradient,
    limit,
    effective_limit,
    min,
    max,
    samples_total,
    app_limited_total,
    drops_total,
    metrics_stop,
  };

  class ExpAvg {
 private:
    double _value = 0.0;
    double _sum = 0;
    const int _window;
    const int _warmup_window;
    int _count = 0;

   public:
    ExpAvg(int window, int warmup_window)
        : _window(window), _warmup_window(warmup_window) {}
    double add(double sample) {
      if (_count < _warmup_window) {
        _count++;
        _sum += sample;
        _value = _sum / _count;  // XXX handle sum overflow and things
      } else {
        const double factor = 2.00 / (_window + 1);
        _value = _value * (1 - factor) + sample * factor;
      }
      return _value;
    }
    double get() const { return _value; }
  };

 private:
  CephContext* _cct;
  PerfCountersRef _perf;
  const DoutPrefix _dp;
  mutable ceph::mutex _mutex = ceph::make_mutex("Gradient2::lock");

  // config
  std::atomic<int64_t> _min_requests;
  std::atomic<int64_t> _max_requests;
  std::atomic<double> _smoothing;
  std::atomic<double> _tolerance;
  std::atomic<int64_t> _estimate_growth;

  // updated on sample()
  std::atomic<int64_t> _estimated_requests;
  ExpAvg _long_rtt_sec;

 public:
  explicit Gradient2(CephContext* cct)
      : _cct(cct),
        _perf(initialize_perf_counters(cct, "rgw-limiter")),
        _dp(cct, ceph_subsys_rgw, "limiter/gradient2: "),
        _min_requests(
            cct->_conf.get_val<int64_t>("rgw_min_concurrent_requests")),
        _max_requests(
            cct->_conf.get_val<int64_t>("rgw_max_concurrent_requests")),
        _smoothing(cct->_conf.get_val<double>("rgw_gradient2_smoothing")),
        _tolerance(cct->_conf.get_val<double>("rgw_gradient2_rtt_tolerance")),
        _estimate_growth(
            cct->_conf.get_val<int64_t>("rgw_gradient2_estimate_growth")),
        _estimated_requests(cct->_conf.get_val<int64_t>(
            "rgw_gradient2_initial_concurrent_requests")),
        _long_rtt_sec(ExpAvg(
            cct->_conf.get_val<int64_t>("rgw_gradient2_long_window"), 10)) {
    if (_max_requests <= 0) {
      _max_requests = std::numeric_limits<int64_t>::max();
    }
    if (_min_requests <= 0) {
      _min_requests = _max_requests / 2;
    }
    _estimated_requests = std::clamp(
        _estimated_requests.load(), _min_requests.load(), _max_requests.load());

    cct->_conf.add_observer(this);
    _perf->set(static_cast<int>(Metric::min), _min_requests);
    _perf->set(static_cast<int>(Metric::max), _max_requests);
    _perf->set(static_cast<int>(Metric::effective_limit), _estimated_requests);
  }
  ~Gradient2() override {}

  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {
        std::string{"rgw_max_concurrent_requests"},
        std::string{"rgw_min_concurrent_requests"},
        std::string{"rgw_gradient2_smoothing"},
        std::string{"rgw_gradient2_rtt_tolerance"},
        std::string{"rgw_gradient2_estimate_growth"},
    };
  }

  // TODO don't update max requests, update a max we track
  void handle_conf_change(
      const ConfigProxy& conf, const std::set<std::string>& changed) override {
    if (changed.count("rgw_max_concurrent_requests")) {
      auto new_max = conf.get_val<int64_t>("rgw_max_concurrent_requests");
      _max_requests =
          new_max > 0 ? new_max : std::numeric_limits<int64_t>::max();
      _perf->set(static_cast<int>(Metric::max), _max_requests);
    }
    if (changed.count("rgw_min_concurrent_requests")) {
      auto new_min = conf.get_val<int64_t>("rgw_min_concurrent_requests");
      _min_requests =
          new_min > 0 ? new_min : _max_requests / 2;
    }
    if (changed.count("rgw_gradient2_smoothing")) {
      _smoothing = conf.get_val<double>("rgw_gradient2_smoothing");
    }
    if (changed.count("rgw_gradient2_rtt_tolerance")) {
      _tolerance = conf.get_val<double>("rgw_gradient2_rtt_tolerance");
    }
    if (changed.count("rgw_gradient2_estimate_growth")) {
      _estimate_growth = conf.get_val<int64_t>("rgw_gradient2_estimate_growth");
    }
  }

  int64_t limit() const override { return _estimated_requests.load(std::memory_order_relaxed); }

  void sample(const Sample& sample) override {
    std::lock_guard<ceph::mutex> lock(_mutex);
    _perf->inc(static_cast<int>(Metric::samples_total));
    if (sample.dropped) {  // use this signal somehow?
      _perf->inc(static_cast<int>(Metric::drops_total));
    }

    const double estimated_limit = _estimated_requests;
    _perf->tset(static_cast<int>(Metric::last_rtt), sample.rtt);

    const auto short_rtt_sec =
        std::chrono::duration<double>(sample.rtt).count();
    const auto long_rtt_sec_now = _long_rtt_sec.add(short_rtt_sec);
    _perf->tset(
        static_cast<int>(Metric::long_rtt), make_timespan(long_rtt_sec_now));

    // If the long RTT is substantially larger than the short RTT then
    // reduce the long RTT measurement. This can happen when latency
    // returns to normal after a prolonged prior of excessive load.
    // Reducing the long RTT without waiting for the exponential
    // smoothing helps bring the system back to steady state.
    if (short_rtt_sec > 0 && long_rtt_sec_now / short_rtt_sec > 2) {
      _long_rtt_sec.add(long_rtt_sec_now * 0.95);
    }
    // Don't grow the limit if we are app limited
    if (sample.inflight < estimated_limit / 2) {
      _perf->inc(static_cast<int>(Metric::app_limited_total));
      return;
    }

    // Rtt could be higher than rtt_noload because of smoothing rtt
    // noload updates so set to 1.0 to indicate no queuing. Otherwise
    // calculate the slope and don't allow it to be reduced by more
    // than half to avoid aggressive load-shedding due to outliers.

    double gradient = 1.0;
    if (sample.dropped) {  // dropped request -> some kind of overload -> slow down
      gradient = 0.5;  // validate find config?
    } else if (short_rtt_sec > 0 && long_rtt_sec_now > 0) {
      const auto g = _tolerance * long_rtt_sec_now / short_rtt_sec;
      gradient = std::clamp(g, 0.5, 1.0);
    }
    double new_limit = estimated_limit * gradient + _estimate_growth;
    new_limit = estimated_limit * (1 - _smoothing) + new_limit * _smoothing;

    if (estimated_limit != new_limit) {
      ldpp_dout_fmt(&_dp, -1,
          "new limit: {} -> {} short_rtt:{}s long_rtt:{}s gradient:{} "
          "growth:{} min:{} max:{}",
          _estimated_requests.load(), new_limit, short_rtt_sec,
          long_rtt_sec_now, gradient, _estimate_growth.load(),
          _min_requests.load(), _max_requests.load());
    }
    const auto clamped = std::max(_min_requests.load(),
        std::min(_max_requests.load(), static_cast<int64_t>(new_limit)));
    _estimated_requests.store(clamped, std::memory_order_relaxed);
    _perf->set(static_cast<int>(Metric::gradient), llround(gradient * 1000));
    _perf->set(static_cast<int>(Metric::limit), new_limit);
    _perf->set(static_cast<int>(Metric::effective_limit), _estimated_requests);
  }

  static PerfCountersRef initialize_perf_counters(
      CephContext* cct, const std::string& name) {
    PerfCountersBuilder pcb(cct, name, static_cast<int>(Metric::metrics_start),
        static_cast<int>(Metric::metrics_stop));
    pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
    pcb.add_time(static_cast<int>(Metric::last_rtt), "last_rtt", "");
    pcb.add_time(static_cast<int>(Metric::long_rtt), "long_rtt", "");
    pcb.add_u64(static_cast<int>(Metric::gradient), "gradient", "");
    pcb.add_u64(static_cast<int>(Metric::limit), "limit", "");
    pcb.add_u64(
        static_cast<int>(Metric::effective_limit), "effective_limit", "");
    pcb.add_u64(static_cast<int>(Metric::min), "min", "");
    pcb.add_u64(static_cast<int>(Metric::max), "max", "");
    pcb.add_u64_counter(
        static_cast<int>(Metric::samples_total), "samples_total", "");
    pcb.add_u64_counter(
        static_cast<int>(Metric::app_limited_total), "app_limited_total", "");
    pcb.add_u64_counter(
        static_cast<int>(Metric::drops_total), "drops_total", "");

    auto logger = PerfCountersRef{pcb.create_perf_counters(), cct};
    cct->get_perfcounters_collection()->add(logger.get());
    return logger;
  }
};

static std::unique_ptr<ConcurrencyLimiter> create_by_name(
    CephContext* cct, std::string_view name) {
  if (name == "static") {
    return std::make_unique<StaticLimiter>(cct);
  } else if (name == "gradient2") {
    return std::make_unique<Gradient2>(cct);
  }
  return nullptr;
}

}  // namespace rgw::limiter
