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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "common/TrackedOp.h"
#include "common/admin_socket.h"
#include "common/async/request_context.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "common/config_obs.h"

namespace boost::asio { class io_context; }

/// Inspection of in-flight RGW requests.
///
/// Two views over one source of truth.  Every place a request can wait pushes
/// a scoped record onto that request's wait stack, and the views are
/// different aggregations of those records:
///
///   in_flight   per request, the phase it reached and what it waits on now
///   threads     per frontend thread, what it is serving and whether it is
///               parked in a synchronous call
///
/// Both read the in-flight registry directly, so the two cannot disagree.
///
/// `waiting_on` names what a request is blocked on, but only the two places
/// that report waits -- RADOS and admission -- can fill it in.  The phase is
/// the complement: a request whose phase advanced and stopped with nothing in
/// `waiting_on` is inside an uninstrumented blocking call, and the phase says
/// which one.  That is why phases are marked only at steps that can block; see
/// mark_phase() in rgw_process.cc.
///
/// Alongside them a watchdog measures how long a task posted to the
/// frontend's io_context takes to run.  That time goes unbounded exactly when
/// every frontend thread is stuck in a synchronous call, which makes it a
/// direct test for thread pool exhaustion -- a condition no per-request view
/// can see, because the requests that would show it have not started yet.  It
/// is reported as the `frontend_executor_lat` perf counter so that it can be
/// alerted on with nobody logged in.  The watchdog is a separate thread
/// rather than a timer on that io_context precisely because the condition it
/// looks for is that io_context not running anything.
///
/// Request tracking is off by default and can be switched on at runtime with
/// `rgw_op_tracker`: a diagnostic that needs a restart is unavailable during
/// the incident it exists for.  The watchdog runs either way, since the
/// executor latency counter costs one posted task per second.
namespace rgw::optracker {

using ceph::async::wait_kind;

/// how often the watchdog probes the frontend executor
inline constexpr auto watchdog_tick = std::chrono::seconds{1};
/// a probe outstanding this long means no frontend thread can run anything
inline constexpr auto stall_threshold = std::chrono::seconds{5};

/// One open wait.
struct WaitRecord {
  uint64_t handle = 0;
  wait_kind kind = wait_kind::other;
  std::string resource;
  ceph::coarse_mono_time start;
  bool blocking = false;
  uint64_t thread = 0;

  double age(ceph::coarse_mono_time now) const {
    return ceph::to_seconds<double>(now - start);
  }
};

/// An in-flight request.
///
/// Outlives the req_state it describes: on completion TrackedOp moves it to
/// the tracker's history, so every field here is a copy rather than a
/// reference into request state.
class TrackedRequest final : public TrackedOp, public ceph::async::wait_sink {
 public:
  TrackedRequest(OpTracker* tracker, const utime_t& initiated, uint64_t id,
                 std::string trans_id);

  /// What the thread running this request is doing, for the thread view.
  struct ThreadUse {
    uint64_t thread = 0;
    std::string trans_id;
    std::string op;
    bool blocked = false;
    std::string blocked_on;
    double blocked_for = 0.0;
  };
  ThreadUse thread_use(ceph::coarse_mono_time now) const;

  /// identity, filled in as the request is parsed.  grouped so that each
  /// point where more becomes known costs one lock rather than one per field.
  /// safe to call from the request's own thread while dumps run concurrently
  void set_op(std::string_view method, std::string_view op_name,
              std::string_view remote);
  void set_target(std::string_view bucket, std::string_view object,
                  std::string_view user);
  void set_result(int http_status, int op_ret);

  // wait_sink
  uint64_t wait_begin(wait_kind kind, std::string_view resource,
                      bool blocking) override;
  void wait_end(uint64_t handle) override;

  /// snapshot of the waits still open, oldest first
  std::vector<WaitRecord> current_waits() const;

 protected:
  void _dump(ceph::Formatter* f) const override;
  void _dump_op_descriptor(std::ostream& os) const override;
  bool filter_out(const std::set<std::string>& filters) override;

 private:
  /// guards the fields below.  get_desc() takes TrackedOp::lock and then this
  /// one by way of _dump_op_descriptor(), so anything needing both must take
  /// them in that order
  mutable ceph::mutex info_lock =
      ceph::make_mutex("rgw::optracker::TrackedRequest::info_lock");

  const uint64_t id;
  const uint64_t thread;   ///< thread the request started on
  std::string trans_id;
  std::string method;
  std::string op_name;
  std::string bucket;
  std::string object;
  std::string user;
  std::string remote;
  int http_status = 0;
  int op_ret = 0;

  uint64_t next_handle = 0;
  std::vector<WaitRecord> waits;   ///< open waits, in the order they began
  uint64_t total_waits = 0;
};

using TrackedRequestRef = boost::intrusive_ptr<TrackedRequest>;

/// Owns the registry and the watchdog, and serves the admin socket commands.
class Tracker : public AdminSocketHook, public md_config_obs_t {
 public:
  explicit Tracker(CephContext* cct);
  ~Tracker() override;

  bool enabled() const { return op_tracker.is_tracking(); }

  OpTracker& ops() { return op_tracker; }

  /// begin probing `frontend` for stalls.  called whether or not request
  /// tracking is enabled, since it can be enabled later and the executor
  /// latency counter is worth having either way.  `tick` overrides the probe
  /// interval for tests
  void start(boost::asio::io_context& frontend, size_t pool_size,
             ceph::timespan tick = watchdog_tick);
  void stop();

  int hook_to_admin_socket();

  int call(std::string_view command, const cmdmap_t& cmdmap,
           const bufferlist& inbl, ceph::Formatter* f, std::ostream& errss,
           bufferlist& out) override;

  // md_config_obs_t
  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override;

  void dump_in_flight(ceph::Formatter* f, bool only_blocked,
                      const std::set<std::string>& filters);
  void dump_threads(ceph::Formatter* f);

 private:
  void watchdog_entry(ceph::timespan tick);
  void probe_frontend();

  CephContext* cct;
  OpTracker op_tracker;

  boost::asio::io_context* frontend = nullptr;
  size_t pool_size = 0;

  /// outstanding executor probe.  `sent` is written only by the watchdog and
  /// only while no probe is outstanding, and the seq_cst store of
  /// `outstanding` orders it against the posted task's read
  struct Probe {
    std::atomic<bool> outstanding{false};
    ceph::coarse_mono_time sent;
    std::atomic<double> last_latency{0.0};
  };
  std::shared_ptr<Probe> probe;
  /// throttles the stall warning.  watchdog thread only
  ceph::coarse_mono_time last_warned;

  std::thread watchdog;
  std::atomic<bool> stopping{false};
  // std::condition_variable only waits on a std::mutex, and ceph::mutex is a
  // distinct debug type in some builds
  std::mutex watchdog_mutex;
  std::condition_variable watchdog_cond;

  bool commands_registered = false;
  bool observing = false;
};

/// RAII: creates the tracked request and retires it into history when the
/// request finishes.
///
/// Constructing one when tracking is disabled is cheap and yields a null
/// wait sink, so callers need no conditionals.
class RequestScope {
 public:
  RequestScope(Tracker* tracker, uint64_t id, std::string trans_id);
  ~RequestScope();

  RequestScope(const RequestScope&) = delete;
  RequestScope& operator=(const RequestScope&) = delete;

  TrackedRequest* get() const { return req.get(); }
  explicit operator bool() const { return req != nullptr; }

  /// the wait_sink to put in the request's request_context, or null when
  /// tracking is disabled
  ceph::async::wait_sink* sink() const { return req.get(); }

 private:
  TrackedRequestRef req;
};

/// Note that a request reached a named phase.  Null-tolerant so that callers
/// need no conditionals when tracking is disabled.
inline void mark_event(TrackedRequest* req, std::string_view event)
{
  if (req != nullptr) {
    req->mark_event(event);
  }
}

} // namespace rgw::optracker
