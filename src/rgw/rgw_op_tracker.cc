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

#include "rgw_op_tracker.h"

#include <algorithm>
#include <set>
#include <utility>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include "common/Formatter.h"
#include "common/Thread.h"
#include "common/ceph_context.h"
#include "common/cmdparse.h"
#include "common/config.h"
#include "common/config_proxy.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/compat.h"
#include "rgw_perf_counters.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context cct

namespace ceph::async {

const char* to_string(wait_kind kind)
{
  switch (kind) {
    case wait_kind::rados: return "rados";
    case wait_kind::admission: return "admission";
    case wait_kind::other: return "other";
  }
  return "unknown";
}

} // namespace ceph::async

namespace rgw::optracker {

namespace {

/// The registry is sharded only to keep request start and finish off a single
/// lock; nothing reads a shard on its own, so there is nothing to tune here.
constexpr uint32_t num_shards = 32;

/// check_ops_in_flight()'s cap on how many slow ops it names per warning.  we
/// do not call it, but OpTracker requires a value
constexpr int log_threshold = 5;

/// at most one stall warning per this interval, so that a sustained stall
/// does not fill the log while the daemon is already in trouble
constexpr auto warn_interval = std::chrono::seconds{60};

uint64_t this_thread_id()
{
  return static_cast<uint64_t>(ceph_gettid());
}

double seconds_since(ceph::coarse_mono_time then, ceph::coarse_mono_time now)
{
  return ceph::to_seconds<double>(now - then);
}

} // namespace

// -- TrackedRequest --

TrackedRequest::TrackedRequest(OpTracker* tracker, const utime_t& initiated,
                               uint64_t id, std::string trans_id)
  : TrackedOp(tracker, initiated), id(id), thread(this_thread_id()),
    trans_id(std::move(trans_id))
{
}

void TrackedRequest::set_op(std::string_view method, std::string_view op_name,
                            std::string_view remote)
{
  {
    std::lock_guard l{info_lock};
    this->method = method;
    this->op_name = op_name;
    this->remote = remote;
  }
  reset_desc();
}

void TrackedRequest::set_target(std::string_view bucket,
                                std::string_view object,
                                std::string_view user)
{
  {
    std::lock_guard l{info_lock};
    this->bucket = bucket;
    this->object = object;
    this->user = user;
  }
  reset_desc();
}

void TrackedRequest::set_result(int http_status, int op_ret)
{
  std::lock_guard l{info_lock};
  this->http_status = http_status;
  this->op_ret = op_ret;
}

uint64_t TrackedRequest::wait_begin(wait_kind kind, std::string_view resource,
                                    bool blocking)
{
  std::lock_guard l{info_lock};
  const uint64_t handle = ++next_handle;
  waits.push_back(WaitRecord{handle, kind, std::string{resource},
                             ceph::coarse_mono_clock::now(), blocking,
                             this_thread_id()});
  ++total_waits;
  return handle;
}

void TrackedRequest::wait_end(uint64_t handle)
{
  std::lock_guard l{info_lock};
  auto i = std::find_if(waits.begin(), waits.end(),
                        [handle](const WaitRecord& w) {
                          return w.handle == handle;
                        });
  if (i != waits.end()) {
    waits.erase(i);
  }
}

std::vector<WaitRecord> TrackedRequest::current_waits() const
{
  std::lock_guard l{info_lock};
  return waits;
}

TrackedRequest::ThreadUse TrackedRequest::thread_use(
    ceph::coarse_mono_time now) const
{
  std::lock_guard l{info_lock};
  ThreadUse use;
  use.thread = thread;
  use.trans_id = trans_id;
  use.op = op_name;
  // a wait that was entered without a yield context occupies the thread that
  // entered it until it returns. that thread, not the one the request started
  // on, is the one an exhausted pool is short of
  for (const auto& w : waits) {
    if (w.blocking) {
      use.thread = w.thread;
      use.blocked = true;
      use.blocked_on = w.resource;
      use.blocked_for = w.age(now);
      break;
    }
  }
  return use;
}

void TrackedRequest::_dump_op_descriptor(std::ostream& os) const
{
  std::lock_guard l{info_lock};
  os << "rgw_request(" << trans_id;
  if (!method.empty()) {
    os << ' ' << method;
  }
  if (!op_name.empty()) {
    os << ' ' << op_name;
  }
  if (!bucket.empty()) {
    os << " bucket=" << bucket;
  }
  if (!object.empty()) {
    os << " obj=" << object;
  }
  os << ')';
}

void TrackedRequest::_dump(ceph::Formatter* f) const
{
  const auto now = ceph::coarse_mono_clock::now();

  // TrackedOp::dump() emits no events of its own, so the phase timeline --
  // the first thing you want from a stuck request -- has to come from here.
  // TrackedOp::lock before info_lock is the order get_desc() establishes.
  std::lock_guard el{lock};
  std::lock_guard il{info_lock};

  f->dump_string("phase", events.empty() ? std::string{}
                                         : std::string{events.back().str});
  f->open_array_section("phases");
  for (const auto& e : events) {
    f->open_object_section("phase");
    f->dump_stream("time") << e.stamp;
    f->dump_string("event", e.str);
    f->close_section();
  }
  f->close_section();

  f->dump_unsigned("req_id", id);
  f->dump_string("trans_id", trans_id);
  f->dump_string("method", method);
  f->dump_string("op", op_name);
  f->dump_string("bucket", bucket);
  f->dump_string("object", object);
  f->dump_string("user", user);
  f->dump_string("remote", remote);
  f->dump_int("http_status", http_status);
  f->dump_int("op_ret", op_ret);
  f->dump_unsigned("thread", thread);
  f->dump_unsigned("waits_total", total_waits);

  f->open_array_section("waiting_on");
  for (const auto& w : waits) {
    f->open_object_section("wait");
    f->dump_string("kind", ceph::async::to_string(w.kind));
    f->dump_string("resource", w.resource);
    f->dump_float("age", w.age(now));
    f->dump_bool("blocking", w.blocking);
    f->dump_unsigned("thread", w.thread);
    f->close_section();
  }
  f->close_section();
}

bool TrackedRequest::filter_out(const std::set<std::string>& filters)
{
  if (filters.empty() || filters.count("") > 0) {
    return true;
  }
  std::lock_guard l{info_lock};
  for (const auto& filter : filters) {
    if (filter == op_name || filter == bucket || filter == user ||
        filter == trans_id || filter == method) {
      return true;
    }
  }
  return false;
}

// -- Tracker --

Tracker::Tracker(CephContext* cct)
  : cct(cct),
    op_tracker(cct, cct->_conf.get_val<bool>("rgw_op_tracker"), num_shards)
{
  const auto complaint =
      cct->_conf.get_val<std::chrono::seconds>("rgw_op_tracker_complaint_time");
  op_tracker.set_complaint_and_threshold(static_cast<float>(complaint.count()),
                                         log_threshold);
  op_tracker.set_history_size_and_duration(
      cct->_conf.get_val<uint64_t>("rgw_op_tracker_history_size"),
      cct->_conf.get_val<uint64_t>("rgw_op_tracker_history_duration"));
  op_tracker.set_history_slow_op_size_and_threshold(
      cct->_conf.get_val<uint64_t>("rgw_op_tracker_history_slow_op_size"),
      static_cast<float>(cct->_conf.get_val<std::chrono::seconds>(
          "rgw_op_tracker_history_slow_op_threshold").count()));

  cct->_conf.add_observer(this);
  observing = true;
}

Tracker::~Tracker()
{
  if (observing) {
    cct->_conf.remove_observer(this);
  }
  stop();
  if (commands_registered) {
    cct->get_admin_socket()->unregister_commands(this);
  }
  op_tracker.on_shutdown();
}

std::vector<std::string> Tracker::get_tracked_keys() const noexcept
{
  return {"rgw_op_tracker"};
}

void Tracker::handle_conf_change(const ConfigProxy& conf,
                                 const std::set<std::string>& changed)
{
  if (changed.count("rgw_op_tracker") == 0) {
    return;
  }
  const bool on = conf.get_val<bool>("rgw_op_tracker");
  if (on == op_tracker.is_tracking()) {
    return;
  }
  // nothing here can block: the watchdog reads no config, so it cannot be
  // waiting on the lock this runs under
  op_tracker.set_tracking(on);
  ldout(cct, 1) << "rgw op tracker " << (on ? "enabled" : "disabled") << dendl;
}

int Tracker::hook_to_admin_socket()
{
  static const std::pair<const char*, const char*> commands[] = {
    {"rgw ops in_flight "
     "name=filterstr,type=CephString,n=N,req=false",
     "show in-flight rgw requests and what each is waiting on"},
    {"rgw ops blocked "
     "name=filterstr,type=CephString,n=N,req=false",
     "show in-flight rgw requests older than the complaint time"},
    {"rgw ops historic",
     "show recently completed rgw requests"},
    {"rgw ops historic_slow",
     "show recently completed slow rgw requests"},
    {"rgw threads",
     "show what each rgw frontend thread is doing"},
  };

  auto* admin_socket = cct->get_admin_socket();
  for (const auto& [cmd, desc] : commands) {
    int r = admin_socket->register_command(cmd, this, desc);
    if (r < 0) {
      lderr(cct) << "rgw op tracker: failed to register '" << cmd
                 << "': " << cpp_strerror(r) << dendl;
      return r;
    }
    commands_registered = true;
  }
  return 0;
}

int Tracker::call(std::string_view command, const cmdmap_t& cmdmap,
                  const bufferlist&, ceph::Formatter* f, std::ostream& errss,
                  bufferlist&)
{
  // the commands are registered whether or not tracking is on, so that this
  // says what to do about it rather than the admin socket saying the command
  // does not exist
  if (!enabled()) {
    errss << "rgw op tracker is disabled; "
             "`ceph config set client.rgw rgw_op_tracker true` enables it "
             "without a restart";
    return -ENOTSUP;
  }

  std::set<std::string> filters{""};
  if (std::vector<std::string> args;
      ceph::common::cmd_getval(cmdmap, "filterstr", args) && !args.empty()) {
    filters.clear();
    filters.insert(args.begin(), args.end());
  }

  if (command == "rgw ops in_flight") {
    dump_in_flight(f, false, filters);
  } else if (command == "rgw ops blocked") {
    dump_in_flight(f, true, filters);
  } else if (command == "rgw ops historic") {
    op_tracker.dump_historic_ops(f, false, filters);
  } else if (command == "rgw ops historic_slow") {
    op_tracker.dump_historic_slow_ops(f, filters);
  } else if (command == "rgw threads") {
    dump_threads(f);
  } else {
    errss << "unknown command " << command;
    return -EINVAL;
  }
  return 0;
}

void Tracker::dump_in_flight(ceph::Formatter* f, bool only_blocked,
                             const std::set<std::string>& filters)
{
  op_tracker.dump_ops_in_flight(f, only_blocked, filters);
}

void Tracker::dump_threads(ceph::Formatter* f)
{
  const auto now = ceph::coarse_mono_clock::now();
  std::vector<TrackedRequest::ThreadUse> uses;
  op_tracker.visit_all_ops_in_flight([&](TrackedOp& op) {
    uses.push_back(static_cast<TrackedRequest&>(op).thread_use(now));
    return true;
  });
  // longest-blocked first, so an exhausted pool reads top-down
  std::sort(uses.begin(), uses.end(), [](const auto& a, const auto& b) {
    return std::tie(b.blocked, a.blocked_for) <
           std::tie(a.blocked, b.blocked_for);
  });

  size_t blocked = 0;
  std::set<uint64_t> seen;
  for (const auto& use : uses) {
    seen.insert(use.thread);
    if (use.blocked) {
      ++blocked;
    }
  }

  f->open_object_section("rgw_threads");
  f->open_array_section("threads");
  for (const auto& use : uses) {
    f->open_object_section("thread");
    f->dump_unsigned("tid", use.thread);
    f->dump_string("serving", use.trans_id);
    f->dump_string("op", use.op);
    f->dump_bool("blocked", use.blocked);
    if (use.blocked) {
      f->dump_string("blocked_on", use.blocked_on);
      f->dump_float("blocked_for", use.blocked_for);
    }
    f->close_section();
  }
  f->close_section();

  f->dump_unsigned("pool_size", pool_size);
  // not "threads busy": on the async path one thread runs many requests, and
  // a suspended request still names the thread it last ran on
  f->dump_unsigned("distinct_threads_seen", seen.size());
  f->dump_unsigned("threads_blocked", blocked);
  if (probe) {
    f->open_object_section("executor_probe");
    const bool outstanding = probe->outstanding.load();
    f->dump_bool("outstanding", outstanding);
    if (outstanding) {
      f->dump_float("outstanding_for", seconds_since(probe->sent, now));
    }
    f->dump_float("last_latency", probe->last_latency.load());
    f->close_section();
  }
  f->close_section();
}

void Tracker::start(boost::asio::io_context& ctx, size_t pool_size,
                    ceph::timespan tick)
{
  frontend = &ctx;
  this->pool_size = pool_size;
  probe = std::make_shared<Probe>();
  stopping = false;
  watchdog = make_named_thread("rgw_op_watchdog",
                               [this, tick] { watchdog_entry(tick); });
}

void Tracker::stop()
{
  if (!watchdog.joinable()) {
    return;
  }
  {
    std::lock_guard l{watchdog_mutex};
    stopping = true;
  }
  watchdog_cond.notify_all();
  watchdog.join();
}

void Tracker::probe_frontend()
{
  const auto now = ceph::coarse_mono_clock::now();

  if (probe->outstanding.load()) {
    // no frontend thread has been able to run anything since the probe was
    // posted, which is what thread pool exhaustion looks like from outside.
    // leave the probe outstanding rather than posting another, so that its
    // age measures the stall instead of a backlog accumulating
    if (now - probe->sent > stall_threshold &&
        now - last_warned > warn_interval) {
      last_warned = now;
      if (perfcounter) {
        perfcounter->inc(l_rgw_frontend_stall);
      }
      lderr(cct) << "frontend executor unresponsive for "
                 << seconds_since(probe->sent, now) << "s; every frontend "
                    "thread is busy or blocked. `ceph daemon <rgw> rgw "
                    "threads` shows what is holding them" << dendl;
    }
    return;
  }

  probe->sent = now;
  probe->outstanding = true;
  boost::asio::post(*frontend, [p = probe] {
    const auto latency = ceph::coarse_mono_clock::now() - p->sent;
    p->last_latency = ceph::to_seconds<double>(latency);
    p->outstanding = false;
    if (perfcounter) {
      perfcounter->tinc(l_rgw_frontend_executor_lat, latency);
    }
  });
}

void Tracker::watchdog_entry(ceph::timespan tick)
{
  std::unique_lock l{watchdog_mutex};
  while (!stopping) {
    watchdog_cond.wait_for(l, tick, [this] { return stopping.load(); });
    if (stopping) {
      break;
    }
    l.unlock();
    probe_frontend();
    l.lock();
  }
}

// -- RequestScope --

RequestScope::RequestScope(Tracker* tracker, uint64_t id, std::string trans_id)
{
  if (tracker == nullptr || !tracker->enabled()) {
    return;
  }
  req.reset(new TrackedRequest(&tracker->ops(), ceph_clock_now(), id,
                               std::move(trans_id)));
  req->tracking_start();
}

RequestScope::~RequestScope()
{
  // dropping the last reference retires it into the tracker's history
}

} // namespace rgw::optracker
