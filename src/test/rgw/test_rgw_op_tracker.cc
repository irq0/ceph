// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/admin_socket.h"
#include "common/ceph_json.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "rgw_op_tracker.h"

using namespace std::chrono_literals;

namespace rgw::optracker {

namespace {

/// Render a dump into parsed JSON so tests assert on structure rather than
/// on formatting.  Returned by pointer because the JSONObj tree points into
/// the parser's own buffer.
///
/// Note that JSONFormatter drops the name of the outermost section, so the
/// parser's root object *is* that section and its fields are looked up
/// directly on the parser.
std::unique_ptr<JSONParser> parse(
    const std::function<void(ceph::Formatter*)>& dump)
{
  ceph::JSONFormatter f{false};
  dump(&f);
  std::ostringstream os;
  f.flush(os);
  const std::string s = os.str();
  auto parser = std::make_unique<JSONParser>();
  EXPECT_TRUE(parser->parse(s.c_str(), s.size())) << s;
  return parser;
}

std::string field(JSONObj* obj, const char* name)
{
  if (obj == nullptr) {
    return {};
  }
  JSONObj* child = obj->find_obj(name);
  return child != nullptr ? child->get_data() : std::string{};
}

void set_tracking(bool on)
{
  g_ceph_context->_conf.set_val("rgw_op_tracker", on ? "true" : "false");
  g_ceph_context->_conf.apply_changes(nullptr);
}

} // namespace

class OpTrackerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    g_ceph_context->_conf.set_val("rgw_op_tracker_complaint_time", "30");
    set_tracking(true);
    tracker = std::make_unique<Tracker>(g_ceph_context);
    ASSERT_TRUE(tracker->enabled());
  }

  void TearDown() override {
    tracker.reset();
  }

  std::unique_ptr<Tracker> tracker;
};

TEST_F(OpTrackerTest, DisabledTrackerMakesNoRequest)
{
  // radosgw creates exactly one tracker, so replace the fixture's rather
  // than standing a second one up beside it
  tracker.reset();
  set_tracking(false);
  Tracker off{g_ceph_context};
  EXPECT_FALSE(off.enabled());

  RequestScope scope{&off, 1, "tx1"};
  EXPECT_FALSE(static_cast<bool>(scope));
  EXPECT_EQ(nullptr, scope.get());
  EXPECT_EQ(nullptr, scope.sink());
  // callers must be able to mark phases unconditionally
  mark_event(scope.get(), "init permissions");
}

TEST_F(OpTrackerTest, NullTrackerIsTolerated)
{
  RequestScope scope{nullptr, 1, "tx1"};
  EXPECT_FALSE(static_cast<bool>(scope));
  mark_event(scope.get(), "init permissions");
}

/// The reason the tracker observes its own config option: it has to be
/// possible to switch on during the incident it exists to diagnose.
TEST_F(OpTrackerTest, TrackingFollowsConfigWithoutRestart)
{
  set_tracking(false);
  EXPECT_FALSE(tracker->enabled());
  {
    RequestScope scope{tracker.get(), 1, "tx1"};
    EXPECT_FALSE(static_cast<bool>(scope));
  }

  set_tracking(true);
  EXPECT_TRUE(tracker->enabled());
  RequestScope scope{tracker.get(), 2, "tx2"};
  EXPECT_TRUE(static_cast<bool>(scope));
  EXPECT_EQ(1u, tracker->ops().get_num_ops_in_flight());
}

TEST_F(OpTrackerTest, InFlightReportsIdentityAndPhase)
{
  RequestScope scope{tracker.get(), 42, "tx42"};
  ASSERT_TRUE(static_cast<bool>(scope));
  scope.get()->set_op("PUT", "put_obj", "10.0.0.1");
  scope.get()->set_target("bucket1", "key1", "user1");
  mark_event(scope.get(), "executing");

  auto parser = parse([&](ceph::Formatter* f) {
    tracker->dump_in_flight(f, false, {""});
  });
  JSONObj* ops = parser->find_obj("ops");
  ASSERT_NE(nullptr, ops);
  auto entries = ops->get_array_elements();
  ASSERT_EQ(1u, entries.size());

  JSONParser op;
  ASSERT_TRUE(op.parse(entries[0].c_str(), entries[0].size()));
  JSONObj* type_data = op.find_obj("type_data");
  ASSERT_NE(nullptr, type_data);
  EXPECT_EQ("tx42", field(type_data, "trans_id"));
  EXPECT_EQ("PUT", field(type_data, "method"));
  EXPECT_EQ("put_obj", field(type_data, "op"));
  EXPECT_EQ("bucket1", field(type_data, "bucket"));
  EXPECT_EQ("key1", field(type_data, "object"));
  EXPECT_EQ("user1", field(type_data, "user"));
  EXPECT_EQ("10.0.0.1", field(type_data, "remote"));
  // the phase timeline: which step the request has reached. TrackedOp::dump()
  // does not emit events itself, so this only appears if _dump() adds it
  EXPECT_EQ("executing", field(type_data, "phase"));
  JSONObj* phases = type_data->find_obj("phases");
  ASSERT_NE(nullptr, phases);
  auto steps = phases->get_array_elements();
  ASSERT_GE(steps.size(), 2u);   // "initiated" from tracking_start, then ours
  JSONParser last;
  ASSERT_TRUE(last.parse(steps.back().c_str(), steps.back().size()));
  EXPECT_EQ("executing", field(&last, "event"));
  // the description is what an operator reads first
  EXPECT_NE(std::string::npos, field(&op, "description").find("tx42"));
}

TEST_F(OpTrackerTest, RetiredRequestLeavesInFlight)
{
  {
    RequestScope scope{tracker.get(), 1, "tx1"};
    scope.get()->set_op("GET", "get_obj", "");
    EXPECT_EQ(1u, tracker->ops().get_num_ops_in_flight());
  }
  EXPECT_EQ(0u, tracker->ops().get_num_ops_in_flight());

  // and shows up in history instead. OpHistory hands insertion to its own
  // service thread, so this is not synchronous with the request retiring
  size_t historic = 0;
  for (int i = 0; i < 100 && historic == 0; ++i) {
    auto parser = parse([&](ceph::Formatter* f) {
      tracker->ops().dump_historic_ops(f, false, {""});
    });
    JSONObj* ops = parser->find_obj("ops");
    ASSERT_NE(nullptr, ops);
    historic = ops->get_array_elements().size();
    if (historic == 0) {
      std::this_thread::sleep_for(10ms);
    }
  }
  EXPECT_EQ(1u, historic);
}

TEST_F(OpTrackerTest, WaitsAreScopedAndOrdered)
{
  RequestScope scope{tracker.get(), 1, "tx1"};
  auto* req = scope.get();
  ASSERT_NE(nullptr, req);

  EXPECT_TRUE(req->current_waits().empty());

  const uint64_t outer =
      req->wait_begin(wait_kind::admission, "admission", false);
  const uint64_t inner = req->wait_begin(wait_kind::rados, ".dir.0", true);

  auto waits = req->current_waits();
  ASSERT_EQ(2u, waits.size());
  // pushed in order, so the oldest is first
  EXPECT_EQ("admission", waits[0].resource);
  EXPECT_FALSE(waits[0].blocking);
  EXPECT_EQ(".dir.0", waits[1].resource);
  EXPECT_TRUE(waits[1].blocking);

  req->wait_end(inner);
  waits = req->current_waits();
  ASSERT_EQ(1u, waits.size());
  EXPECT_EQ("admission", waits[0].resource);

  req->wait_end(outer);
  EXPECT_TRUE(req->current_waits().empty());

  // an unknown or repeated handle must not disturb the stack
  req->wait_end(inner);
  req->wait_end(9999);
  EXPECT_TRUE(req->current_waits().empty());
}

TEST_F(OpTrackerTest, WaitsCloseOutOfOrder)
{
  RequestScope scope{tracker.get(), 1, "tx1"};
  auto* req = scope.get();
  const uint64_t first = req->wait_begin(wait_kind::rados, "a", false);
  const uint64_t second = req->wait_begin(wait_kind::rados, "b", false);

  // concurrent RADOS ops under one request need not complete in order
  req->wait_end(first);
  auto waits = req->current_waits();
  ASSERT_EQ(1u, waits.size());
  EXPECT_EQ("b", waits[0].resource);
  req->wait_end(second);
  EXPECT_TRUE(req->current_waits().empty());
}

TEST_F(OpTrackerTest, WaitsAgeFromWhenTheyBegan)
{
  RequestScope scope{tracker.get(), 1, "tx1"};
  auto* req = scope.get();

  req->wait_begin(wait_kind::rados, "a", false);
  std::this_thread::sleep_for(20ms);
  req->wait_begin(wait_kind::rados, "b", false);

  const auto now = ceph::coarse_mono_clock::now();
  const auto waits = req->current_waits();
  ASSERT_EQ(2u, waits.size());
  // the oldest wait is the one to report a request as stuck on
  EXPECT_GT(waits[0].age(now), waits[1].age(now));
}

/// A blocking wait is the whole basis of the thread view: it is the one thing
/// that says a frontend thread is unavailable rather than merely suspended.
TEST_F(OpTrackerTest, ThreadViewCountsOnlyBlockingWaits)
{
  auto threads = [&] {
    return parse([&](ceph::Formatter* f) { tracker->dump_threads(f); });
  };

  RequestScope scope{tracker.get(), 1, "tx1"};
  scope.get()->set_op("GET", "get_obj", "");

  // suspended on an async op: the thread went back to the pool
  const uint64_t async = scope.get()->wait_begin(wait_kind::rados, "a", false);
  {
    auto parser = threads();
    EXPECT_EQ("1", field(parser.get(), "distinct_threads_seen"));
    EXPECT_EQ("0", field(parser.get(), "threads_blocked"));
  }
  scope.get()->wait_end(async);

  // entered without a yield context: the thread is parked in the call
  const uint64_t sync = scope.get()->wait_begin(wait_kind::rados, "b", true);
  {
    auto parser = threads();
    EXPECT_EQ("1", field(parser.get(), "threads_blocked"));
    JSONObj* list = parser->find_obj("threads");
    ASSERT_NE(nullptr, list);
    auto entries = list->get_array_elements();
    ASSERT_EQ(1u, entries.size());
    JSONParser t;
    ASSERT_TRUE(t.parse(entries[0].c_str(), entries[0].size()));
    EXPECT_EQ("tx1", field(&t, "serving"));
    EXPECT_EQ("get_obj", field(&t, "op"));
    EXPECT_EQ("b", field(&t, "blocked_on"));
  }

  // and the mark goes away with the wait, rather than needing to be cleared
  scope.get()->wait_end(sync);
  EXPECT_EQ("0", field(threads().get(), "threads_blocked"));
}

TEST_F(OpTrackerTest, WaitsAreSafeAcrossThreads)
{
  RequestScope scope{tracker.get(), 1, "tx1"};
  auto* req = scope.get();

  // a wait can begin on one thread and end on another when the coroutine
  // resumes elsewhere
  std::atomic<uint64_t> handle{0};
  std::thread opener{[&] {
    handle = req->wait_begin(wait_kind::rados, "obj", false);
  }};
  opener.join();
  EXPECT_EQ(1u, req->current_waits().size());

  std::thread closer{[&] { req->wait_end(handle.load()); }};
  closer.join();
  EXPECT_TRUE(req->current_waits().empty());
}

TEST_F(OpTrackerTest, FilterMatchesIdentityFields)
{
  RequestScope a{tracker.get(), 1, "tx1"};
  a.get()->set_op("PUT", "put_obj", "");
  a.get()->set_target("bucket1", "key1", "");
  RequestScope b{tracker.get(), 2, "tx2"};
  b.get()->set_op("GET", "get_obj", "");
  b.get()->set_target("bucket2", "key2", "");

  auto count = [&](const std::set<std::string>& filters) {
    auto parser = parse([&](ceph::Formatter* f) {
      tracker->dump_in_flight(f, false, filters);
    });
    JSONObj* ops = parser->find_obj("ops");
    EXPECT_NE(nullptr, ops);
    return ops->get_array_elements().size();
  };

  EXPECT_EQ(2u, count({""}));
  EXPECT_EQ(1u, count({"put_obj"}));
  EXPECT_EQ(1u, count({"bucket2"}));
  EXPECT_EQ(2u, count({"put_obj", "get_obj"}));
  EXPECT_EQ(0u, count({"nonesuch"}));
}

TEST_F(OpTrackerTest, BlockedViewExcludesYoungRequests)
{
  RequestScope scope{tracker.get(), 1, "tx1"};
  auto parser = parse([&](ceph::Formatter* f) {
    // complaint time is 30s, so a request created just now is not blocked
    tracker->dump_in_flight(f, true, {""});
  });
  JSONObj* ops = parser->find_obj("ops");
  ASSERT_NE(nullptr, ops);
  EXPECT_TRUE(ops->get_array_elements().empty());
}

/// Registration and dispatch through the real AdminSocket, which is what a
/// `ceph daemon client.rgw.X ...` invocation exercises: the command
/// descriptors have to parse, and the arguments have to reach call().
TEST_F(OpTrackerTest, AdminSocketCommandsRegisterAndDispatch)
{
  ASSERT_EQ(0, tracker->hook_to_admin_socket());

  RequestScope a{tracker.get(), 1, "tx1"};
  a.get()->set_op("PUT", "put_obj", "");
  a.get()->set_target("bucket1", "key1", "");
  a.get()->wait_begin(wait_kind::rados, ".dir.bucket1.7", false);
  RequestScope b{tracker.get(), 2, "tx2"};
  b.get()->set_op("GET", "get_obj", "");
  b.get()->set_target("bucket2", "key2", "");

  auto* asok = g_ceph_context->get_admin_socket();
  auto run = [&](const std::vector<std::string>& cmd) {
    bufferlist out;
    std::ostringstream errss;
    const int r = asok->execute_command(cmd, {}, errss, &out);
    EXPECT_EQ(0, r) << cmd[0] << ": " << errss.str();
    return out.to_str();
  };

  for (const char* cmd : {"rgw ops in_flight", "rgw ops blocked",
                          "rgw ops historic", "rgw ops historic_slow",
                          "rgw threads"}) {
    const std::string out = run({std::string{"{\"prefix\": \""} +
                                 cmd + "\"}"});
    JSONParser parser;
    EXPECT_TRUE(parser.parse(out.c_str(), out.size())) << cmd << ": " << out;
  }

  // the whole point of in_flight is naming what a request waits on
  {
    const std::string out = run({"{\"prefix\": \"rgw ops in_flight\"}"});
    EXPECT_NE(std::string::npos, out.find("tx1")) << out;
    EXPECT_NE(std::string::npos, out.find(".dir.bucket1.7")) << out;
  }

  // and the filter argument has to survive the command descriptor
  {
    const std::string out =
        run({"{\"prefix\": \"rgw ops in_flight\", "
             "\"filterstr\": [\"put_obj\"]}"});
    EXPECT_NE(std::string::npos, out.find("tx1")) << out;
    EXPECT_EQ(std::string::npos, out.find("tx2")) << out;
  }
}

/// The commands are registered even when tracking is off, so that the error
/// says what to do rather than that the command does not exist.
TEST_F(OpTrackerTest, AdminSocketSaysHowToEnableWhenDisabled)
{
  ASSERT_EQ(0, tracker->hook_to_admin_socket());
  set_tracking(false);

  bufferlist out;
  std::ostringstream errss;
  const int r = g_ceph_context->get_admin_socket()->execute_command(
      {"{\"prefix\": \"rgw ops in_flight\"}"}, {}, errss, &out);
  EXPECT_EQ(-ENOTSUP, r);
  EXPECT_NE(std::string::npos, errss.str().find("rgw_op_tracker"))
      << errss.str();
}

/// The exhaustion detector: a task posted to a busy executor lands promptly,
/// and one posted to a wedged executor does not.  This is the one thing here
/// that works with tracking off, which is why the watchdog is unconditional.
TEST_F(OpTrackerTest, WatchdogProbeDetectsAWedgedExecutor)
{
  boost::asio::io_context ctx;
  auto guard = boost::asio::make_work_guard(ctx);

  std::atomic<bool> wedge{false};
  std::thread runner{[&] {
    // a single-threaded executor stands in for a pool whose every thread is
    // stuck in a synchronous call
    while (!wedge) {
      ctx.run_for(10ms);
    }
  }};

  tracker->start(ctx, 1, 100ms);

  auto executor_probe = [&](JSONParser* parser) {
    JSONObj* probe = parser->find_obj("executor_probe");
    EXPECT_NE(nullptr, probe);
    return probe;
  };

  // healthy: probes land promptly, so none of them is outstanding for long
  std::this_thread::sleep_for(400ms);
  {
    auto parser = parse([&](ceph::Formatter* f) { tracker->dump_threads(f); });
    JSONObj* probe = executor_probe(parser.get());
    ASSERT_NE(nullptr, probe);
    const std::string age = field(probe, "outstanding_for");
    if (!age.empty()) {
      EXPECT_LT(std::stod(age), 0.5);
    }
  }

  // nothing runs the executor from here on
  wedge = true;
  runner.join();
  std::this_thread::sleep_for(400ms);
  {
    auto parser = parse([&](ceph::Formatter* f) { tracker->dump_threads(f); });
    JSONObj* probe = executor_probe(parser.get());
    ASSERT_NE(nullptr, probe);
    EXPECT_EQ("true", field(probe, "outstanding"));
    EXPECT_GT(std::stod(field(probe, "outstanding_for")), 0.1);
  }

  tracker->stop();
  guard.reset();
  ctx.stop();
}

} // namespace rgw::optracker

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
