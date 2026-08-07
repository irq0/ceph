// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>

#include "common/ceph_mutex.h"
#include "common/ceph_time.h"

namespace ceph::async {

/// Accumulates the time an operation spends waiting on a backend.
///
/// Attached to an optional_yield so that calls made under that yield context
/// can report into it without every intervening signature having to carry an
/// accumulator.  Typically owned by a request and read once the request
/// completes; it must outlive every optional_yield copied from the one it was
/// attached to.
///
/// Two durations are tracked because backends are often driven concurrently:
///
///   work: the sum of individual operation durations.  This is total backend
///         work caused by the request and may exceed the request's own
///         latency when operations overlap.
///
///   busy: wall time during which at least one operation was outstanding.
///         This is the share of the request's latency that was spent waiting
///         on the backend, so busy <= the request's total latency.
///
/// busy/work is the concurrency the request actually achieved.
class backend_latency {
 public:
  void op_begin() {
    std::lock_guard l{mutex};
    if (outstanding++ == 0) {
      busy_start = ceph::coarse_mono_clock::now();
    }
  }

  /// Report a completed operation.  Must be paired with an op_begin().
  void op_end(ceph::timespan dur) {
    std::lock_guard l{mutex};
    work_time += dur;
    ++op_count;
    if (--outstanding == 0) {
      busy_time += ceph::coarse_mono_clock::now() - busy_start;
    }
  }

  ceph::timespan work() const {
    std::lock_guard l{mutex};
    return work_time;
  }

  /// Wall time with at least one operation outstanding.  Operations still in
  /// flight are not counted, so read this once the request has quiesced.
  ceph::timespan busy() const {
    std::lock_guard l{mutex};
    return busy_time;
  }

  uint64_t ops() const {
    std::lock_guard l{mutex};
    return op_count;
  }

 private:
  // Completions may arrive on librados finisher threads (blocking path) or on
  // the yield context's strand (async path), so the counters are locked rather
  // than relying on either being serialized.
  mutable ceph::mutex mutex = ceph::make_mutex("ceph::async::backend_latency");
  ceph::timespan work_time = ceph::timespan::zero();
  ceph::timespan busy_time = ceph::timespan::zero();
  ceph::coarse_mono_time busy_start;
  uint64_t op_count = 0;
  uint32_t outstanding = 0;
};

} // namespace ceph::async
