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
#include <string_view>

namespace ceph::async {

class backend_latency;

/// What a request is blocked on, for grouping waits by the kind of resource
/// they contend for.
enum class wait_kind {
  rados,      ///< a RADOS operation
  admission,  ///< the frontend's admission/concurrency limiter
  other,
};

const char* to_string(wait_kind kind);

/// Receives notice that a request has begun and finished waiting on something.
///
/// Implemented by whatever tracks in-flight requests.  Waits nest within a
/// request, but the begin and end may run on different threads when a
/// coroutine suspends in between, so implementations must be internally
/// synchronized.
class wait_sink {
 public:
  virtual ~wait_sink() = default;

  /// Record the start of a wait.  The returned handle identifies it to
  /// wait_end(); an implementation that is not recording may return anything.
  /// `resource` is copied.
  virtual uint64_t wait_begin(wait_kind kind, std::string_view resource,
                              bool blocking) = 0;

  /// Record the end of the wait that wait_begin() returned `handle` for.
  virtual void wait_end(uint64_t handle) = 0;
};

/// Per-request state that travels with an optional_yield.
///
/// Bundled into one struct rather than carried as separate pointers so that
/// adding another per-request accumulator does not widen optional_yield
/// again.  Owned by the request; it must outlive every optional_yield copied
/// from the one it was attached to.
struct request_context {
  /// accumulates time spent waiting on the storage backend
  backend_latency* latency = nullptr;
  /// records individual waits so they can be inspected while in flight
  wait_sink* waits = nullptr;
};

/// Scoped wait.  Reports the wait for as long as this object is alive, and
/// does nothing when constructed from a null sink.
class wait_guard {
 public:
  wait_guard() = default;

  wait_guard(wait_sink* sink, wait_kind kind, std::string_view resource,
             bool blocking)
    : sink(sink)
  {
    if (sink) {
      handle = sink->wait_begin(kind, resource, blocking);
    }
  }

  ~wait_guard() { finish(); }

  wait_guard(wait_guard&& o) noexcept : sink(o.sink), handle(o.handle) {
    o.sink = nullptr;
  }
  wait_guard& operator=(wait_guard&& o) noexcept {
    if (this != &o) {
      finish();
      sink = o.sink;
      handle = o.handle;
      o.sink = nullptr;
    }
    return *this;
  }

  wait_guard(const wait_guard&) = delete;
  wait_guard& operator=(const wait_guard&) = delete;

  /// End the wait early.  Idempotent.
  void finish() {
    if (sink) {
      sink->wait_end(handle);
      sink = nullptr;
    }
  }

 private:
  wait_sink* sink = nullptr;
  uint64_t handle = 0;
};

} // namespace ceph::async
