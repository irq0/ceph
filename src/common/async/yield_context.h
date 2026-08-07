// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>

#include "acconfig.h"

namespace ceph::async { class backend_latency; }

/// optional-like wrapper for a boost::asio::yield_context. operations that take
/// an optional_yield argument will, when passed a non-empty yield context,
/// suspend this coroutine instead of the blocking the thread of execution
///
/// may also carry a backend_latency sink. this travels with the call rather
/// than with the thread of execution, so it stays correct across a coroutine
/// suspension, which a thread_local accumulator would not. the sink is not
/// owned and must outlive every copy of this object
class optional_yield {
  boost::asio::yield_context *y = nullptr;
  ceph::async::backend_latency *lat = nullptr;
 public:
  /// construct with a valid io and yield_context
  optional_yield(boost::asio::yield_context& y) noexcept : y(&y) {}

  /// type tag to construct an empty object
  struct empty_t {};
  optional_yield(empty_t) noexcept {}

  /// implicit conversion to bool, returns true if non-empty
  operator bool() const noexcept { return y; }

  /// return a reference to the yield_context. only valid if non-empty
  boost::asio::yield_context& get_yield_context() const noexcept { return *y; }

  /// return a copy that reports backend latency into the given sink. the sink
  /// must outlive the returned object and everything copied from it
  optional_yield with_latency_sink(
      ceph::async::backend_latency *sink) const noexcept {
    optional_yield copy{*this};
    copy.lat = sink;
    return copy;
  }

  /// the backend_latency sink to report into, or null if there is none
  ceph::async::backend_latency *latency_sink() const noexcept { return lat; }
};

// type tag object to construct an empty optional_yield
static constexpr optional_yield::empty_t null_yield{};
