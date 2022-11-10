// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#ifndef RGW_STATUS_PAGE_H
#define RGW_STATUS_PAGE_H

#include <boost/beast/http.hpp>
#include <boost/beast/http/status.hpp>
#include <iostream>
#include <memory>
#include <string>

#include "common/perf_counters_collection.h"

namespace http = boost::beast::http;

class StatusPage {
 public:
  virtual ~StatusPage() = default;
  virtual std::string name() const = 0;
  virtual std::string prefix() const = 0;
  virtual std::string content_type() const = 0;
  virtual http::status render(std::ostream& os) = 0;
};

class PerfCounterStatusPage : public StatusPage {
 private:
  const PerfCountersCollection* perf_counters;

 public:
  PerfCounterStatusPage(const PerfCountersCollection* perf_counters);
  virtual ~PerfCounterStatusPage() override;
  std::string name() const override { return "Perf Counters"; };
  std::string prefix() const override { return "/perf"; };
  std::string content_type() const override { return "text/html"; };
  http::status render(std::ostream& os) override;
};

class PrometheusStatusPage : public StatusPage {
 private:
  const PerfCountersCollection* perf_counters;

 public:
  PrometheusStatusPage(const PerfCountersCollection* perf_counters);
  virtual ~PrometheusStatusPage() override;
  std::string name() const override { return "Prometheus Metrics"; };
  std::string prefix() const override { return "/prometheus"; };
  std::string content_type() const override {
    return "text/plain; version=0.0.4";
  };
  http::status render(std::ostream& os) override;
};

#endif  // RGW_STATUS_PAGE_H
