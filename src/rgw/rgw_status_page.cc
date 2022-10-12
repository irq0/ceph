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

#include "rgw_status_page.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <chrono>

#include "common/Formatter.h"
#include "common/perf_counters.h"

using namespace fmt::literals;

PerfCounterStatusPage::PerfCounterStatusPage(
    const PerfCountersCollection* perf_counters
)
    : perf_counters(perf_counters) {}

PerfCounterStatusPage::~PerfCounterStatusPage() {}

constexpr const char* metric_type(perfcounter_type_d type) {
  if (type & PERFCOUNTER_COUNTER) {
    return "counter";
  } else {
    return "gauge";
  }
}

constexpr bool is_histogram(perfcounter_type_d type) {
  return (type & PERFCOUNTER_HISTOGRAM) != 0;
}

constexpr bool is_longrunavg(perfcounter_type_d type) {
  return (type & PERFCOUNTER_LONGRUNAVG) != 0;
}

constexpr const char* metric_type_human(perfcounter_type_d type) {
  if (is_histogram(type)) {
    return "histogram";
  } else if (is_longrunavg(type)) {
    return "running avg";
  } else {
    return metric_type(type);
  }
}

constexpr const char* metric_value_type_human(perfcounter_type_d type) {
  if (type & PERFCOUNTER_TIME) {
    return "time";
  } else {
    return "int";
  }
}

static std::string format_bytes(uint64_t bytes) {
  if (bytes < 1024) {
    return fmt::format("{} B", bytes);
  } else {
    double mantissa = bytes;
    int i = 0;
    for (; mantissa >= 1024 && i < 5; mantissa /= 1024, ++i) {
    }
    return fmt::format("{:.3Lf} {}B ({:d})", mantissa, "_KMGT"[i], bytes);
  }
}

static std::string format_int_value(uint64_t val, unit_t unit) {
  if (unit == unit_t::UNIT_BYTES) {
    return format_bytes(val);
  } else {
    return fmt::format("{:L}", val);
  }
}

http::status PerfCounterStatusPage::render(std::ostream& os) {
  os << R"(
<h1>Perf Counters</h1>
<table>
<thead>
  <tr>
    <th>Path</th>
    <th>Description</th>
    <th>Type</th>
    <th>Value Type</th>
    <th>Prio</th>
    <th>Value</th>
  </tr>
</thead>
<tbody>
)";

  perf_counters->with_counters(
      [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
        for (const auto& kv : by_path) {
          auto& path = kv.first;
          auto& data = *(kv.second.data);
          auto& perf_counters = *(kv.second.perf_counters);
          if (std::string::npos != path.find("mempool.")) {
            continue;
          }

          auto format_value = [&]() {
            if (is_histogram(data.type)) {
              auto f = Formatter::create("table");
              bufferlist bl;
              perf_counters.dump_formatted_histograms(f, false);
              f->flush(bl);
              return bl.to_str();
            } else if (is_longrunavg(data.type)) {
              // TODO properly format PERFCOUNTER_TIME
              auto avg = data.read_avg();
              return fmt::format(
                  "(sum, avgcount)=({}, {})", avg.first, avg.second
              );
            } else {
              if (data.type & PERFCOUNTER_U64) {
                return format_int_value(data.u64, data.unit);
              } else if (data.type & PERFCOUNTER_TIME) {
                std::chrono::seconds secs{data.u64};
                return fmt::format("{:L}", secs.count());
              } else {
                return std::string("???");
              }
            }
          };

          fmt::print(
              os, R"(
<tr>
  <td>{path}</td>
  <td>{descr}</td>
  <td>{type}</td>
  <td>{value_type}</td>
  <td>{prio}</td>
  <td>{value}</td>
</tr>
)",
              "path"_a = path,
              "descr"_a = data.description ? data.description : "",
              "type"_a = metric_type_human(data.type),
              "value_type"_a = metric_value_type_human(data.type),
              "prio"_a = perf_counters.get_adjusted_priority(data.prio),
              "value"_a = format_value()
          );
        }
      }
  );

  os << R"(
</tbody>
</table>
)";

  return http::status::ok;
}
