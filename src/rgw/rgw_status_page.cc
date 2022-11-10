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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <functional>
#include <sstream>
#include <vector>

#include "common/Formatter.h"
#include "common/perf_counters.h"
#include "common/perf_histogram.h"
#include "rgw_op_type.h"
#include "rgw_perf_counters.h"
using namespace fmt::literals;

PerfCounterStatusPage::PerfCounterStatusPage(
    const PerfCountersCollection* perf_counters
)
    : perf_counters(perf_counters) {}

PerfCounterStatusPage::~PerfCounterStatusPage() {}

static int64_t get_quants(int64_t i, PerfHistogramCommon::scale_type_d st) {
  switch (st) {
    case PerfHistogramCommon::SCALE_LINEAR:
      return i;
    case PerfHistogramCommon::SCALE_LOG2:
      return int64_t(1) << (i - 1);
  }
  ceph_assert(false && "Invalid scale type");
}

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

constexpr bool is_scalar(perfcounter_type_d type) {
  return !is_histogram(type) && !is_longrunavg(type);
}

constexpr bool is_counter(perfcounter_type_d type) {
  return is_scalar(type) && (type & PERFCOUNTER_COUNTER) != 0;
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
    return fmt::format("{:.3Lf} {}B ({:d})", mantissa, " KMGT"[i], bytes);
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
              std::ostringstream os;
              // we know the axis of this one
              if (&perf_counters == perfcounter_ops_svc_time_hist) {
                os << "<table><tr>\n";
                const PerfHistogramCommon::axis_config_d ac =
                    perfcounter_op_hist_x_axis_config;

                fmt::print(os, "<th><{}</th>", ac.m_min);
                uint64_t prev_upper = ac.m_min;
                for (int64_t bucket_no = 1; bucket_no < ac.m_buckets - 1;
                     bucket_no++) {
                  uint64_t upper = std::max(
                      0L, ac.m_min + get_quants(bucket_no, ac.m_scale_type) *
                                         ac.m_quant_size
                  );
                  fmt::print(os, "<th><{}</th>", upper);
                  prev_upper = upper;
                }
                fmt::print(os, "<th><∞</th>", prev_upper);

                os << "</tr>\n"
                   << "<tr>\n";

                for (int64_t bucket_no = 0; bucket_no < ac.m_buckets;
                     bucket_no++) {
                  uint64_t count = data.histogram->read_bucket(bucket_no, 0);
                  fmt::print(os, "<td>{}</td>", count);
                }
                os << "</tr>\n"
                   << "</table>";
                return os.str();
              } else {
                auto f = Formatter::create("table");
                bufferlist bl;
                bl.append("<pre>\n");
                data.histogram->dump_formatted(f);
                f->flush(bl);
                bl.append("</pre>\n");
                return bl.to_str();
              }
            } else if (is_longrunavg(data.type)) {
              const auto pair = data.read_avg();
              const auto sum = pair.first;
              const auto count = pair.second;
              const auto avg =
                  static_cast<double>(sum) /
                  std::max(static_cast<double>(1), static_cast<double>(count));
              if (data.type & PERFCOUNTER_TIME) {
                return fmt::format(
                    "<ul><li>sum: {}</li><li>count: "
                    "{}</li><li>avg: {}</li></ul>",
                    std::chrono::nanoseconds(sum).count(), count,
                    std::chrono::nanoseconds(static_cast<uint64_t>(avg)).count()
                );
              } else {
                return fmt::format(
                    "<ul><li>sum: {}</li><li>count: "
                    "{}</li><li>avg: {:.2f}</li></ul>",
                    sum, count, avg
                );
              }
            } else {
              if (data.type & PERFCOUNTER_U64) {
                return format_int_value(data.u64, data.unit);
              } else if (data.type & PERFCOUNTER_TIME) {
                return fmt::format(
                    "{:d}.{:09d}s", data.u64 / 1000000000ull,
                    data.u64 % 1000000000ull
                );
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

constexpr const char* metric_type_prom(perfcounter_type_d type) {
  if (is_histogram(type)) {
    return "histogram";
  } else if (is_longrunavg(type)) {
    return "gauge";
  } else {
    return metric_type(type);
  }
}

PrometheusStatusPage::PrometheusStatusPage(
    const PerfCountersCollection* perf_counters
)
    : perf_counters(perf_counters) {}

PrometheusStatusPage::~PrometheusStatusPage() {}

static std::string format_labels(const std::vector<std::string>& labels) {
  if (labels.empty()) {
    return "";
  } else {
    return fmt::format("{{{}}}", fmt::join(labels, ", "));
  }
}

http::status PrometheusStatusPage::render(std::ostream& os) {
  os << "# s3gw prometheus exporter\n";
  perf_counters->with_counters(
      [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
        bool printing_collection = false;
        for (const auto& kv : by_path) {
          auto& path = kv.first;
          auto& data = *(kv.second.data);
          auto perf_counters = kv.second.perf_counters;
          auto& collection = perf_counters->get_name();
          // not interesting
          if (std::string::npos != path.find("mempool.")) {
            continue;
          }
          // accessed when printing service time histograms
          if (perf_counters == perfcounter_ops_svc_time_sum) {
            continue;
          }
          std::string name(path);
          std::vector<std::string> labels;

          auto format_scalar = [&]() {
            // TODO convert this to two metrics. _sum and _avgcount?
            if (is_longrunavg(data.type)) {
              const auto pair = data.read_avg();
              const auto sum = static_cast<double>(pair.first);
              const auto count = static_cast<double>(pair.second);
              const auto avg =
                  sum / std::max(static_cast<decltype(count)>(1), count);
              return fmt::format("{:f}", avg);
            } else {
              if (data.type & PERFCOUNTER_U64) {
                return format_int_value(data.u64, data.unit);
              } else if (data.type & PERFCOUNTER_TIME) {
                return fmt::format(
                    "{:d}.{:09d}", data.u64 / 1000000000ull,
                    data.u64 % 1000000000ull
                );
              } else {
                return std::string("-23.42");
              }
            }
          };

          // combine selected perf counter collections into single,
          // labeled prometheus metric
          bool print_header = true;
          if (perf_counters == perfcounter_ops_svc_time_hist) {
            labels.emplace_back(fmt::format("op=\"{}\"", data.name));
            print_header = !printing_collection;
            printing_collection = true;
            name = collection;
          } else {
            printing_collection = false;
          }
          std::replace(name.begin(), name.end(), '.', '_');
          std::replace(name.begin(), name.end(), '-', '_');

          if (is_counter(data.type)) {
            // prometheus: counter metrics should have "_total" suffix
            name.append("_total");
          }

          if (print_header) {
            fmt::print(
                os, R"(
# HELP {name} {descr} ({human_type} {human_value_type})
# TYPE {name} {type}
)",
                "name"_a = name,
                "descr"_a = data.description ? data.description : "",
                "human_type"_a = metric_type_human(data.type),
                "human_value_type"_a = metric_value_type_human(data.type),
                "type"_a = metric_type_prom(data.type)
            );
          }

          // 1D ceph perf histogram + time counter -> prometheus histogram
          if (perf_counters == perfcounter_ops_svc_time_hist) {
            name = collection;

            const PerfHistogramCommon::axis_config_d ac =
                perfcounter_op_hist_x_axis_config;

            uint64_t count = 0;
            for (int64_t bucket_no = 0; bucket_no < ac.m_buckets; bucket_no++) {
              std::vector<std::string> bucket_labels(labels);
              bucket_labels.emplace_back(
                  (bucket_no == ac.m_buckets - 1)
                      ? "le=\"+Inf\""
                      : fmt::format(
                            "le=\"{}\"",
                            std::max(
                                0L, (ac.m_min +
                                     get_quants(bucket_no, ac.m_scale_type) *
                                         ac.m_quant_size) -
                                        1
                            )
                        )
              );
              count += data.histogram->read_bucket(bucket_no, 0);
              fmt::print(
                  os, "{name}_bucket{labels} {value}\n", "name"_a = name,
                  "labels"_a = format_labels(bucket_labels), "value"_a = count
              );
            }

            // XXX histogram buckets may be any time resolution. This has to match
            const auto sum = perfcounter_ops_svc_time_sum->tget(data.idx);
            const auto str_labels = format_labels(labels);
            fmt::print(os, "{}_sum{} {}\n", name, str_labels, sum.usec());
            fmt::print(os, "{}_count{} {}\n\n", name, str_labels, count);
          } else if (!is_histogram(data.type)) {
            fmt::print(
                os, "{name}{labels} {value}\n", "name"_a = name,
                "labels"_a = format_labels(labels),
                "type"_a = metric_type_prom(data.type),
                "value"_a = format_scalar()
            );
          } else {
            fmt::print(
                os,
                "# {name}{labels} unsupported perf counter conversion "
                "({type} {value_type})\n",
                "type"_a = metric_type_human(data.type),
                "value_type"_a = metric_value_type_human(data.type),
                "name"_a = name, "labels"_a = format_labels(labels)
            );
          }
        }
      }
  );
  return http::status::ok;
}
