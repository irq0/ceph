#include "rgw_filestore_types.h"

#include <chrono>
#include <ostream>

#include "common/Formatter.h"
#include "include/ceph_features.h"
#include "include/encoding.h"
#include "include/stringify.h"

using namespace std::chrono_literals;
using ceph::bufferlist;
using ceph::decode;
using ceph::decode_nohead;
using ceph::encode;
using ceph::encode_nohead;

size_t rgw_saloid_t::encoded_size() const {
  return 23;
}

size_t rgw_salcoll_t::encoded_size() const {
  return 100;
}

void rgw_saloid_t::encode(ceph::buffer::list& bl) const {
  // TODO
}

void rgw_saloid_t::decode(ceph::buffer::list::const_iterator& bl) {
  // TODO
}

void rgw_salcoll_t::encode(ceph::buffer::list& bl) const {
  // TODO
}

void rgw_salcoll_t::decode(ceph::buffer::list::const_iterator& bl) {
  // TODO
}

int cmp(const rgw_saloid_t& l, const rgw_saloid_t& r) {
  if (l.name < r.name) {
    return -1;
  } else {
    return 1;
  }
}

std::ostream& operator<<(std::ostream& out, const rgw_saloid_t& o) {
  out << "saloid{}";
  return out;
}

// TODO
bool rgw_salcoll_t::parse(const std::string& str) {
  name = str;
  return true;
}

std::ostream& operator<<(std::ostream& out, const rgw_salcoll_t& o) {
  out << "salcoll{}";
  return out;
}

void rgw_filestore_perf_stat_t::dump(Formatter* f) const {
  // *_ms values just for compatibility.
  f->dump_float("commit_latency_ms", os_commit_latency_ns / 1000000.0);
  f->dump_float("apply_latency_ms", os_apply_latency_ns / 1000000.0);
  f->dump_unsigned("commit_latency_ns", os_commit_latency_ns);
  f->dump_unsigned("apply_latency_ns", os_apply_latency_ns);
}

void rgw_filestore_perf_stat_t::encode(ceph::buffer::list& bl,
                                     uint64_t features) const {
  uint8_t target_v = 2;
  if (!HAVE_FEATURE(features, OS_PERF_STAT_NS)) {
    target_v = 1;
  }
  ENCODE_START(target_v, target_v, bl);
  if (target_v >= 2) {
    encode(os_commit_latency_ns, bl);
    encode(os_apply_latency_ns, bl);
  } else {
    constexpr auto NS_PER_MS = std::chrono::nanoseconds(1ms).count();
    uint32_t commit_latency_ms = os_commit_latency_ns / NS_PER_MS;
    uint32_t apply_latency_ms = os_apply_latency_ns / NS_PER_MS;
    encode(commit_latency_ms, bl);  // for compatibility with older monitor.
    encode(apply_latency_ms, bl);   // for compatibility with older monitor.
  }
  ENCODE_FINISH(bl);
}

void rgw_filestore_perf_stat_t::decode(ceph::buffer::list::const_iterator& bl) {
  DECODE_START(2, bl);
  if (struct_v >= 2) {
    decode(os_commit_latency_ns, bl);
    decode(os_apply_latency_ns, bl);
  } else {
    uint32_t commit_latency_ms;
    uint32_t apply_latency_ms;
    decode(commit_latency_ms, bl);
    decode(apply_latency_ms, bl);
    constexpr auto NS_PER_MS = std::chrono::nanoseconds(1ms).count();
    os_commit_latency_ns = commit_latency_ms * NS_PER_MS;
    os_apply_latency_ns = apply_latency_ms * NS_PER_MS;
  }
  DECODE_FINISH(bl);
}

void rgw_filestore_perf_stat_t::generate_test_instances(
    std::list<rgw_filestore_perf_stat_t*>& o) {
  o.push_back(new rgw_filestore_perf_stat_t());
  o.push_back(new rgw_filestore_perf_stat_t());
  o.back()->os_commit_latency_ns = 20000000;
  o.back()->os_apply_latency_ns = 30000000;
}

void dump(Formatter* f, const osd_alerts_t& alerts) {
  for (auto& a : alerts) {
    std::string s0 = " osd: ";
    s0 += stringify(a.first);
    std::string s;
    for (auto& aa : a.second) {
      s = s0;
      s += " ";
      s += aa.first;
      s += ":";
      s += aa.second;
      f->dump_string("alert", s);
    }
  }
}
