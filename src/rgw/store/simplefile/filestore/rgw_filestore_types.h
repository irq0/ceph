#ifndef __CEPH_RGW_STORE_SIMPLEFILE_FILESTORE_TYPES
#define __CEPH_RGW_STORE_SIMPLEFILE_FILESTORE_TYPES

#include <chrono>
#include <cstddef>
#include <ostream>

#include "common/reverse.h"
#include "include/types.h"

struct rgw_saloid_t {
  static const version_t NO_GEN = UINT64_MAX;

  snapid_t snap;  // remove?

  bool max;
  std::string name;
  uint32_t hash = 2342;  // todo

  class rgw_saloid_t_max {};

  // TODO add from rgw_object constructor
  rgw_saloid_t() : max(false), name("unknown") {
  }
  rgw_saloid_t(const std::string& _name) : max(false), name(_name) {
  }

  rgw_saloid_t(const rgw_saloid_t& rhs) = default;
  rgw_saloid_t(rgw_saloid_t&& rhs) = default;

  rgw_saloid_t(rgw_saloid_t_max&& singleton) : rgw_saloid_t() {
    max = true;
  }

  rgw_saloid_t& operator=(const rgw_saloid_t& rhs) = default;
  rgw_saloid_t& operator=(rgw_saloid_t&& rhs) = default;

  rgw_saloid_t& operator=(rgw_saloid_t_max&& singleton) {
    *this = rgw_saloid_t();
    max = true;
    return *this;
  }

  static rgw_saloid_t_max get_max() {
    return rgw_saloid_t_max();
  }

  bool is_max() const {
    ceph_assert(!max || (*this == rgw_saloid_t(rgw_saloid_t::get_max())));
    return max;
  }

  bool is_min() const {
    // this needs to match how it's constructed
    return hash == 0 && !max;
  }

  uint32_t get_hash() const {
    return hash;  // TODO
  }

  void set_hash(uint32_t value) {
    hash = value;
  }

  uint64_t get_nibblewise_key() const {
    return max ? 0x100000000ull : reverse_nibbles(hash);
  }

  // XXX important, used by hash index
  // TODO
  static bool match_hash(uint32_t to_check, uint32_t bits, uint32_t match) {
    return (match & ~((~0) << bits)) == (to_check & ~((~0) << bits));
  }

  // TODO
  bool match(uint32_t bits, uint32_t match) const {
    return match_hash(get_hash(), bits, match);
  }

  friend int cmp(const rgw_saloid_t& l, const rgw_saloid_t& r);
  friend bool operator==(const rgw_saloid_t&, const rgw_saloid_t&);
  friend bool operator!=(const rgw_saloid_t&, const rgw_saloid_t&);
  friend bool operator>(const rgw_saloid_t& l, const rgw_saloid_t& r) {
    return cmp(l, r) > 0;
  }
  friend bool operator>=(const rgw_saloid_t& l, const rgw_saloid_t& r) {
    return cmp(l, r) >= 0;
  }
  friend bool operator<(const rgw_saloid_t& l, const rgw_saloid_t& r) {
    return cmp(l, r) < 0;
  }
  friend bool operator<=(const rgw_saloid_t& l, const rgw_saloid_t& r) {
    return cmp(l, r) <= 0;
  }

  const std::string& get_key() const {
    return name;  // TODO
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  size_t encoded_size() const;
};

WRITE_CLASS_ENCODER(rgw_saloid_t)
WRITE_EQ_OPERATORS_1(rgw_saloid_t, name)
std::ostream& operator<<(std::ostream& out, const rgw_saloid_t& o);
int cmp(const rgw_saloid_t& l, const rgw_saloid_t& r);

namespace std {
template <>
struct hash<rgw_saloid_t> {
  size_t operator()(const rgw_saloid_t& r) const {
    static hash<std::string> HO;
    size_t hash = HO(r.name);
    return hash;
  }
};
}  // namespace std

struct rgw_salcoll_t {
  std::string name;

  // TODO add from rgw_bucket constructor
  rgw_salcoll_t() : name("unknown") {
  }
  rgw_salcoll_t(const std::string& _name) : name(_name) {
  }

  int operator<(const rgw_salcoll_t& rhs) const {
    return name < rhs.name;
  }

  const std::string to_str() const {
    return name;
  }

  bool parse(const std::string& str);

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  size_t encoded_size() const;
};

WRITE_CLASS_ENCODER(rgw_salcoll_t)
WRITE_EQ_OPERATORS_1(rgw_salcoll_t, name)

std::ostream& operator<<(std::ostream& out, const rgw_salcoll_t& o);

namespace std {
template <>
struct hash<rgw_salcoll_t> {
  size_t operator()(const rgw_salcoll_t& r) const {
    static hash<std::string> HO;
    size_t hash = HO(r.name);
    return hash;
  }
};
}  // namespace std

struct rgw_filestore_perf_stat_t {
  // cur_op_latency is in ns since double add/sub are not associative
  uint64_t os_commit_latency_ns;
  uint64_t os_apply_latency_ns;

  rgw_filestore_perf_stat_t() : os_commit_latency_ns(0), os_apply_latency_ns(0) {
  }

  bool operator==(const rgw_filestore_perf_stat_t& r) const {
    return os_commit_latency_ns == r.os_commit_latency_ns &&
           os_apply_latency_ns == r.os_apply_latency_ns;
  }

  void add(const rgw_filestore_perf_stat_t& o) {
    os_commit_latency_ns += o.os_commit_latency_ns;
    os_apply_latency_ns += o.os_apply_latency_ns;
  }
  void sub(const rgw_filestore_perf_stat_t& o) {
    os_commit_latency_ns -= o.os_commit_latency_ns;
    os_apply_latency_ns -= o.os_apply_latency_ns;
  }
  void dump(ceph::Formatter* f) const;
  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static void generate_test_instances(std::list<rgw_filestore_perf_stat_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(rgw_filestore_perf_stat_t)

typedef std::map<std::string, std::string> osd_alert_list_t;
/// map osd id -> alert_list_t
typedef std::map<int, osd_alert_list_t> osd_alerts_t;
void dump(ceph::Formatter* f, const osd_alerts_t& alerts);

/** rgw_filestore_statfs_t
 * ObjectStore full statfs information
 */
struct rgw_filestore_statfs_t {
  uint64_t total = 0;                ///< Total bytes
  uint64_t available = 0;            ///< Free bytes available
  uint64_t internally_reserved = 0;  ///< Bytes reserved for internal purposes

  int64_t allocated = 0;  ///< Bytes allocated by the store

  int64_t data_stored = 0;      ///< Bytes actually stored by the user
  int64_t data_compressed = 0;  ///< Bytes stored after compression
  int64_t data_compressed_allocated =
      0;  ///< Bytes allocated for compressed data
  int64_t data_compressed_original = 0;  ///< Bytes that were compressed

  int64_t omap_allocated = 0;     ///< approx usage of omap data
  int64_t internal_metadata = 0;  ///< approx usage of internal metadata

  void reset() {
    *this = rgw_filestore_statfs_t();
  }
  void floor(int64_t f) {
#define FLOOR(x) \
  if (int64_t(x) < f) x = f
    FLOOR(total);
    FLOOR(available);
    FLOOR(internally_reserved);
    FLOOR(allocated);
    FLOOR(data_stored);
    FLOOR(data_compressed);
    FLOOR(data_compressed_allocated);
    FLOOR(data_compressed_original);

    FLOOR(omap_allocated);
    FLOOR(internal_metadata);
#undef FLOOR
  }

  bool operator==(const rgw_filestore_statfs_t& other) const;
  bool is_zero() const {
    return *this == rgw_filestore_statfs_t();
  }

  uint64_t get_used() const {
    return total - available - internally_reserved;
  }

  // this accumulates both actually used and statfs's internally_reserved
  uint64_t get_used_raw() const {
    return total - available;
  }

  float get_used_raw_ratio() const {
    if (total) {
      return (float)get_used_raw() / (float)total;
    } else {
      return 0.0;
    }
  }

  // helpers to ease legacy code porting
  uint64_t kb_avail() const {
    return available >> 10;
  }
  uint64_t kb() const {
    return total >> 10;
  }
  uint64_t kb_used() const {
    return (total - available - internally_reserved) >> 10;
  }
  uint64_t kb_used_raw() const {
    return get_used_raw() >> 10;
  }

  uint64_t kb_used_data() const {
    return allocated >> 10;
  }
  uint64_t kb_used_omap() const {
    return omap_allocated >> 10;
  }

  uint64_t kb_used_internal_metadata() const {
    return internal_metadata >> 10;
  }

  void add(const rgw_filestore_statfs_t& o) {
    total += o.total;
    available += o.available;
    internally_reserved += o.internally_reserved;
    allocated += o.allocated;
    data_stored += o.data_stored;
    data_compressed += o.data_compressed;
    data_compressed_allocated += o.data_compressed_allocated;
    data_compressed_original += o.data_compressed_original;
    omap_allocated += o.omap_allocated;
    internal_metadata += o.internal_metadata;
  }
  void sub(const rgw_filestore_statfs_t& o) {
    total -= o.total;
    available -= o.available;
    internally_reserved -= o.internally_reserved;
    allocated -= o.allocated;
    data_stored -= o.data_stored;
    data_compressed -= o.data_compressed;
    data_compressed_allocated -= o.data_compressed_allocated;
    data_compressed_original -= o.data_compressed_original;
    omap_allocated -= o.omap_allocated;
    internal_metadata -= o.internal_metadata;
  }
  void dump(ceph::Formatter* f) const;
  DENC(rgw_filestore_statfs_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.total, p);
    denc(v.available, p);
    denc(v.internally_reserved, p);
    denc(v.allocated, p);
    denc(v.data_stored, p);
    denc(v.data_compressed, p);
    denc(v.data_compressed_allocated, p);
    denc(v.data_compressed_original, p);
    denc(v.omap_allocated, p);
    denc(v.internal_metadata, p);
    DENC_FINISH(p);
  }
  static void generate_test_instances(std::list<rgw_filestore_statfs_t*>& o);
};
WRITE_CLASS_DENC(rgw_filestore_statfs_t)

std::ostream& operator<<(std::ostream& lhs, const rgw_filestore_statfs_t& rhs);

#endif  // __CEPH_RGW_STORE_SIMPLEFILE_FILESTORE_TYPES
