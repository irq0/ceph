// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "include/common_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include "rgw_common.h"
#include "common/async/backend_latency.h"
#include "common/perf_counters_cache.h"
#include "common/perf_counters_key.h"

extern PerfCounters *perfcounter;
extern int rgw_perf_start(CephContext *cct);
extern void rgw_perf_stop(CephContext *cct);

enum {
  l_rgw_first = 15000,
  l_rgw_req,
  l_rgw_failed_req,

  l_rgw_qlen,
  l_rgw_qactive,

  l_rgw_cache_hit,
  l_rgw_cache_miss,

  l_rgw_keystone_token_cache_hit,
  l_rgw_keystone_token_cache_miss,

  l_rgw_gc_retire,

  l_rgw_lc_expire_current,
  l_rgw_lc_expire_noncurrent,
  l_rgw_lc_expire_dm,
  l_rgw_lc_transition_current,
  l_rgw_lc_transition_noncurrent,
  l_rgw_lc_abort_mpu,

  l_rgw_pubsub_event_triggered,
  l_rgw_pubsub_event_lost,
  l_rgw_pubsub_store_ok,
  l_rgw_pubsub_store_fail,
  l_rgw_pubsub_events,
  l_rgw_pubsub_push_ok,
  l_rgw_pubsub_push_failed,
  l_rgw_pubsub_push_pending,
  l_rgw_pubsub_missing_conf,

  l_rgw_lua_current_vms,
  l_rgw_lua_script_ok,
  l_rgw_lua_script_fail,

  l_rgw_d4n_cache_hits,
  l_rgw_d4n_cache_misses,
  l_rgw_d4n_cache_evictions,

  l_rgw_kms_fetch_lat,
  l_rgw_kms_error_transient,
  l_rgw_kms_error_permanent,
  l_rgw_kms_error_secret_store,
  l_rgw_last,
};

enum {
  l_rgw_op_first = 16000,

  l_rgw_op_put_obj,
  l_rgw_op_put_obj_b,
  l_rgw_op_put_obj_lat,

  l_rgw_op_get_obj,
  l_rgw_op_get_obj_b,
  l_rgw_op_get_obj_lat,

  l_rgw_op_del_obj,
  l_rgw_op_del_obj_b,
  l_rgw_op_del_obj_lat,

  l_rgw_op_del_bucket,
  l_rgw_op_del_bucket_lat,

  l_rgw_op_copy_obj,
  l_rgw_op_copy_obj_b,
  l_rgw_op_copy_obj_lat,

  l_rgw_op_list_obj,
  l_rgw_op_list_obj_lat,

  l_rgw_op_list_buckets,
  l_rgw_op_list_buckets_lat,
  
  l_rgw_op_head_obj,
  l_rgw_op_head_obj_lat,
  
  l_rgw_op_last
};

enum {
  l_rgw_topic_first = 17000,

  l_rgw_persistent_topic_len,
  l_rgw_persistent_topic_size,

  l_rgw_topic_last
};

enum {
  l_rgw_lc_per_bucket_first = 18000,

  l_rgw_lc_per_bucket_start_time,
  l_rgw_lc_per_bucket_end_time,
  l_rgw_lc_per_bucket_obj_scanned,
  l_rgw_lc_per_bucket_obj_pending,
  l_rgw_lc_per_bucket_obj_expired,
  l_rgw_lc_per_bucket_obj_noncur_expired,
  l_rgw_lc_per_bucket_obj_dm_expired,
  l_rgw_lc_per_bucket_obj_transitioned,
  l_rgw_lc_per_bucket_obj_mpu_aborted,

  l_rgw_lc_per_bucket_last
};

enum {
  l_rgw_op_hist_first = 19000,
  l_rgw_op_hist_lat,
  // Of the request latency above, the share spent waiting on RADOS: wall time
  // with at least one RADOS op outstanding.  Bounded by l_rgw_op_hist_lat.
  l_rgw_op_hist_rados_lat,
  // Sum of the request's individual RADOS op durations.  RGW drives the data
  // path concurrently, so this is total RADOS work and may exceed the request
  // latency.  rados_work/rados_lat is the concurrency actually achieved.
  l_rgw_op_hist_rados_work,
  l_rgw_op_hist_rados_ops,
  l_rgw_op_hist_last,
};

enum {
  l_rgw_rados_pool_first = 19100,
  l_rgw_rados_pool_ops,
  l_rgw_rados_pool_lat,
  l_rgw_rados_pool_last,
};

namespace rgw::op_counters {

struct CountersContainer {
  std::shared_ptr<PerfCounters> user_counters;
  std::shared_ptr<PerfCounters> bucket_counters;
};

CountersContainer get(req_state *s);

void inc(const CountersContainer &counters, int idx, uint64_t v);

void tinc(const CountersContainer &counters, int idx, utime_t);

void tinc(const CountersContainer &counters, int idx, ceph::timespan amt);

} // namespace rgw::op_counters

namespace rgw::persistent_topic_counters {

class CountersManager {
  std::unique_ptr<PerfCounters> topic_counters;
  CephContext *cct;

public:
  CountersManager(const std::string& name, CephContext *cct);

  void set(int idx, uint64_t v);

  ~CountersManager();

};

} // namespace rgw::persistent_topic_counters

namespace rgw::lc_counters {

std::shared_ptr<PerfCounters> get(const std::string& bucket_name,
                                  const std::string& tenant);

} // namespace rgw::lc_counters

namespace rgw::op_hist {

PerfCounters* get(CephContext* cct, RGWOpType type, const char* op_name);
void htinc(PerfCounters* counters, int idx, ceph::timespan amt);

} // namespace rgw::op_hist

#ifdef WITH_RADOSGW_RADOS
namespace rgw::rados_pool_counters {

/// Record one completed RADOS operation against its pool's counters, which are
/// labeled with the numeric pool id and created on first use.  Answers "which
/// pool is slow" without needing any request context, so it covers RADOS
/// traffic that no request owns (GC, lifecycle, sync) as well as request
/// traffic.
///
/// Labeled by pool id rather than name so these line up directly with the
/// Objecter's own per-pool counters, which cannot resolve a name without
/// inverting its lock order.  Join ceph_pool_metadata for names.
void record(CephContext* cct, int64_t pool_id, ceph::timespan dur);

void shutdown(CephContext* cct);

/// Times one RADOS operation and, on completion, reports it to both the
/// request's backend_latency sink (if the yield carries one) and the per-pool
/// counters.
///
/// For blocking calls the destructor does the reporting.  Operations that
/// complete asynchronously must move this into the completion handler so that
/// it is destroyed there rather than at submission.
/// Holds the pool id by value rather than a reference to the IoCtx: on the
/// yielding path this outlives the OpFunc closure that supplied the IoCtx, so
/// anything borrowed from it would dangle by the time the operation completes.
class rados_op_timer {
 public:
  /// Defined out of line so that this header needs only the librados forward
  /// declaration.
  rados_op_timer(CephContext* cct, librados::IoCtx& ioctx, optional_yield y);

  rados_op_timer(rados_op_timer&& o) noexcept
    : cct(o.cct), pool_id(o.pool_id), sink(o.sink), start(o.start) {
    o.cct = nullptr; // marks the source as moved-from
    o.sink = nullptr;
  }
  rados_op_timer& operator =(rados_op_timer&&) = delete;
  rados_op_timer(const rados_op_timer&) = delete;
  rados_op_timer& operator =(const rados_op_timer&) = delete;

  ~rados_op_timer() {
    if (cct == nullptr) { // moved from
      return;
    }
    const auto dur = ceph::coarse_mono_clock::now() - start;
    if (sink != nullptr) {
      sink->op_end(dur);
    }
    rados_pool_counters::record(cct, pool_id, dur);
  }

 private:
  CephContext* cct;
  int64_t pool_id;
  ceph::async::backend_latency* sink;
  ceph::coarse_mono_time start = ceph::coarse_mono_clock::now();
};

} // namespace rgw::rados_pool_counters

#else // WITH_RADOSGW_RADOS

namespace rgw::rados_pool_counters {

// rgw_aio.cc is also compiled into non-RADOS backends. Keep its shared
// instrumentation call sites valid without pulling RADOS counters into them.
class rados_op_timer {
 public:
  rados_op_timer(CephContext*, librados::IoCtx&, optional_yield) {}
};

} // namespace rgw::rados_pool_counters

#endif // WITH_RADOSGW_RADOS
