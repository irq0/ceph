#pragma once
#include "include/rados.h"

namespace ceph::osdc::perf {

enum {
  l_osdop_first = 123400,
  l_osdop_ops,       // sub-ops issued: counted per sub-op, at submit
  l_osdop_compound,  // requests this op dominated that carried other sub-ops
  l_osdop_latency,   // request latency: recorded once per request, at completion
  l_osdop_last,
};

enum {
#define OSDOP_SLOT(op, opcode, str) osdop_slot_##op,
__CEPH_FORALL_OSD_OPS(OSDOP_SLOT)
#undef OSDOP_SLOT
  osdop_slot_count,
};

inline int osdop_slot_of(int op) {
  switch (op) {
#define OSDOP_CASE(o, opcode, str) case CEPH_OSD_OP_##o: return osdop_slot_##o;
__CEPH_FORALL_OSD_OPS(OSDOP_CASE)
#undef OSDOP_CASE
    default: return -1;
  }
}

/*
 * A request carries a vector of sub-ops but has exactly one duration, so
 * latency must be attributed to exactly one of them.  Rank the ops; the
 * highest-ranked one names the request.
 */
constexpr bool osdop_is_guard(int op) {
  switch (op) {
    case CEPH_OSD_OP_STAT:
    case CEPH_OSD_OP_ASSERT_VER:
    case CEPH_OSD_OP_GET_INTERNAL_VERSIONS:
    case CEPH_OSD_OP_CMPXATTR:
    case CEPH_OSD_OP_CMPEXT:
    case CEPH_OSD_OP_OMAP_CMP:
    case CEPH_OSD_OP_CREATE:
    case CEPH_OSD_OP_SETALLOCHINT:
      return true;
    default:
      return false;
  }
}

constexpr int osdop_rank(int op) {
  if (osdop_is_guard(op)) return 0;
  if (ceph_osd_op_type_exec(op)) return 6;
  if (ceph_osd_op_type_pg(op)) return 5; 
  if (ceph_osd_op_mode_cache(op)) return 4; 
  if (ceph_osd_op_type_data(op)) return ceph_osd_op_mode_modify(op) ? 3 : 2;
  if (ceph_osd_op_type_attr(op)) return 1;
  return 0;  // subops, unknown
}
} // namespace ceph::osdc::perf
