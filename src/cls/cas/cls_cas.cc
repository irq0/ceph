// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/**
 * /file
 * Content-addressed storage methods
 */

#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"
#include "cls/refcount/cls_refcount_ops.h"
#include "common/Clock.h"

#include "global/global_context.h"
#include "include/compat.h"

CLS_VER(0,1)
CLS_NAME(cas)

cls_handle_t h_class;
cls_method_handle_t h_cas_get;
cls_method_handle_t h_cas_put;


#define CAS_REFCOUNT_ATTR "cas.refcount"
#define CAS_METADATA_ATTR_PREFIX "cas.meta."

struct cls_cas_put_op {
  bufferlist data;
  string fp_type;

  cls_cas_put_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(fp_type, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(fp_type, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_cas_put_op)

static int get_refcount(cls_method_context_t hctx, uint64_t *refcount)
{
  bufferlist bl;
  int ret = cls_cxx_getxattr(hctx, CAS_REFCOUNT_ATTR, &bl);

  if (ret == -ENODATA) {
    *refcount = 0;
    return 0;
  } else if (ret < 0) {
    *refcount = 0;
    return ret;
  } else {
    try {
      bufferlist::iterator iter = bl.begin();
      ::decode(*refcount, iter);
    return 0;
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: failed to decode refcount entry\n");
      return -EIO;
    }
  }
}

static int set_refcount(cls_method_context_t hctx, uint64_t refcount)
{
  bufferlist bl;
  ::encode(refcount, bl);

  int ret = cls_cxx_setxattr(hctx, CAS_REFCOUNT_ATTR, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int mod_refcount(cls_method_context_t hctx, int64_t delta)
{
  uint64_t cur_refcount = 0;
  int ret = -1;

  ret = get_refcount(hctx, &cur_refcount);
  if (ret < 0)
    return ret;

  uint64_t new_refcount = cur_refcount + delta;
  ret = set_refcount(hctx, new_refcount);
  if (ret < 0)
    return ret;

  CLS_LOG(0, "mod_refcount: %" PRIu64 " -> %" PRIu64, cur_refcount, new_refcount);
  return 0;
}

static int set_cas_metadata(cls_method_context_t hctx, string fp_type)
{
  bufferlist bl;
  ::encode(fp_type, bl);

  int ret = cls_cxx_setxattr(hctx, CAS_METADATA_ATTR_PREFIX "fp_type", &bl);
  if (ret < 0)
    return ret;

  return 0;
}


static int cls_cas_put(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(0, "PUT");

  bufferlist::iterator in_iter = in->begin();

  cls_cas_put_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_cas_put(): failed to decode entry\n");
    return -EINVAL;
  }

  // TODO compress
  int ret = -1;
  ret = cls_cxx_write_full(hctx, &op.data);
  if (ret < 0)
    return ret;

  ret = mod_refcount(hctx, +1);
  if (ret < 0)
    return ret;

  ret = set_cas_metadata(hctx, op.fp_type);
  if (ret < 0)
    return ret;

  return 0;
}


static int cls_cas_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(0, "GET");

  uint64_t size;
  time_t mtime;

  int ret = -1;


  ret = cls_cxx_stat(hctx, &size, &mtime);
  if (ret < 0)
    return ret;

  ret = cls_cxx_read(hctx, 0, size, out);
  if (ret < 0)
    return ret;

  return 0;
}

void __cls_init()
{
  CLS_LOG(1, "Loaded CAS class!");

  cls_register("cas", &h_class);

  cls_register_cxx_method(h_class, "get", CLS_METHOD_RD | CLS_METHOD_WR, cls_cas_get, &h_cas_get);
  cls_register_cxx_method(h_class, "put", CLS_METHOD_RD | CLS_METHOD_WR, cls_cas_put, &h_cas_put);

  return;
}
