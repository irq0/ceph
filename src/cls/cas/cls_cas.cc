// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*!
 * \file cls_cas.cc
 * \brief Content-addressed storage methods
 *
 * Methods to manage content-addressed objects. Used by
 * https://github.com/irq0/veintidos
 *
 * The class defines three operations: PUT, UP, DOWN
 * - PUT is a special write function that also sets metadata and initializes
 *     the object's reference counter. If the object already exists it ignores
 *     data and metadata passed and just increments the reference counter
 * - UP increments the object's reference counter
 * - DOWN decrements the object's reference counter
 *
 * Pinned objects: Not deleted when refcount hits zero. Set when
 *  refcount overflows a uint64.
 *
 */

#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "common/ceph_json.h"

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"
#include "cls/refcount/cls_refcount_ops.h"
#include "common/Clock.h"
#include <boost/archive/iterators/binary_from_base64.hpp>

#include "global/global_context.h"
#include "include/compat.h"

CLS_VER(0,1)
CLS_NAME(cas)

cls_handle_t h_class;
cls_method_handle_t h_cas_put;
cls_method_handle_t h_cas_up;
cls_method_handle_t h_cas_down;

//! Xattr for reference counter
#define CAS_REFCOUNT_ATTR "cas.refcount"

//! Xattr to mark pinned state of an object

//! Pinned objects aren't removed when their refcount hits 0
#define CAS_PINNED_ATTR "cas.pinned"

//! Prefix for metadata xattrs
#define CAS_METADATA_ATTR_PREFIX "cas.meta."


/*! \brief Get refcount for object
 *
 * \param refcount Out parameter for reference counter,
 *    set to 0 if no such object exists
 * \return 0 on success, error code of ceph op
 */

static int get_refcount(cls_method_context_t hctx, uint64_t *refcount)
{
  bufferlist bl;
  int ret = cls_cxx_getxattr(hctx, CAS_REFCOUNT_ATTR, &bl);

  if (ret == -ENODATA || ret == -ENOENT) {
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

/*! \brief Set refcount for object
 *
 * Set reference count for object. Overwrite if any previous value
 *
 * \param refcount New reference count
 * \return 0 on success, error code of ceph op
 */
static int set_refcount(cls_method_context_t hctx, uint64_t refcount)
{
  bufferlist bl;
  ::encode(refcount, bl);

  int ret = cls_cxx_setxattr(hctx, CAS_REFCOUNT_ATTR, &bl);
  if (ret < 0)
    return ret;

  return 0;
}


/*! \brief Pin an object
 *
 * A pinned object won't be deleted when it's refcount hits zero
 *
 * \return 0 on success, error code of Ceph op
 */
static int pin_object(cls_method_context_t hctx)
{
  bufferlist bl;
  ::encode(real_clock::now(), bl);

  int ret = cls_cxx_setxattr(hctx, CAS_PINNED_ATTR, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

/*! \brief Check if object is pinned
 *
 * Check if object is pinned. Also find out since when
 *
 * \param out_pinned Out parameter for pinned state
 * \param out_pinned_since Optional out parameter to return UNIX time stamp of
 *    when the object was pinned
 * \return 0 on success, error code of Ceph op otherwise
 */
static int object_pinned(cls_method_context_t hctx, bool *out_pinned, uint64_t *out_pinned_since=nullptr)
{
  bufferlist bl;
  int ret = cls_cxx_getxattr(hctx, CAS_PINNED_ATTR, &bl);

  if (ret == -ENODATA) {
    *out_pinned = false;
    return 0;
  } else if (ret < 0) {
    return ret;
  }

  uint64_t pinned_attr = 0;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(pinned_attr, iter);
    return 0;
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode pinned attr entry\n");
    return -EIO;
  }

  if (pinned_attr > 0) {
    *out_pinned = true;
    if (out_pinned_since != nullptr) {
      *out_pinned_since = pinned_attr;
    }
  }

  return 0;
}

/*! \brief Modify reference count
 *
 * Modify reference counter of an object and optionally return the new value
 *
 * \param delta Value to add to reference count
 * \param out_new_refcount Optional out parameter set to new reference count
 * \return 0 on success, error code of Ceph op otherwise
 */
static int mod_refcount(cls_method_context_t hctx, int64_t delta, uint64_t *out_new_refcount=nullptr)
{
  uint64_t cur_refcount = 0;
  int ret = -1;

  ret = get_refcount(hctx, &cur_refcount);
  if (ret < 0)
    return ret;

  uint64_t new_refcount = 0;
  if (cur_refcount + delta > std::numeric_limits<uint64_t>::max()) {
    CLS_LOG(1, "mod_refcount beyond uint64_t limit: pinning object");
    new_refcount = std::numeric_limits<uint64_t>::max();

    ret = pin_object(hctx);
    if (ret < 0)
      return ret;
  } else {
    new_refcount = cur_refcount + delta;
  }

  ret = set_refcount(hctx, new_refcount);
  if (ret < 0)
    return ret;

  if (out_new_refcount != nullptr)
    *out_new_refcount = new_refcount;

  CLS_LOG(10, "mod_refcount: %" PRIu64 " -> %" PRIu64, cur_refcount, new_refcount);
  return 0;
}

/*! \brief Save CAS metadata in object's xattrs
 *
 * Save CAS metadata in Xattrs. All key / value pairs must be strings. Stored under a
 * shared prefix
 *
 * \param metadata Map containing metadata key value pairs
 * \return 0 on success, error code of Ceph op otherwise
 */
static int set_cas_metadata(cls_method_context_t hctx, const map<string, string>& metadata)
{
  bufferlist bl;

  for (const auto& item : metadata) {
    bl.clear();
    ::encode(item.second, bl);

    string key(CAS_METADATA_ATTR_PREFIX);
    key += item.first;

    int ret = cls_cxx_setxattr(hctx, key.c_str(), &bl);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: failed set metadata attr_k=%s attr_v=%s", key.c_str(), bl.c_str());
      return ret;
    }
  }

  return 0;
}


/*! \brief Save new object
 *
 * Save data and metadata encoded in in. Initialize with reference count 1
 *
 * Expected JSON data in bufferlist:
 * {"meta": [{"key":..,"val":..},..],
 *  "data": "BASE64 encoded data"}
 *
 * \param in Encoded data and metadata
 * \return 0 on success, error code of failing Ceph op otherwise
 */
static int initialize_object(cls_method_context_t hctx, bufferlist *in)
{
  CLS_LOG(10, "NEW OBJ");
  CLS_LOG(25, "NEW OBJ: %s", in->c_str());

  int ret = -1;

  map<string, string> metadata;
  bufferlist data;

  try {
    JSONDecoder json_dec(*in);

    JSONDecoder::decode_json("meta", metadata, &json_dec.parser, true);
    JSONDecoder::decode_json("data", data, &json_dec.parser, true);
  } catch (const JSONDecoder::err& err) {
    CLS_LOG(1, "ERROR: failed to decode JSON entry: %s\n", err.message.c_str());
    return -EINVAL;
  }

  stringstream ss;
  data.hexdump(ss);
  CLS_LOG(25, "Data:\n %s", ss.str().c_str());

  ret = cls_cxx_write_full(hctx, &data);
  if (ret < 0)
    return ret;

  ret = set_refcount(hctx, 1);
  if (ret < 0)
    return ret;

  ret = set_cas_metadata(hctx, metadata);
  if (ret < 0)
    return ret;

  return 0;
}

/*! \brief Delete object, if not pinned
 *
 * Delete object, if not pinned.
 */
static int destroy_object(cls_method_context_t hctx)
{
  CLS_LOG(10, "DESTROY OBJ");

  int ret = -1;

  bool pinned = true;
  ret = object_pinned(hctx, &pinned);
  if (ret < 0)
    return ret;

  if (!pinned) {
    ret = cls_cxx_remove(hctx);
    if (ret < 0)
      return ret;
  } else {
    CLS_LOG(0, "Object pinned: Won't remove");
  }

  return 0;
}


/*! \brief CAS PUT
 *
 * CAS class put operation
 */
static int cls_cas_put(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "PUT");
  CLS_LOG(25, "PUT: %s", in->c_str());

  int ret = -1;
  uint64_t size;
  time_t mtime;

  int stat_ret = cls_cxx_stat(hctx, &size, &mtime);
  if (stat_ret == -ENOENT) {
    ret = initialize_object(hctx, in);
  } else if (stat_ret < 0) {
    ret = stat_ret;
  } else {
    ret = mod_refcount(hctx, +1);
  }

  return ret;
}

/*! \brief CAS UP
 *
 * CAS class put operation
 */
static int cls_cas_up(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "UP");
  CLS_LOG(25, "UP: %s", in->c_str());

  int ret = -1;
  uint64_t size;
  time_t mtime;

  int stat_ret = cls_cxx_stat(hctx, &size, &mtime);
  if (stat_ret == -ENOENT) {
    return -EINVAL;
  } else if (stat_ret < 0) {
    ret = stat_ret;
  } else {
    uint64_t new_refcount = 0;
    ret = mod_refcount(hctx, +1, &new_refcount);

    ::encode(new_refcount, *out);
  }

  return ret;
}

/*! \brief CAS DOWN
 *
 * CAS class put operation
 */
static int cls_cas_down(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "DOWN");
  CLS_LOG(25, "DOWN: %s", in->c_str());

  int ret = -1;
  uint64_t size;
  time_t mtime;

  int stat_ret = cls_cxx_stat(hctx, &size, &mtime);
  if (stat_ret == -ENOENT) {
    return -EINVAL;
  } else if (stat_ret < 0) {
    ret = stat_ret;
  } else {
    uint64_t new_refcount = 0;
    ret = mod_refcount(hctx, -1, &new_refcount);

    if (new_refcount <= 0)
      ret = destroy_object(hctx);

    ::encode(new_refcount, *out);
    return new_refcount;
  }

  return ret;
}

/*! \brief Init
 *
 * CAS class initialization
 */
void __cls_init()
{
  CLS_LOG(1, "Loaded CAS class!");

  cls_register("cas", &h_class);

  cls_register_cxx_method(h_class, "put", CLS_METHOD_RD | CLS_METHOD_WR, cls_cas_put, &h_cas_put);
  cls_register_cxx_method(h_class, "up", CLS_METHOD_RD | CLS_METHOD_WR, cls_cas_up, &h_cas_up);
  cls_register_cxx_method(h_class, "down", CLS_METHOD_RD | CLS_METHOD_WR, cls_cas_down, &h_cas_down);

  return;
}
