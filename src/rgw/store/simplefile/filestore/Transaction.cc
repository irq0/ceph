// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Transaction.h"

#include "common/Formatter.h"

using std::less;
using std::list;
using std::map;
using std::ostream;
using std::set;
using std::string;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

void decode_str_str_map_to_bl(bufferlist::const_iterator& p, bufferlist* out) {
  auto start = p;
  __u32 n;
  decode(n, p);
  unsigned len = 4;
  while (n--) {
    __u32 l;
    decode(l, p);
    p += l;
    len += 4 + l;
    decode(l, p);
    p += l;
    len += 4 + l;
  }
  start.copy(len, *out);
}

void decode_str_set_to_bl(bufferlist::const_iterator& p, bufferlist* out) {
  auto start = p;
  __u32 n;
  decode(n, p);
  unsigned len = 4;
  while (n--) {
    __u32 l;
    decode(l, p);
    p += l;
    len += 4 + l;
  }
  start.copy(len, *out);
}

namespace ceph::os {

void Transaction::dump(ceph::Formatter* f) {
  f->open_array_section("ops");
  iterator i = begin();
  int op_num = 0;
  bool stop_looping = false;
  while (i.have_op() && !stop_looping) {
    Transaction::Op* op = i.decode_op();
    f->open_object_section("op");
    f->dump_int("op_num", op_num);

    switch (op->op) {
      case Transaction::OP_NOP:
        f->dump_string("op_name", "nop");
        break;
      case Transaction::OP_CREATE: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        f->dump_string("op_name", "create");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
      } break;

      case Transaction::OP_TOUCH: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        f->dump_string("op_name", "touch");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
      } break;

      case Transaction::OP_WRITE: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        bufferlist bl;
        i.decode_bl(bl);
        f->dump_string("op_name", "write");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
        f->dump_unsigned("length", len);
        f->dump_unsigned("offset", off);
        f->dump_unsigned("bufferlist length", bl.length());
      } break;

      case Transaction::OP_ZERO: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        f->dump_string("op_name", "zero");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
        f->dump_unsigned("offset", off);
        f->dump_unsigned("length", len);
      } break;

      case Transaction::OP_TRIMCACHE: {
        // deprecated, no-op
        f->dump_string("op_name", "trim_cache");
      } break;

      case Transaction::OP_TRUNCATE: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        f->dump_string("op_name", "truncate");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
        f->dump_unsigned("offset", off);
      } break;

      case Transaction::OP_REMOVE: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        f->dump_string("op_name", "remove");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
      } break;

      case Transaction::OP_RMATTRS: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        f->dump_string("op_name", "rmattrs");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
      } break;

      case Transaction::OP_CLONE: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        rgw_saloid_t noid = i.get_oid(op->dest_oid);
        f->dump_string("op_name", "clone");
        f->dump_stream("collection") << cid;
        f->dump_stream("src_oid") << oid;
        f->dump_stream("dst_oid") << noid;
      } break;

      case Transaction::OP_CLONERANGE: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        rgw_saloid_t noid = i.get_oid(op->dest_oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        f->dump_string("op_name", "clonerange");
        f->dump_stream("collection") << cid;
        f->dump_stream("src_oid") << oid;
        f->dump_stream("dst_oid") << noid;
        f->dump_unsigned("offset", off);
        f->dump_unsigned("len", len);
      } break;

      case Transaction::OP_CLONERANGE2: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        rgw_saloid_t noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
        f->dump_string("op_name", "clonerange2");
        f->dump_stream("collection") << cid;
        f->dump_stream("src_oid") << oid;
        f->dump_stream("dst_oid") << noid;
        f->dump_unsigned("src_offset", srcoff);
        f->dump_unsigned("len", len);
        f->dump_unsigned("dst_offset", dstoff);
      } break;

      case Transaction::OP_MKCOLL: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        f->dump_string("op_name", "mkcoll");
        f->dump_stream("collection") << cid;
      } break;

      case Transaction::OP_COLL_HINT: {
        using ceph::decode;
        rgw_salcoll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint;
        f->dump_string("op_name", "coll_hint");
        f->dump_stream("collection") << cid;
        f->dump_unsigned("type", type);
        bufferlist hint;
        i.decode_bl(hint);
        auto hiter = hint.cbegin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          decode(pg_num, hiter);
          decode(num_objs, hiter);
          f->dump_unsigned("pg_num", pg_num);
          f->dump_unsigned("expected_num_objects", num_objs);
        }
      } break;

      case Transaction::OP_COLL_SET_BITS: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        f->dump_string("op_name", "coll_set_bits");
        f->dump_stream("collection") << cid;
        f->dump_unsigned("bits", op->split_bits);
      } break;

      case Transaction::OP_RMCOLL: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        f->dump_string("op_name", "rmcoll");
        f->dump_stream("collection") << cid;
      } break;

      case Transaction::OP_COLL_ADD: {
        rgw_salcoll_t ocid = i.get_cid(op->cid);
        rgw_salcoll_t ncid = i.get_cid(op->dest_cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        f->dump_string("op_name", "collection_add");
        f->dump_stream("src_collection") << ocid;
        f->dump_stream("dst_collection") << ncid;
        f->dump_stream("oid") << oid;
      } break;

      case Transaction::OP_COLL_REMOVE: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        f->dump_string("op_name", "collection_remove");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
      } break;

      case Transaction::OP_COLL_MOVE: {
        rgw_salcoll_t ocid = i.get_cid(op->cid);
        rgw_salcoll_t ncid = i.get_cid(op->dest_cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        f->open_object_section("collection_move");
        f->dump_stream("src_collection") << ocid;
        f->dump_stream("dst_collection") << ncid;
        f->dump_stream("oid") << oid;
        f->close_section();
      } break;

      case Transaction::OP_COLL_SETATTR: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
        f->dump_string("op_name", "collection_setattr");
        f->dump_stream("collection") << cid;
        f->dump_string("name", name);
        f->dump_unsigned("length", bl.length());
      } break;

      case Transaction::OP_COLL_RMATTR: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        string name = i.decode_string();
        f->dump_string("op_name", "collection_rmattr");
        f->dump_stream("collection") << cid;
        f->dump_string("name", name);
      } break;

      case Transaction::OP_COLL_RENAME: {
        f->dump_string("op_name", "collection_rename");
      } break;

      case Transaction::OP_SPLIT_COLLECTION: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        rgw_salcoll_t dest = i.get_cid(op->dest_cid);
        f->dump_string("op_name", "op_split_collection_create");
        f->dump_stream("collection") << cid;
        f->dump_stream("bits") << bits;
        f->dump_stream("rem") << rem;
        f->dump_stream("dest") << dest;
      } break;

      case Transaction::OP_SPLIT_COLLECTION2: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        rgw_salcoll_t dest = i.get_cid(op->dest_cid);
        f->dump_string("op_name", "op_split_collection");
        f->dump_stream("collection") << cid;
        f->dump_stream("bits") << bits;
        f->dump_stream("rem") << rem;
        f->dump_stream("dest") << dest;
      } break;

      case Transaction::OP_MERGE_COLLECTION: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        rgw_salcoll_t dest = i.get_cid(op->dest_cid);
        f->dump_string("op_name", "op_merge_collection");
        f->dump_stream("collection") << cid;
        f->dump_stream("dest") << dest;
        f->dump_stream("bits") << bits;
      } break;

      case Transaction::OP_COLL_MOVE_RENAME: {
        rgw_salcoll_t old_cid = i.get_cid(op->cid);
        rgw_saloid_t old_oid = i.get_oid(op->oid);
        rgw_salcoll_t new_cid = i.get_cid(op->dest_cid);
        rgw_saloid_t new_oid = i.get_oid(op->dest_oid);
        f->dump_string("op_name", "op_coll_move_rename");
        f->dump_stream("old_collection") << old_cid;
        f->dump_stream("old_oid") << old_oid;
        f->dump_stream("new_collection") << new_cid;
        f->dump_stream("new_oid") << new_oid;
      } break;

      case Transaction::OP_TRY_RENAME: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t old_oid = i.get_oid(op->oid);
        rgw_saloid_t new_oid = i.get_oid(op->dest_oid);
        f->dump_string("op_name", "op_coll_move_rename");
        f->dump_stream("collection") << cid;
        f->dump_stream("old_oid") << old_oid;
        f->dump_stream("new_oid") << new_oid;
      } break;

      case Transaction::OP_SETALLOCHINT: {
        rgw_salcoll_t cid = i.get_cid(op->cid);
        rgw_saloid_t oid = i.get_oid(op->oid);
        uint64_t expected_object_size = op->expected_object_size;
        uint64_t expected_write_size = op->expected_write_size;
        f->dump_string("op_name", "op_setallochint");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
        f->dump_stream("expected_object_size") << expected_object_size;
        f->dump_stream("expected_write_size") << expected_write_size;
      } break;

      default:
        f->dump_string("op_name", "unknown");
        f->dump_unsigned("op_code", op->op);
        stop_looping = true;
        break;
    }
    f->close_section();
    op_num++;
  }
  f->close_section();
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

void Transaction::generate_test_instances(list<Transaction*>& o) {
  o.push_back(new Transaction);

  Transaction* t = new Transaction;
  t->nop();
  o.push_back(t);

  t = new Transaction;
  rgw_salcoll_t c("1,2");
  rgw_salcoll_t c2("4,5");
  rgw_saloid_t o1("obj");
  rgw_saloid_t o2("obj2");
  rgw_saloid_t o3("obj3");
  t->touch(c, o1);
  bufferlist bl;
  bl.append("some data");
  t->write(c, o1, 1, bl.length(), bl);
  t->zero(c, o1, 22, 33);
  t->truncate(c, o1, 99);
  t->remove(c, o1);
  o.push_back(t);

  t = new Transaction;
  t->clone(c, o1, o2);
  t->clone(c, o1, o3);
  t->clone_range(c, o1, o2, 1, 12, 99);

  t->create_collection(c, 12);
  t->collection_move_rename(c, o2, c2, o3);
  t->remove_collection(c);
  o.push_back(t);
}

ostream& operator<<(ostream& out, const Transaction& tx) {
  return out << "Transaction(" << &tx << ")";
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

}  // namespace ceph::os
