// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "include/stringify.h"
#include "cls/cephfs_fscrypt/cls_cephfs_fscrypt_ops.h"

#include "include/utime.h"
#include "common/Clock.h"
#include "global/global_context.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include <errno.h>
#include <string>
#include <vector>

using namespace std;

/// creates a temporary pool and initializes an IoCtx for each test
class cls_cephfs_fscrypt : public ::testing::Test {
  librados::Rados rados;
  std::string pool_name;
 protected:
  librados::IoCtx ioctx;

  void SetUp() {
    pool_name = get_temp_pool_name();
    /* create pool */
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  void TearDown() {
    /* remove pool */
    ioctx.close();
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
  }
};

static librados::ObjectWriteOperation *new_write_op() {
  return new librados::ObjectWriteOperation();
}
static librados::ObjectReadOperation *new_read_op() {
  return new librados::ObjectReadOperation();
}

TEST_F(cls_cephfs_fscrypt, basic_write_read) {
  auto op = new_write_op();
  cls_cephfs_fscrypt_write_op write_op;
  write_op.offset = 0;
  write_op.block_size = 32;
  write_op.iv =
      {
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      };
  write_op.key =
      {
              23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42
      };
  write_op.data.append("0123456789abcdef0123456789abcdef");

  bufferlist bl;
  encode(write_op, bl);
  op->exec("cephfs_fscrypt", "write_block", bl);
  ASSERT_EQ(0, ioctx.operate("test", op));  // <---

  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("test", &size, &mtime));
  ASSERT_EQ(write_op.block_size, size);

  cls_cephfs_fscrypt_read_op read_args;
  read_args.offset = write_op.offset;
  read_args.block_size = write_op.block_size;
  read_args.iv = write_op.iv;
  read_args.key = write_op.key;
  bl.clear();
  encode(read_args, bl);

  auto read_op = new_read_op();
  bufferlist read_data;
  int prval;
  bufferlist foo;
  read_op->exec("cephfs_fscrypt", "read_block", bl, &read_data, &prval);
  ASSERT_EQ(0, ioctx.operate("test", read_op, &foo));

  ASSERT_EQ(write_op.data, read_data);
}

TEST_F(cls_cephfs_fscrypt, zero) {
  // write block "0123456789abcdef0123456789abcdef"
  auto write_op = new_write_op();
  cls_cephfs_fscrypt_write_op write_args;
  write_args.offset = 0;
  write_args.block_size = 32;
  write_args.iv =
      {
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      };
  write_args.key =
      {
              23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42
      };
  write_args.data.append("0123456789abcdef0123456789abcdef");

  bufferlist bl;
  encode(write_args, bl);
  write_op->exec("cephfs_fscrypt", "write_block", bl);
  ASSERT_EQ(0, ioctx.operate("zero", write_op));

  // zero out:
  // "0123456789abcdef0123456789abcdef"
  //            xxxxx
  //
  auto zero_op = new_write_op();
  cls_cephfs_fscrypt_zero_op zero_args;
  zero_args.offset = 10;
  zero_args.block_size = write_args.block_size;
  zero_args.zero_length = 5;
  zero_args.iv = write_args.iv;
  zero_args.key = write_args.key;

  bl.clear();
  encode(zero_args, bl);
  zero_op->exec("cephfs_fscrypt", "zero", bl);
  ASSERT_EQ(0, ioctx.operate("zero", zero_op));

  // read back
  cls_cephfs_fscrypt_read_op read_args;
  read_args.offset = write_args.offset;
  read_args.block_size = write_args.block_size;
  read_args.iv = write_args.iv;
  read_args.key = write_args.key;

  bl.clear();
  encode(read_args, bl);

  auto read_op = new_read_op();
  bufferlist read_data;
  int prval;
  bufferlist foo;
  read_op->exec("cephfs_fscrypt", "read_block", bl, &read_data, &prval);
  ASSERT_EQ(0, ioctx.operate("zero", read_op, &foo));

  bufferlist expected(write_args.data);
  expected.zero(zero_args.offset, zero_args.zero_length);
  ASSERT_EQ(expected, read_data);
}
