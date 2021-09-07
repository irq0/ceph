#ifndef CEPH_CLS_CEPHFS_FSCRYPT_OPS_H
#define CEPH_CLS_CEPHFS_FSCRYPT_OPS_H

#include "include/types.h"
#include "common/hobject.h"
#include "common/Formatter.h"

static constexpr int AES_BLOCK_SIZE = 16;
static constexpr int AES_XTS_256_KEY_LENGTH = 2 * (256 / 8);
using fscrypt_iv_t = std::array<unsigned char, AES_BLOCK_SIZE>;
using fscrypt_key_t = std::array<unsigned char, AES_XTS_256_KEY_LENGTH>;

struct cls_cephfs_fscrypt_zero_op {
  uint64_t offset;
  uint64_t block_size;
  uint64_t zero_length;

  fscrypt_iv_t iv;
  fscrypt_key_t key;  // 2x256 bit AES XTS key

  cls_cephfs_fscrypt_zero_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(offset, bl);
    encode(block_size, bl);
    encode(zero_length, bl);
    encode(key, bl);
    encode(iv, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(offset, bl);
    decode(block_size, bl);
    decode(zero_length, bl);
    decode(key, bl);
    decode(iv, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_cephfs_fscrypt_zero_op)

struct cls_cephfs_fscrypt_write_op {
  uint64_t offset;
  uint64_t block_size;
  fscrypt_iv_t iv;
  fscrypt_key_t key;  // 2x256 bit AES XTS key
  bufferlist data;

  cls_cephfs_fscrypt_write_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(offset, bl);
    encode(block_size, bl);
    encode(key, bl);
    encode(iv, bl);
    encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(offset, bl);
    decode(block_size, bl);
    decode(key, bl);
    decode(iv, bl);
    decode(data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_cephfs_fscrypt_write_op)

struct cls_cephfs_fscrypt_read_op {
  uint64_t offset;
  uint64_t block_size;
  fscrypt_iv_t iv;
  fscrypt_key_t key;  // 2x256 bit AES XTS key

  cls_cephfs_fscrypt_read_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(offset, bl);
    encode(block_size, bl);
    encode(key, bl);
    encode(iv, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(offset, bl);
    decode(block_size, bl);
    decode(key, bl);
    decode(iv, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_cephfs_fscrypt_read_op)


#endif  // CEPH_CLS_CEPHFS_FSCRYPT_OPS_H
