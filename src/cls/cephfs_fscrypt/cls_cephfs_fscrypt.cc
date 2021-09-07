#include <errno.h>

#include <ctype.h>

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#include "objclass/objclass.h"

#include "include/buffer.h"
#include "cls_cephfs_fscrypt_ops.h"
#include "include/compat.h"
#include "osd/osd_types.h"
#include "include/intarith.h"
#include "common/hex.h"

using ceph::bufferlist;
using ceph::decode;

CLS_VER(1,0)
CLS_NAME(cephfs_fscrypt)

// does the key endianness make sense?

static int crypt_block(unsigned char *key, unsigned char *iv, bufferlist *src, bufferlist *dst, bool do_encrypt) {
  using evp_cipher_ctx_t = std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>;
  evp_cipher_ctx_t ctx(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
  if (!ctx) {
    CLS_LOG(0, "11");
    return -1;
  }

  int ret = EVP_CipherInit_ex(ctx.get(), EVP_aes_256_xts(), nullptr, key, iv, static_cast<int>(do_encrypt));

  if (ret != 1) {
    CLS_LOG(0, "12");
    return -1;
  }

  int out_len = 0;

  ret = EVP_CipherUpdate(ctx.get(), reinterpret_cast<unsigned char *>(dst->c_str()), &out_len,
                         reinterpret_cast<const unsigned char *>(src->c_str()), src->length());
  ceph_assert(static_cast<unsigned int>(out_len) == src->length());
  if (ret != 1) {
    CLS_LOG(0, "13");
    return -1;
  }
  return out_len;
}

static std::string hexstring(const char *p, int len) {
  std::ostringstream os;
  for (int i = 0; i < len; i++) {
    os << std::hex << std::setfill('0') << std::setw(2) << static_cast<unsigned int>(static_cast<uint8_t>(p[i])) << ' ';
  }
  os << " |";
  for (int i = 0; i < len; i++) {
    char c = p[i];
    if (std::isprint(c) && c != '\n') {
      os << c;
    } else {
      os << '.';
    }
  }
  os << "| ";
  os << "len=" << std::dec << len;
  return os.str();
}

static int read_and_decrypt_block(cls_method_context_t hctx, uint64_t offset, uint64_t block_size,
                                  unsigned char* key, unsigned char* iv,
                                  bufferlist *out_data) {
  // 1. read block
  bufferlist crypto_bl;
  crypto_bl.reserve(block_size);
  int read_ret = cls_cxx_read(hctx, offset, block_size, &crypto_bl);
  if (read_ret != static_cast<int>(block_size)) {
    CLS_LOG(0, "ERROR: failed to read block %d\n", read_ret);
    return -EINVAL;
  }
  CLS_LOG(0, "3242 block read: off=%lu bsize=%lu ret=%d\n", offset, block_size, read_ret);
  CLS_LOG(0, "2342 read crypto block: %s", hexstring(crypto_bl.c_str(), 32).c_str());

  // 2. decrypt block
  out_data->append_zero(block_size);
  int decrypt_ret =  crypt_block(key, iv, &crypto_bl, out_data, false);
  CLS_LOG(0, "2342 decrypted block: %s", hexstring(out_data->c_str(), 32).c_str());
  return decrypt_ret;
}

static int encrypt_and_write_block(cls_method_context_t hctx, uint64_t offset,
                                   uint64_t block_size, unsigned char *key,
                                   unsigned char *iv, bufferlist *in_data) {
  bufferlist bl;
  bl.append_zero(block_size);
  int encrypt_ret = crypt_block(key, iv, in_data, &bl, true);
  if (encrypt_ret < 0) {
    CLS_LOG(0, "encrypt failed");
    return encrypt_ret;
  }

  CLS_LOG(0, "2342 plain block: %s", hexstring(in_data->c_str(), 32).c_str());
  CLS_LOG(0, "2342 off=%lu bs=%lu bl_size=%u ret=%d", offset, block_size, bl.length(), encrypt_ret);
  CLS_LOG(0, "2342 crypto block: %s", hexstring(bl.c_str(), 32).c_str());

  return cls_cxx_write(hctx, offset, block_size, &bl);
}

/**
 * Read/partially zero/write fscrypt encrypted block Client sends over
 * everything needed for decryption (key(!), tweak) and modification
 * (offset, length)
 */
static int zero(cls_method_context_t hctx,
                bufferlist *in, bufferlist *out) {
  // 0. decode args
  auto in_iter = in->cbegin();
  cls_cephfs_fscrypt_zero_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: failed to decode args\n");
    return -EINVAL;
  }

  CLS_LOG(0, "3242 iv: %s", hexstring(reinterpret_cast<char*>(op.iv.data()),
                                      op.iv.max_size()).c_str());
  CLS_LOG(0, "3242 key: %s", hexstring(reinterpret_cast<char*>(op.key.data()),
                                       op.key.max_size()).c_str());
  bufferlist plain_bl;
  uint64_t block_offset = p2align(op.offset, op.block_size);
  uint64_t offset_in_block = p2phase(op.offset, op.block_size);
  unsigned char *key = reinterpret_cast<unsigned char*>(op.key.data());
  unsigned char *iv = reinterpret_cast<unsigned char*>(op.iv.data());

  CLS_LOG(0, "2342 ops: off=%lu blocksize=%lu zero_length=%lu block_offset=%lu off_in_block=%lu",
          op.offset, op.block_size, op.zero_length,
          block_offset, offset_in_block);
  int read_ret = read_and_decrypt_block(hctx, block_offset, op.block_size,
                                        key, iv, &plain_bl);
  if (read_ret < 0) {
    CLS_LOG(0, "ERROR: failed to read block %d\n", read_ret);
    return -EINVAL;
  } else if (read_ret == 0) {  // block doesn't exist - create
    plain_bl.append_zero(op.block_size);
  }
  CLS_LOG(0, "3242 block read: off=%lu boff=%lu off_in_block=%lu bsize=%lu zero_len=%lu ret=%d\n", op.offset, block_offset,
          offset_in_block, op.block_size, op.zero_length, read_ret);

  CLS_LOG(0, "2342 plain block: %s", hexstring(plain_bl.c_str(), 32).c_str());

  plain_bl.zero(offset_in_block, op.zero_length);
  return encrypt_and_write_block(hctx, block_offset, op.block_size, key, iv, &plain_bl);
}

static int write_block(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  auto in_iter = in->cbegin();
  cls_cephfs_fscrypt_write_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: failed to decode args\n");
    return -EINVAL;
  }

  CLS_LOG(0, "3242 iv: %s", hexstring(reinterpret_cast<char*>(op.iv.data()),
                                      op.iv.max_size()).c_str());
  CLS_LOG(0, "3242 key: %s", hexstring(reinterpret_cast<char*>(op.key.data()),
                                       op.key.max_size()).c_str());

  uint64_t block_offset = p2align(op.offset, op.block_size);
  unsigned char *key = reinterpret_cast<unsigned char*>(op.key.data());
  unsigned char *iv = reinterpret_cast<unsigned char*>(op.iv.data());
  return encrypt_and_write_block(hctx, block_offset, op.block_size, key, iv, &op.data);
}

static int read_block(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  auto in_iter = in->cbegin();
  cls_cephfs_fscrypt_read_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: failed to decode args\n");
    return -EINVAL;
  }

  CLS_LOG(0, "3242 iv: %s", hexstring(reinterpret_cast<char*>(op.iv.data()),
                                      op.iv.max_size()).c_str());
  CLS_LOG(0, "3242 key: %s", hexstring(reinterpret_cast<char*>(op.key.data()),
                                       op.key.max_size()).c_str());

  uint64_t block_offset = p2align(op.offset, op.block_size);
  unsigned char *key = reinterpret_cast<unsigned char*>(op.key.data());
  unsigned char *iv = reinterpret_cast<unsigned char*>(op.iv.data());
  return read_and_decrypt_block(hctx, block_offset, op.block_size, key, iv, out);
}

CLS_INIT(cephfs_fscrypt)
{
  CLS_LOG(1, "2342 Loaded cephfs_fscrypt class!");

  cls_handle_t h_class;
  cls_method_handle_t h_zero;
  cls_method_handle_t h_read_block;
  cls_method_handle_t h_write_block;

  cls_register("cephfs_fscrypt", &h_class);

  cls_register_cxx_method(h_class, "zero",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  zero,
			  &h_zero);

  cls_register_cxx_method(h_class, "read_block",
			  CLS_METHOD_RD,
			  read_block,
			  &h_read_block);

  cls_register_cxx_method(h_class, "write_block",
			  CLS_METHOD_WR,
			  write_block,
			  &h_write_block);
  return;
}
