#ifndef FS_CEPH_HASH_H
#define FS_CEPH_HASH_H

#define CEPH_STR_HASH_LINUX          0x1 /* linux dcache hash */
#define CEPH_STR_HASH_RJENKINS       0x2 /* robert jenkins' */
#define CEPH_STR_HASH_TRUNCATED_SHA1 0x3 /* SHA1 truncated to 32 bit */
#define CEPH_STR_HASH_TRUNCATED_MD5  0x4 /* MD5 truncated to 32 bit */
#define CEPH_STR_HASH_ADLER32        0x5 /* Adler-32 checksum */
#define CEPH_STR_HASH_CRC32          0x6 /* CRC32 checksum */
#define CEPH_STR_HASH_CRC32C         0x7 /* CRC32C checksum */
#define CEPH_STR_HASH_XXHASH          0x8 /* xxHash checksum */

extern unsigned ceph_str_hash_linux(const char *s, unsigned len);
extern unsigned ceph_str_hash_rjenkins(const char *s, unsigned len);
extern unsigned ceph_str_hash_truncated_md5(const char *s, unsigned len);
extern unsigned ceph_str_hash_truncated_sha1(const char *s, unsigned len);
extern unsigned ceph_str_hash_adler32(const char *s, unsigned len);
extern unsigned ceph_str_hash_crc32(const char *s, unsigned len);
extern unsigned ceph_str_hash_crc32c(const char *s, unsigned len);
extern unsigned ceph_str_hash_xxhash(const char *s, unsigned len);

extern unsigned ceph_str_hash(int type, const char *s, unsigned len);
extern const char *ceph_str_hash_name(int type);

#endif
