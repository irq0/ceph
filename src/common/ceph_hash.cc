
#include "include/types.h"

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/sha.h>
#include <cryptopp/md5.h>
#include <cryptopp/adler32.h>
#include <cryptopp/crc.h>
#include <cryptopp/integer.h>

/*
 * Robert Jenkin's hash function.
 * http://burtleburtle.net/bob/hash/evahash.html
 * This is in the public domain.
 */
#define mix(a, b, c)						\
	do {							\
		a = a - b;  a = a - c;  a = a ^ (c >> 13);	\
		b = b - c;  b = b - a;  b = b ^ (a << 8);	\
		c = c - a;  c = c - b;  c = c ^ (b >> 13);	\
		a = a - b;  a = a - c;  a = a ^ (c >> 12);	\
		b = b - c;  b = b - a;  b = b ^ (a << 16);	\
		c = c - a;  c = c - b;  c = c ^ (b >> 5);	\
		a = a - b;  a = a - c;  a = a ^ (c >> 3);	\
		b = b - c;  b = b - a;  b = b ^ (a << 10);	\
		c = c - a;  c = c - b;  c = c ^ (b >> 15);	\
	} while (0)

unsigned ceph_str_hash_rjenkins(const char *str, unsigned length)
{
	const unsigned char *k = (const unsigned char *)str;
	__u32 a, b, c;  /* the internal state */
	__u32 len;      /* how many key bytes still need mixing */

	/* Set up the internal state */
	len = length;
	a = 0x9e3779b9;      /* the golden ratio; an arbitrary value */
	b = a;
	c = 0;               /* variable initialization of internal state */

	/* handle most of the key */
	while (len >= 12) {
		a = a + (k[0] + ((__u32)k[1] << 8) + ((__u32)k[2] << 16) +
			 ((__u32)k[3] << 24));
		b = b + (k[4] + ((__u32)k[5] << 8) + ((__u32)k[6] << 16) +
			 ((__u32)k[7] << 24));
		c = c + (k[8] + ((__u32)k[9] << 8) + ((__u32)k[10] << 16) +
			 ((__u32)k[11] << 24));
		mix(a, b, c);
		k = k + 12;
		len = len - 12;
	}

	/* handle the last 11 bytes */
	c = c + length;
	switch (len) {            /* all the case statements fall through */
	case 11:
		c = c + ((__u32)k[10] << 24);
	case 10:
		c = c + ((__u32)k[9] << 16);
	case 9:
		c = c + ((__u32)k[8] << 8);
		/* the first byte of c is reserved for the length */
	case 8:
		b = b + ((__u32)k[7] << 24);
	case 7:
		b = b + ((__u32)k[6] << 16);
	case 6:
		b = b + ((__u32)k[5] << 8);
	case 5:
		b = b + k[4];
	case 4:
		a = a + ((__u32)k[3] << 24);
	case 3:
		a = a + ((__u32)k[2] << 16);
	case 2:
		a = a + ((__u32)k[1] << 8);
	case 1:
		a = a + k[0];
		/* case 0: nothing left to add */
	}
	mix(a, b, c);

	return c;
}

/*
 * linux dcache hash
 */
unsigned ceph_str_hash_linux(const char *str, unsigned length)
{
	unsigned long hash = 0;

	while (length--) {
		unsigned char c = *str++;
		hash = (hash + (c << 4) + (c >> 4)) * 11;
	}
	return hash;
}

unsigned ceph_str_hash_truncated_sha1(const char *str, unsigned length)
{
        CryptoPP::SHA1 hash;
        byte digest[CryptoPP::SHA1::DIGESTSIZE];
        hash.CalculateTruncatedDigest(digest, 4, (byte*)str, length);

        // cryptopp sha1 digest is big endian
        // compared the results to:
        // python -c 'import hashlib, struct; m = hashlib.sha1(); m.update("dinge"); print struct.unpack("<I", m.digest()[:4])[0]'
        // dinge -> 3917024803
        return *(uint32_t*)digest;
}

unsigned ceph_str_hash_truncated_md5(const char *str, unsigned length)
{
        CryptoPP::Weak1::MD5 hash;
        byte digest[CryptoPP::Weak1::MD5::DIGESTSIZE];
        hash.CalculateTruncatedDigest(digest, 4, (byte*)str, length);

        // cryptopp md5 is little endian.
        // compared the results to:
        // python -c 'import hashlib, struct; m = hashlib.md5(); m.update("dinge"); print struct.unpack("<I", m.digest()[:4])[0]'
        // dinge -> 461942412
        return *(uint32_t*)digest;
}


unsigned ceph_str_hash_adler32(const char *str, unsigned length)
{
        CryptoPP::Adler32 hash;
        byte digest[CryptoPP::Adler32::DIGESTSIZE];
        hash.CalculateDigest(digest, (byte*)str, length);

        // cryptopp crc32 seemes to be big endian
        // compared the results to: python -c 'import zlib; print zlib.adler32("dinge") & 0xffffffff'
        //  dinge -> 102367752
        return CryptoPP::Integer(digest, CryptoPP::Adler32::DIGESTSIZE).ConvertToLong();
}


unsigned ceph_str_hash_crc32(const char *str, unsigned length)
{
        CryptoPP::CRC32 hash;
        byte digest[CryptoPP::CRC32::DIGESTSIZE];
        hash.CalculateDigest(digest, (byte*)str, length);

        // cryptopp crc32 seemes to be little endian.
        // compared the results to: python -c 'import binascii; print binascii.crc32("dinge") & 0xffffffff'
        // dinge -> 3113422980
        return *(uint32_t*)digest;
}

unsigned ceph_str_hash(int type, const char *s, unsigned len)
{
	switch (type) {
	case CEPH_STR_HASH_LINUX:
		return ceph_str_hash_linux(s, len);
	case CEPH_STR_HASH_RJENKINS:
		return ceph_str_hash_rjenkins(s, len);
        case CEPH_STR_HASH_TRUNCATED_SHA1:
                return ceph_str_hash_truncated_sha1(s, len);
        case CEPH_STR_HASH_TRUNCATED_MD5:
                return ceph_str_hash_truncated_md5(s, len);
        case CEPH_STR_HASH_ADLER32:
                return ceph_str_hash_adler32(s, len);
        case CEPH_STR_HASH_CRC32:
                return ceph_str_hash_crc32(s, len);
	default:
		return -1;
	}
}

const char *ceph_str_hash_name(int type)
{
	switch (type) {
	case CEPH_STR_HASH_LINUX:
		return "linux";
	case CEPH_STR_HASH_RJENKINS:
		return "rjenkins";
        case CEPH_STR_HASH_TRUNCATED_SHA1:
                return "sha1-trunc";
        case CEPH_STR_HASH_TRUNCATED_MD5:
                return "md5-trunc";
        case CEPH_STR_HASH_ADLER32:
                return "adler32";
        case CEPH_STR_HASH_CRC32:
                return "crc32";
	default:
		return "unknown";
	}
}
