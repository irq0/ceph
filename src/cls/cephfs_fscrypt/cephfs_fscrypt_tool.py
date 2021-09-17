#!/usr/bin/env python3

"""
commandline wrapper around the cephfs_fscrypt object class
"""

import sys
import struct
import binascii

import rados

# Example usage:
# python3 ../cephfs_fscrypt_tool.py \
#    ceph.conf \
#    100000005ea.00000000 \
#    00000000000000000000000000000000 ]
#    53e374ee40b4e0844433c792c93b1037478b184ca5fed5006be21b399a8d0eb9c41b6be8dedf0c4ef5d2dcb0f4fdf13e6bc4474ccf61c4d513a110de67117c87 \
#   0 4096

conf, obj, iv_hex, key_hex, offset, blocksize = sys.argv[1:]

iv = binascii.a2b_hex(iv_hex)
key = binascii.a2b_hex(key_hex)

cluster = rados.Rados(conffile=conf)
cluster.connect()
io = cluster.open_ioctx('cephfs.a.data')

print("stat", io.stat(obj))
print("enc  ", binascii.b2a_hex(io.read(obj, int(blocksize), 0)[:32]))

# poor mans ceph decode.h :)
args = struct.pack("<BBIQQ64s16s",
                   1,
                   1,
                   8 + 8 + len(key) + len(iv),
                   int(offset),
                   int(blocksize),
                   key,
                   iv)

size, data = io.execute(obj, "cephfs_fscrypt", "read_block", args)

print("data:", data[:32])
print("data:", binascii.b2a_hex(data[:32]))

io.close()
