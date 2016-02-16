#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>


#include <string>
#include <fstream>
#include <sstream>
#include <cassert>

#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_writer.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/config.h"

#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/OSDMap.h"
#include "crush/CrushWrapper.h"
#include "crush/CrushCompiler.h"
#include "crush/CrushTester.h"
#include "include/assert.h"
#include "include/ceph_hash.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_client

void usage()
{
  std::cout << "usage: hashsim ...\n";
  std::cout << "\n";
  std::cout << "Simulate object to pg mappings under different object hash functions\n";
  generic_client_usage();
  cout.flush();
}

/*
 * Since the conversions made in the new ceph_str_hash_.. functions are not
 * endian safe, compare some known values here.
 */
static void check_hashes()
{
  std::string s;

  s = "dinge";
  if (ceph_str_hash_truncated_sha1(s.c_str(), s.length()) != 3917024803) abort();
  if (ceph_str_hash_truncated_md5(s.c_str(), s.length()) != 461942412) abort();
  if (ceph_str_hash_adler32(s.c_str(), s.length()) != 102367752) abort();
  if (ceph_str_hash_crc32(s.c_str(), s.length()) != 3113422980) abort();
  if (ceph_str_hash_linux(s.c_str(), s.length()) != 285901154) abort();
  if (ceph_str_hash_rjenkins(s.c_str(), s.length()) != 1740849162) abort();

  s = "Ceph is a distributed object store and file system designed to provide excellent performance, reliability and scalability.";
  if (ceph_str_hash_truncated_sha1(s.c_str(), s.length()) != 2509992478) abort();
  if (ceph_str_hash_truncated_md5(s.c_str(), s.length()) != 174182118) abort();
  if (ceph_str_hash_adler32(s.c_str(), s.length()) != 3703909802) abort();
  if (ceph_str_hash_crc32(s.c_str(), s.length()) != 3875590023) abort();
  if (ceph_str_hash_linux(s.c_str(), s.length()) != 1002001631) abort();
  if (ceph_str_hash_rjenkins(s.c_str(), s.length()) != 3668998067) abort();
}

int sim(const OSDMap& osdmap, const ceph_file_layout& layout, std::string filename, uint64_t size)
{
  // pseudo cephfs stiping - don't do inodes. Pass the filename to the striper instead of an inode number
  string striper_format = boost::str(boost::format("%s.%%016llx") % filename);

  vector<ObjectExtent> extents;
  Striper::file_to_extents(g_ceph_context, striper_format.c_str(),
			   &layout, 0, size, 0, extents);

  object_locator_t oloc(0);

  json_spirit::Array out_oids;
  json_spirit::Array out_osds;
  vector<std::string> out_pgs;
  vector<int> out_primary_osds;

  // map extents to pgs and osds
  for (vector<ObjectExtent>::iterator it = extents.begin();
       it != extents.end();
       ++it) {

    ObjectExtent e = *it;

    pg_t pgid = osdmap.object_locator_to_pg(e.oid, oloc);

    vector<int> osds;
    int primary;
    osdmap.pg_to_acting_osds(pgid, &osds, &primary);

    {
      std::ostringstream os;
      os << e.oid;
      out_oids.push_back(json_spirit::Value(os.str()));
    }
    out_osds.push_back(json_spirit::Value(osds.begin(), osds.end()));

    {
      std::ostringstream os;
      os << pgid;
      out_pgs.push_back(os.str());
    }

    out_primary_osds.push_back(primary);
  }

  // json_spirit::Object j;

  // j.push_back(json_spirit::Pair("filename", json_spirit::Value(filename)));
  // j.push_back(json_spirit::Pair("size", json_spirit::Value(size)));
  // j.push_back(json_spirit::Pair("oids", json_spirit::Value(out_oids.begin(), out_oids.end())));
  // j.push_back(json_spirit::Pair("pgs", json_spirit::Value(out_pgs.begin(), out_pgs.end())));
  // j.push_back(json_spirit::Pair("osds", json_spirit::Value(out_osds.begin(), out_osds.end())));
  // j.push_back(json_spirit::Pair("primary_osds", json_spirit::Value(out_primary_osds.begin(), out_primary_osds.end())));

  json_spirit::Array j;

  j.push_back(json_spirit::Value(filename));
  j.push_back(json_spirit::Value(size));
  j.push_back(json_spirit::Value(out_oids.begin(), out_oids.end()));
  j.push_back(json_spirit::Value(out_pgs.begin(), out_pgs.end()));
  j.push_back(json_spirit::Value(out_osds.begin(), out_osds.end()));
  j.push_back(json_spirit::Value(out_primary_osds.begin(), out_primary_osds.end()));



  json_spirit::write(j, std::cout);
  std::cout << std::endl;

  return 0;
}


int main(int argc, const char **argv)
{
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  vector<const char*> def_args;

  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  check_hashes();

  std::string osdmap_filename("");
  std::string filename("");
  uint64_t size = 0;
  bool interactive_mode = false;

  int num_osds = -1;

  std::string val;
  std::ostringstream err;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_witharg(args, i, &filename, err, "--filename", (char*)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &osdmap_filename, err, "--osdmap", (char*)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &val, err, "--size", (char*)NULL)) {
      size = boost::lexical_cast<size_t>(val);
    } else if (ceph_argparse_witharg(args, i, &val, err, "--osds", (char*)NULL)) {
      num_osds = boost::lexical_cast<size_t>(val);
    } else if (ceph_argparse_flag(args, i, "-i", "--interactive", (char*)NULL)) {
      interactive_mode = true;
    } else {
      ++i;
    }
  }

  if ((num_osds < 0) && (osdmap_filename.empty())) {
    std::cerr << argv[0] << ": Bad num osds or no osdmap. Exiting" << std::endl;
    return 1;
  }

  uint32_t order = 22; // -> 4M objects
  uint32_t object_size = 1ull << order;
  uint32_t stripe_unit = object_size;
  uint32_t stripe_count = 1;

  uuid_d fsid;
  memset(&fsid, 0, sizeof(uuid_d));

  OSDMap osdmap;

  if (!osdmap_filename.empty()) {
    cerr << argv[0] << ": Loading osdmap from file" << std::endl;
    bufferlist bl;
    std::string error;
    if (bl.read_file(osdmap_filename.c_str(), &error)) {
      cerr << argv[0] << ": error reading .ceph_osdmap: " << error << std::endl;
      return 10;
    }

    try {
      osdmap.decode(bl);
    } catch (ceph::buffer::end_of_buffer &eob) {
      cerr << argv[0] << ": Exception (end_of_buffer) in decode(), exit." << std::endl;
      return 11;
    }
  } else if (num_osds > 0) {
    cerr << argv[0] << ": Using fresh osdmap" << std::endl;
    osdmap.build_simple(g_ceph_context, 0, fsid, num_osds,
			g_conf->osd_pg_bits, g_conf->osd_pgp_bits);
  }

  {
    pg_pool_t * pool = const_cast<pg_pool_t *>(osdmap.get_pg_pool(0));

    if(g_conf->osd_pool_default_flag_hashpsonlyprefix) {
      pool->set_flag(pg_pool_t::FLAG_HASHPSONLYPREFIX);
    } else {
      pool->unset_flag(pg_pool_t::FLAG_HASHPSONLYPREFIX);
    }

    pool->object_hash = g_conf->osd_pool_object_hash;
  }

  {
    int n = osdmap.get_max_osd();
    for (int i=0; i<n; i++) {
      osdmap.set_state(i, osdmap.get_state(i) | CEPH_OSD_UP);
      osdmap.set_weight(i, CEPH_OSD_IN);
      osdmap.crush->adjust_item_weightf(g_ceph_context, i, 1.0);
    }
  }

  cerr << argv[0] << ": Start OSDMap dump" << std::endl;

  JSONFormatter jsf(true);
  jsf.open_object_section("osdmap");
  osdmap.dump(&jsf);
  jsf.close_section();
  jsf.flush(cerr);
  cerr << argv[0] << ": End OSDMap dump" << std::endl;

  ceph_file_layout layout;
  memset(&layout, 0, sizeof(layout));
  layout.fl_stripe_unit = stripe_unit;
  layout.fl_stripe_count = stripe_count;
  layout.fl_object_size = object_size;
  layout.fl_pg_pool = 0;

  std::cerr << argv[0] << ": "
	    << " #pg: " << osdmap.get_pg_num(0)
	    << " #up_osds: " << osdmap.get_num_up_osds()
	    << " #in_osds: " << osdmap.get_num_in_osds()
	    << " max_osd: " << osdmap.get_max_osd()
	    << " #osds: " << osdmap.get_num_osds()
	    << std::endl;
  {
    const pg_pool_t * pool = osdmap.get_pg_pool(0);

    std::cerr << argv[0] << ": "
	      << " (pool0) prefix_hash: " << pool->has_flag(pg_pool_t::FLAG_HASHPSONLYPREFIX)
	      << " (pool0) hash algorithm: " << pool->get_object_hash() << " (" << ceph_str_hash_name(pool->get_object_hash()) << ") "
	      << " (conf) prefix_hash: " << g_conf->osd_pool_default_flag_hashpsonlyprefix
	      << " (conf) hash algorithm: " << g_conf->osd_pool_object_hash
	      << std::endl;

    if ((pool->get_object_hash() != g_conf->osd_pool_object_hash) ||
	(pool->has_flag(pg_pool_t::FLAG_HASHPSONLYPREFIX) != g_conf->osd_pool_default_flag_hashpsonlyprefix)) {
	  std::cerr << argv[0] << ": object hash and/or prefixhash setting not as configured. Exiting" << std::endl;
	  exit(20);
    }
  }

  if (interactive_mode) {
    cin.sync_with_stdio(false);

    for (std::string line; std::getline(std::cin, line);) {
      std::stringstream ss(line);

      std::string ssize;
      std::getline(ss, ssize, ' ');
      std::getline(ss, filename);

      try {
	size = boost::lexical_cast<size_t>(ssize);

	if (size == 0) {
	  std::cerr << argv[0] << ": Skipping record size=0: " << filename << std::endl;
	} else if (filename.empty()) {
	  std::cerr << argv[0] << ": Skipping empty filename" << filename << std::endl;
	} else {
	  sim(osdmap, layout, filename, size);
	}
      } catch (exception& e) {
	std::cerr << argv[0] << ": Skipping record exception: " << e.what() << std::endl;
      }
    }
  } else {
    if (filename.empty() || (size == 0)) {
      std::cerr << argv[0] << "Bad commandline" << std::endl;
      exit(1);
    }
    return sim(osdmap, layout, filename, size);
  }
}
