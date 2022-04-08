// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t; origami-fold-style: triple-braces -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include "rgw_sal_simplefile.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <sstream>
#include <system_error>

#include "cls/rgw/cls_rgw_client.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"
#include "rgw_bucket.h"
#include "rgw_lc.h"
#include "rgw_multi.h"
#include "rgw_rest_conn.h"
#include "rgw_sal.h"
#include "rgw_service.h"
#include "rgw_tracer.h"
#include "rgw_zone.h"
#include "services/svc_config_key.h"
#include "services/svc_quota.h"
#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

// Zone {{{

const RGWZoneGroup &SimpleFileZone::get_zonegroup() { return *zonegroup; }

int SimpleFileZone::get_zonegroup(const std::string &id, RGWZoneGroup &zg) {
  zg = *zonegroup;
  return 0;
}

const RGWZoneParams &SimpleFileZone::get_params() { return *zone_params; }

const rgw_zone_id &SimpleFileZone::get_id() { return cur_zone_id; }

const RGWRealm &SimpleFileZone::get_realm() { return *realm; }

const std::string &SimpleFileZone::get_name() const {
  return zone_params->get_name();
}

bool SimpleFileZone::is_writeable() { return true; }

bool SimpleFileZone::get_redirect_endpoint(std::string *endpoint) {
  return false;
}

bool SimpleFileZone::has_zonegroup_api(const std::string &api) const {
  return false;
}

const std::string &SimpleFileZone::get_current_period_id() {
  return current_period->get_id();
}

SimpleFileZone::SimpleFileZone(SimpleFileStore *_store) : store(_store) {
  realm = new RGWRealm();
  zonegroup = new RGWZoneGroup();
  zone_public_config = new RGWZone();
  zone_params = new RGWZoneParams();
  current_period = new RGWPeriod();
  cur_zone_id = rgw_zone_id(zone_params->get_id());
  RGWZonePlacementInfo info;
  RGWZoneStorageClasses sc;
  sc.set_storage_class("STANDARD", nullptr, nullptr);
  info.storage_classes = sc;
  zone_params->placement_pools["default"] = info;
}
// }}}

// Object {{{

SimpleFileObject::SimpleFileReadOp::SimpleFileReadOp(SimpleFileObject *_source,
                                                     RGWObjectCtx *_rctx)
    : source(_source), rctx(_rctx) {}

int SimpleFileObject::SimpleFileReadOp::prepare(optional_yield y,
                                                const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::SimpleFileReadOp::read(int64_t ofs, int64_t end,
                                             bufferlist &bl, optional_yield y,
                                             const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::SimpleFileReadOp::get_attr(const DoutPrefixProvider *dpp,
                                                 const char *name,
                                                 bufferlist &dest,
                                                 optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::SimpleFileReadOp::iterate(const DoutPrefixProvider *dpp,
                                                int64_t ofs, int64_t end,
                                                RGWGetDataCB *cb,
                                                optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

SimpleFileObject::SimpleFileDeleteOp::SimpleFileDeleteOp(
    SimpleFileObject *_source)
    : source(_source) {}

int SimpleFileObject::SimpleFileDeleteOp::delete_obj(
    const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_object(const DoutPrefixProvider *dpp,
                                    optional_yield y, bool prevent_versioning) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_obj_aio(const DoutPrefixProvider *dpp,
                                     RGWObjState *astate, Completions *aio,
                                     bool keep_index_consistent,
                                     optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::copy_object(
    User *user, req_info *info, const rgw_zone_id &source_zone,
    rgw::sal::Object *dest_object, rgw::sal::Bucket *dest_bucket,
    rgw::sal::Bucket *src_bucket, const rgw_placement_rule &dest_placement,
    ceph::real_time *src_mtime, ceph::real_time *mtime,
    const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
    bool high_precision_time, const char *if_match, const char *if_nomatch,
    AttrsMod attrs_mod, bool copy_if_newer, Attrs &attrs,
    RGWObjCategory category, uint64_t olh_epoch,
    boost::optional<ceph::real_time> delete_at, std::string *version_id,
    std::string *tag, std::string *etag, void (*progress_cb)(off_t, void *),
    void *progress_data, const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

/** TODO Create a randomized instance ID for this object */
void SimpleFileObject::gen_rand_obj_instance_name() {
  store.ceph_context();
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileObject::get_obj_attrs(optional_yield y,
                                    const DoutPrefixProvider *dpp,
                                    rgw_obj *target_obj) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileObject::modify_obj_attrs(const char *attr_name,
                                       bufferlist &attr_val, optional_yield y,
                                       const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_obj_attrs(const DoutPrefixProvider *dpp,
                                       const char *attr_name,
                                       optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

MPSerializer *SimpleFileObject::get_serializer(const DoutPrefixProvider *dpp,
                                               const std::string &lock_name) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

int SimpleFileObject::transition(Bucket *bucket,
                                 const rgw_placement_rule &placement_rule,
                                 const real_time &mtime, uint64_t olh_epoch,
                                 const DoutPrefixProvider *dpp,
                                 optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

bool SimpleFileObject::placement_rules_match(rgw_placement_rule &r1,
                                             rgw_placement_rule &r2) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}

int SimpleFileObject::dump_obj_layout(const DoutPrefixProvider *dpp,
                                      optional_yield y, Formatter *f) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::swift_versioning_restore(bool &restored, /* out */
                                               const DoutPrefixProvider *dpp) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::swift_versioning_copy(const DoutPrefixProvider *dpp,
                                            optional_yield y) {
  return -ENOTSUP;
}

int SimpleFileObject::omap_get_vals(const DoutPrefixProvider *dpp,
                                    const std::string &marker, uint64_t count,
                                    std::map<std::string, bufferlist> *m,
                                    bool *pmore, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileObject::omap_get_all(const DoutPrefixProvider *dpp,
                                   std::map<std::string, bufferlist> *m,
                                   optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
                                            const std::string &oid,
                                            const std::set<std::string> &keys,
                                            Attrs *vals) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileObject::omap_set_val_by_key(const DoutPrefixProvider *dpp,
                                          const std::string &key,
                                          bufferlist &val, bool must_exist,
                                          optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

// }}}

// User {{{

int SimpleFileUser::read_attrs(const DoutPrefixProvider *dpp,
                               optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  /** Read the User attributes from the backing Store */
  return -ENOTSUP;
}

int SimpleFileUser::merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                          Attrs &new_attrs, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  /** Set the attributes in attrs, leaving any other existing attrs set, and
   * write them to the backing store; a merge operation */
  return -ENOTSUP;
}

int SimpleFileUser::read_stats(const DoutPrefixProvider *dpp, optional_yield y,
                               RGWStorageStats *stats,
                               ceph::real_time *last_stats_sync,
                               ceph::real_time *last_stats_update) {
  /** Read the User stats from the backing Store, synchronous */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::read_stats_async(const DoutPrefixProvider *dpp,
                                     RGWGetUserStats_CB *cb) {
  /** Read the User stats from the backing Store, asynchronous */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::complete_flush_stats(const DoutPrefixProvider *dpp,
                                         optional_yield y) {
  /** Flush accumulated stat changes for this User to the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::read_usage(
    const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
    std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) {
  /** Read detailed usage stats for this User from the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::trim_usage(const DoutPrefixProvider *dpp,
                               uint64_t start_epoch, uint64_t end_epoch) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::load_user(const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO (0)" << dendl;
  return 0;
}

int SimpleFileUser::store_user(const DoutPrefixProvider *dpp, optional_yield y,
                               bool exclusive, RGWUserInfo *old_info) {
  /** Store this User to the backing store */ ldpp_dout(dpp, 10)
      << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::remove_user(const DoutPrefixProvider *dpp,
                                optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

static void populate_buckets_from_path(const SimpleFileStore& store, const DoutPrefixProvider *dpp, std::filesystem::path path, BucketList& buckets) {
  for (auto const &dir_entry : std::filesystem::directory_iterator{path}) {
    auto bucket =
        std::unique_ptr<Bucket>(new SimpleFileBucket{dir_entry.path(), store});
    bucket->load_bucket(dpp, null_yield);
    buckets.add(std::move(bucket));
  }
}

int SimpleFileUser::list_buckets(const DoutPrefixProvider *dpp,
                                 const std::string &marker,
                                 const std::string &end_marker, uint64_t max,
                                 bool need_stats, BucketList &buckets,
                                 optional_yield y) {
  // TODO this should list buckets assigned to a user. for now we just get every
  // bucket
  populate_buckets_from_path(store, dpp, store.buckets_path(), buckets);
  ldpp_dout(dpp, 10) << __func__ << ": TODO " << marker << ", " << end_marker
                     << ", "
                     << "max=" << max << ", "
                     << "buckets=" << buckets.get_buckets() << dendl;
  return 0;
}

int SimpleFileUser::create_bucket(
    const DoutPrefixProvider *dpp, const rgw_bucket &b,
    const std::string &zonegroup_id, rgw_placement_rule &placement_rule,
    std::string &swift_ver_location, const RGWQuotaInfo *pquota_info,
    const RGWAccessControlPolicy &policy, Attrs &attrs, RGWBucketInfo &info,
    obj_version &ep_objv, bool exclusive, bool obj_lock_enabled, bool *existed,
    req_info &req_info, std::unique_ptr<Bucket> *bucket, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

// }}}

// Bucket {{{

std::unique_ptr<Object> SimpleFileBucket::get_object(const rgw_obj_key &key) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  /** TODO Get an @a Object belonging to this bucket */
  return nullptr;
}

int SimpleFileBucket::list(const DoutPrefixProvider *dpp, ListParams &, int,
                           ListResults &results, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": iterating " << objects_path() << dendl;
  for (auto const &dir_entry :
       std::filesystem::directory_iterator{objects_path()}) {
    ldpp_dout(dpp, 10) << __func__ << ": adding object from " << dir_entry
                       << dendl;
    if (dir_entry.is_directory()) {
      JSONParser object_meta_parser;
      const auto object_meta_path =
          dir_entry.path() / "rgw_bucket_dir_entry.json";
      if (!object_meta_parser.parse(object_meta_path.c_str())) {
        ldpp_dout(dpp, 10) << "Failed to parse object metadata from "
                           << object_meta_path << ". Skipping" << dendl;
      }
      rgw_bucket_dir_entry rgw_dir;
      rgw_dir.decode_json(&object_meta_parser);
      results.objs.push_back(rgw_dir);
    }
  }

  ldpp_dout(dpp, 10) << __func__ << ": TODO " << dendl;
  return 0;
}

int SimpleFileBucket::remove_bucket(const DoutPrefixProvider *dpp,
                                    bool delete_children,
                                    bool forward_to_master, req_info *req_info,
                                    optional_yield y) {
  /** Remove this bucket from the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::remove_bucket_bypass_gc(int concurrent_max,
                                              bool keep_index_consistent,
                                              optional_yield y,
                                              const DoutPrefixProvider *dpp) {
  /** Remove this bucket, bypassing garbage collection.  May be removed */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::load_bucket(const DoutPrefixProvider *dpp,
                                  optional_yield y, bool get_stats) {
  std::filesystem::path meta_file_path =
      bucket_metadata_path("RGWBucketInfo.json");
  JSONParser bucket_meta_parser;
  if (!bucket_meta_parser.parse(meta_file_path.c_str())) {
    ldpp_dout(dpp, 10) << "Failed to parse bucket metadata from "
                       << meta_file_path << ". Returing EINVAL" << dendl;
    return -EINVAL;
  }

  info.decode_json(&bucket_meta_parser);
  ldpp_dout(dpp, 10) << __func__ << ": TODO " << meta_file_path << dendl;
  return 0;
}

int SimpleFileBucket::chown(const DoutPrefixProvider *dpp, User *new_user,
                            User *old_user, optional_yield y,
                            const std::string *marker) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
bool SimpleFileBucket::is_owner(User *user) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}
int SimpleFileBucket::check_empty(const DoutPrefixProvider *dpp,
                                  optional_yield y) {
  /** Check in the backing store if this bucket is empty */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                            Attrs &new_attrs,
                                            optional_yield y) {
  /** Set the attributes in attrs, leaving any other existing attrs set, and
   * write them to the backing store; a merge operation */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<MultipartUpload> SimpleFileBucket::get_multipart_upload(
    const std::string &oid, std::optional<std::string> upload_id,
    ACLOwner owner, ceph::real_time mtime) {
  /** Create a multipart upload in this bucket */
  return std::unique_ptr<MultipartUpload>();
}

int SimpleFileBucket::list_multiparts(
    const DoutPrefixProvider *dpp, const std::string &prefix,
    std::string &marker, const std::string &delim, const int &max_uploads,
    std::vector<std::unique_ptr<MultipartUpload>> &uploads,
    std::map<std::string, bool> *common_prefixes, bool *is_truncated) {
  /** List multipart uploads currently in this bucket */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::abort_multiparts(const DoutPrefixProvider *dpp,
                                       CephContext *cct) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

SimpleFileBucket::SimpleFileBucket(const std::filesystem::path& _path, const SimpleFileStore& _store) : store(_store), path(_path), acls() {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
}

// }}}

// Bucket: Boring Methods {{{

int SimpleFileBucket::try_refresh_info(const DoutPrefixProvider *dpp,
                                       ceph::real_time *pmtime) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::read_usage(
    const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
    std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::trim_usage(const DoutPrefixProvider *dpp,
                                 uint64_t start_epoch, uint64_t end_epoch) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::rebuild_index(const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::check_quota(const DoutPrefixProvider *dpp,
                                  RGWQuotaInfo &user_quota,
                                  RGWQuotaInfo &bucket_quota, uint64_t obj_size,
                                  optional_yield y, bool check_size_only) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::read_stats(
    const DoutPrefixProvider *dpp, int shard_id, std::string *bucket_ver,
    std::string *master_ver, std::map<RGWObjCategory, RGWStorageStats> &stats,
    std::string *max_marker, bool *syncstopped) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::read_stats_async(const DoutPrefixProvider *dpp,
                                       int shard_id,
                                       RGWGetBucketStats_CB *ctx) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::sync_user_stats(const DoutPrefixProvider *dpp,
                                      optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::update_container_stats(const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::check_bucket_shards(const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::put_info(const DoutPrefixProvider *dpp, bool exclusive,
                               ceph::real_time mtime) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

// }}}

// Store > User {{{

std::unique_ptr<User> SimpleFileStore::get_user(const rgw_user &u) {
  return std::make_unique<SimpleFileUser>(u, *this);
}
int SimpleFileStore::get_user_by_access_key(const DoutPrefixProvider *dpp,
                                            const std::string &key,
                                            optional_yield y,
                                            std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO (returning dummy user)" << dendl;
  user->reset(new SimpleFileUser(dummy_user, *this));
  return 0;
}

int SimpleFileStore::get_user_by_email(const DoutPrefixProvider *dpp,
                                       const std::string &email,
                                       optional_yield y,
                                       std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  user->reset(new SimpleFileUser(dummy_user, *this));
  return 0;
}

int SimpleFileStore::get_user_by_swift(const DoutPrefixProvider *dpp,
                                       const std::string &user_str,
                                       optional_yield y,
                                       std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

// }}}

// Store > Bucket {{{

int SimpleFileStore::set_buckets_enabled(const DoutPrefixProvider *dpp,
                                         std::vector<rgw_bucket> &buckets,
                                         bool enabled) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileStore::get_bucket(User *u, const RGWBucketInfo &i,
                                std::unique_ptr<Bucket> *bucket) {
  // TODO implement get_bucket by RGWBucketInfo
  ldout(ctx(), 10) << __func__ << ": TODO get_bucket by RGWBucketInfo" << dendl;
  return -ENOTSUP;
}

int SimpleFileStore::get_bucket(const DoutPrefixProvider *dpp, User *u,
                                const rgw_bucket &b,
                                std::unique_ptr<Bucket> *result,
                                optional_yield y) {
  const auto path = bucket_path(b);

  if (!std::filesystem::exists(path)) {
    ldpp_dout(dpp, 10) << __func__ << ": bucket "
                       << " path does not exist: " << path << dendl;
    return -ENOENT;
  }
  auto bucket = make_unique<SimpleFileBucket>(path, *this);
  const int ret = bucket->load_bucket(dpp, y);
  if (ret < 0) {
    return ret;
  }
  ldpp_dout(dpp, 10) << __func__ << ": bucket: " << bucket->get_name() << dendl;
  result->reset(bucket.release());
  return 0;
}

int SimpleFileStore::get_bucket(const DoutPrefixProvider *dpp, User *u,
                                const std::string &tenant,
                                const std::string &name,
                                std::unique_ptr<Bucket> *bucket,
                                optional_yield y) {
  // TODO implement get_bucket by name
  ldpp_dout(dpp, 10) << __func__ << ": TODO get_bucket by name" << dendl;
  return -ENOTSUP;
}

// }}}

// Lifecycle {{{
std::unique_ptr<Lifecycle> SimpleFileStore::get_lifecycle(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
RGWLC *SimpleFileStore::get_rgwlc(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

// }}}

// Store > Completions {{{
std::unique_ptr<Completions> SimpleFileStore::get_completions(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
// }}}

// Store > Notifications {{{
std::unique_ptr<Notification> SimpleFileStore::get_notification(
    rgw::sal::Object *obj, rgw::sal::Object *src_obj, struct req_state *s,
    rgw::notify::EventType event_type, const std::string *object_name) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

std::unique_ptr<Notification> SimpleFileStore::get_notification(
    const DoutPrefixProvider *dpp, rgw::sal::Object *obj,
    rgw::sal::Object *src_obj, rgw::notify::EventType event_type,
    rgw::sal::Bucket *_bucket, std::string &_user_id, std::string &_user_tenant,
    std::string &_req_id, optional_yield y) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

// }}}

// Store > Writer {{{
std::unique_ptr<Writer> SimpleFileStore::get_append_writer(
    const DoutPrefixProvider *dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user &owner,
    const rgw_placement_rule *ptail_placement_rule,
    const std::string &unique_tag, uint64_t position,
    uint64_t *cur_accounted_size) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
/** Get a Writer that atomically writes an entire object */
std::unique_ptr<Writer> SimpleFileStore::get_atomic_writer(
    const DoutPrefixProvider *dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user &owner,
    const rgw_placement_rule *ptail_placement_rule, uint64_t olh_epoch,
    const std::string &unique_tag) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

// }}}

// Store: Boring Methods {{{
std::unique_ptr<RGWOIDCProvider> SimpleFileStore::get_oidc_provider() {
  RGWOIDCProvider *p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int SimpleFileStore::forward_request_to_master(const DoutPrefixProvider *dpp,
                                               User *user, obj_version *objv,
                                               bufferlist &in_data,
                                               JSONParser *jp, req_info &info,
                                               optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::string SimpleFileStore::zone_unique_id(uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}
std::string SimpleFileStore::zone_unique_trans_id(const uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

int SimpleFileStore::cluster_stat(RGWClusterStat &stats) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

void SimpleFileStore::wakeup_meta_sync_shards(std::set<int> &shard_ids) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

void SimpleFileStore::wakeup_data_sync_shards(
    const DoutPrefixProvider *dpp, const rgw_zone_id &source_zone,
    std::map<int, std::set<std::string>> &shard_ids) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileStore::register_to_service_map(const DoutPrefixProvider *dpp,
                                             const string &daemon_type,
                                             const map<string, string> &meta) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

void SimpleFileStore::get_ratelimit(RGWRateLimitInfo &bucket_ratelimit,
                                    RGWRateLimitInfo &user_ratelimit,
                                    RGWRateLimitInfo &anon_ratelimit) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

void SimpleFileStore::get_quota(RGWQuotaInfo &bucket_quota,
                                RGWQuotaInfo &user_quota) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileStore::get_sync_policy_handler(
    const DoutPrefixProvider *dpp, std::optional<rgw_zone_id> zone,
    std::optional<rgw_bucket> bucket, RGWBucketSyncPolicyHandlerRef *phandler,
    optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

RGWDataSyncStatusManager *SimpleFileStore::get_data_sync_manager(
    const rgw_zone_id &source_zone) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::read_all_usage(
    const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry> &usage) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::trim_all_usage(const DoutPrefixProvider *dpp,
                                    uint64_t start_epoch, uint64_t end_epoch) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::get_config_key_val(string name, bufferlist *bl) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::meta_list_keys_init(const DoutPrefixProvider *dpp,
                                         const string &section,
                                         const string &marker, void **phandle) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::meta_list_keys_next(const DoutPrefixProvider *dpp,
                                         void *handle, int max,
                                         list<string> &keys, bool *truncated) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

void SimpleFileStore::meta_list_keys_complete(void *handle) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

std::string SimpleFileStore::meta_get_marker(void *handle) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

int SimpleFileStore::meta_remove(const DoutPrefixProvider *dpp,
                                 string &metadata_key, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

const RGWSyncModuleInstanceRef &SimpleFileStore::get_sync_module() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return sync_module;
}

std::string SimpleFileStore::get_host_id() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

std::unique_ptr<LuaScriptManager> SimpleFileStore::get_lua_script_manager() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return std::make_unique<UnsupportedLuaScriptManager>();
}

std::unique_ptr<RGWRole> SimpleFileStore::get_role(
    std::string name, std::string tenant, std::string path,
    std::string trust_policy, std::string max_session_duration_str,
    std::multimap<std::string, std::string> tags) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  RGWRole *p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> SimpleFileStore::get_role(std::string id) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  RGWRole *p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

int SimpleFileStore::get_roles(const DoutPrefixProvider *dpp, optional_yield y,
                               const std::string &path_prefix,
                               const std::string &tenant,
                               vector<std::unique_ptr<RGWRole>> &roles) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

// }}}

// Store > Logging {{{
int SimpleFileStore::log_usage(
    const DoutPrefixProvider *dpp,
    map<rgw_user_bucket, RGWUsageBatch> &usage_info) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::log_op(const DoutPrefixProvider *dpp, string &oid,
                            bufferlist &bl) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

// }}}

// Initialization {{{

void SimpleFileStore::finalize(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

SimpleFileStore::SimpleFileStore(CephContext *c,
                                 const std::filesystem::path &data_path)
    : dummy_user(), sync_module(), zone(this), data_path(data_path), cctx(c) {
  dummy_user.user_email = "simplefile@example.com";
  dummy_user.display_name = "Test User";
  dummy_user.max_buckets = 42;
  dummy_user.admin = 1;

  dummy_user.access_keys.insert({"test", RGWAccessKey("test", "test")});
  ldout(ctx(), 0) << "Simplefile store serving data from " << data_path
                  << dendl;
}

}  // namespace rgw::sal

extern "C" {
void *newSimpleFileStore(CephContext *cct) {
  rgw::sal::SimpleFileStore *store = new rgw::sal::SimpleFileStore(cct, "/tmp");
  return store;
}
}

// }}}
