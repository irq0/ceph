// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t; origami-fold-style: triple-braces -*- vim: ts=8 sw=2 smarttab ft=cpp
#include "rgw_sal_simplefile.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <sstream>
#include <system_error>

#include "cls/rgw/cls_rgw_client.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "include/encoding.h"
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


static const std::string rgw_bucket_metadata_object_name("rgw_bucket_metadata");

// Zone {{{

ZoneGroup &SimpleFileZone::get_zonegroup() {
  return *zonegroup;
}

int SimpleFileZone::get_zonegroup(const std::string &id,
                                  std::unique_ptr<ZoneGroup> *zonegroup) {
  zonegroup->reset(new SimpleFileZoneGroup(store, std::make_unique<RGWZoneGroup>()));
  return 0;
}

const rgw_zone_id &SimpleFileZone::get_id() {
  return cur_zone_id;
}

const std::string &SimpleFileZone::get_name() const {
  return zone_params->get_name();
}

const RGWAccessKey &SimpleFileZone::get_system_key() {
  return zone_params->system_key;
}

const std::string &SimpleFileZone::get_realm_name() {
  return realm->get_name();
}

const std::string &SimpleFileZone::get_realm_id() {
  return realm->get_id();
}

bool SimpleFileZone::is_writeable() {
  return true;
}

bool SimpleFileZone::get_redirect_endpoint(std::string *endpoint) {
  return false;
}

bool SimpleFileZone::has_zonegroup_api(const std::string &api) const {
  return false;
}

const std::string &SimpleFileZone::get_current_period_id() {
  return current_period->get_id();
}

SimpleFileZone::SimpleFileZone(const SimpleFileStore &_store) : store(_store) {
  realm = new RGWRealm();
  zonegroup = new SimpleFileZoneGroup(_store, std::make_unique<RGWZoneGroup>());
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

// ZoneGroup {{{

const std::string &SimpleFileZoneGroup::get_endpoint() const {
  if (!group->endpoints.empty()) {
    return group->endpoints.front();
  } else {
    auto z = group->zones.find(group->master_zone);
    if (z != group->zones.end() && !z->second.endpoints.empty()) {
      return z->second.endpoints.front();
    }
  }
  return empty;
}

int SimpleFileZoneGroup::get_placement_target_names(
    std::set<std::string> &names) const {
  for (const auto &target : group->placement_targets) {
    names.emplace(target.second.name);
  }

  return 0;
}

SimpleFileZoneGroup::SimpleFileZoneGroup(const SimpleFileStore &_store,
                                         std::unique_ptr<RGWZoneGroup> _group)
    : store(_store), group(std::move(_group)) {
}

// }}}

// Object > read {{{

SimpleFileObject::SimpleFileReadOp::SimpleFileReadOp(SimpleFileObject *_source)
    : source(_source),
      ch(source->open_os_collection()),
      oid(source->get_os_oid()) {
}

int SimpleFileObject::SimpleFileReadOp::prepare(optional_yield y,
                                                const DoutPrefixProvider *dpp) {
  struct stat st;
  source->store.get_object_store()->stat(ch, oid, &st);

  source->set_key(source->get_key());
  // must set size, otherwise neither read / iterate is called
  source->set_obj_size(st.st_size);
  return 0;
}

int SimpleFileObject::SimpleFileReadOp::get_attr(const DoutPrefixProvider *dpp,
                                                 const char *name,
                                                 bufferlist &dest,
                                                 optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO: " << name << dendl;

  if (std::strcmp(name, "user.rgw.acl") == 0) {
    // TODO support 'user.rgw.acl' to support read_permissions. Return
    // empty policy since our test user is admin for now.
    RGWAccessControlPolicy policy;
    policy.encode(dest);
    return 0;
  }
  return -ENOTSUP;
}

// sync read
int SimpleFileObject::SimpleFileReadOp::read(int64_t ofs, int64_t end,
                                             bufferlist &bl, optional_yield y,
                                             const DoutPrefixProvider *dpp) {
  const size_t len = end + 1 - ofs;  // XXX add bounds check
  ldpp_dout(dpp, 10) << __func__ << ": offset=" << ofs << " end=" << end
                     << " len=" << len << dendl;
  return source->store.get_object_store()->read(ch, oid, ofs, len, bl);
}

// async read
int SimpleFileObject::SimpleFileReadOp::iterate(const DoutPrefixProvider *dpp,
                                                int64_t ofs, int64_t end,
                                                RGWGetDataCB *cb,
                                                optional_yield y) {
  const size_t len = end + 1 - ofs;  // XXX add bounds check
  bufferlist bl;
  ldpp_dout(dpp, 10) << __func__ << ": offset=" << ofs << " end=" << end
                     << " len=" << len << dendl;
  const int ret = source->store.get_object_store()->read(ch, oid, ofs, len, bl);

  if (ret < 0) {
    ldpp_dout(dpp, 10) << "Failed to read object from object store " << dendl;
    return ret;
  }
  cb->handle_data(bl, ofs, ret);
  return 0;
}

// }}}

// Object > Delete {{{

SimpleFileObject::SimpleFileDeleteOp::SimpleFileDeleteOp(
    SimpleFileObject *_source)
    : source(_source),
      cid(source->get_os_collection()),
      oid(source->get_os_oid()),
      os_transaction() {
}

int SimpleFileObject::SimpleFileDeleteOp::delete_obj(
    const DoutPrefixProvider *dpp, optional_yield y) {
  os_transaction.remove(cid, oid);
  const int ret = source->store.transact(source->get_bucket()->get_key(),
                                         std::move(os_transaction));
  ldpp_dout(dpp, 10) << __func__ << ": delete " << os_transaction
                     << " ret=" << ret << dendl;
  return ret;
}

int SimpleFileObject::delete_object(const DoutPrefixProvider *dpp,
                                    optional_yield y, bool prevent_versioning) {
  ::ObjectStore::Transaction t;
  t.remove(get_os_collection(), get_os_oid());
  const int ret = store.transact(get_bucket()->get_key(), std::move(t));
  ldpp_dout(dpp, 10) << __func__ << ": delete " << t << " ret=" << ret << dendl;
  return ret;
}

int SimpleFileObject::delete_obj_aio(const DoutPrefixProvider *dpp,
                                     RGWObjState *astate, Completions *aio,
                                     bool keep_index_consistent,
                                     optional_yield y) {
  return -ENOTSUP;
  //  return delete_object(dpp, y, false);
}

// }}}

// Object > misc actions {{{

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

/// }}}

// Object > state, attrs {{{

int SimpleFileObject::get_obj_state(const DoutPrefixProvider *dpp,
                                    RGWObjState **_state, optional_yield y,
                                    bool follow_olh) {
  auto ch = open_os_collection();
  auto oid = get_os_oid();
  struct stat st;

  int ret = store.get_object_store()->stat(ch, oid, &st);
  if (ret == -ENOENT) {
    state.exists = false;
  } else {
    state.size = st.st_size;
    state.accounted_size = st.st_size;
    state.exists = true;
    state.mtime = ceph::real_clock::from_timespec(st.st_mtim);
  }

  *_state = &state;
  ldpp_dout(dpp, 10) << __func__ << ": state from stat" << dendl;
  return 0;
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

// }}}

// Object > misc {{{

void SimpleFileObject::gen_rand_obj_instance_name() {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO noop" << dendl;
  return;
}

MPSerializer *SimpleFileObject::get_serializer(const DoutPrefixProvider *dpp,
                                               const std::string &lock_name) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO nullptr" << dendl;
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

int SimpleFileObject::transition_to_cloud(
    Bucket *bucket, rgw::sal::PlacementTier *tier, rgw_bucket_dir_entry &o,
    std::set<std::string> &cloud_targets, CephContext *cct, bool update_object,
    const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

bool SimpleFileObject::placement_rules_match(rgw_placement_rule &r1,
                                             rgw_placement_rule &r2) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO return true" << dendl;
  return true;
}

int SimpleFileObject::dump_obj_layout(const DoutPrefixProvider *dpp,
                                      optional_yield y, Formatter *f) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::swift_versioning_restore(bool &restored, /* out */
                                               const DoutPrefixProvider *dpp) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileObject::swift_versioning_copy(const DoutPrefixProvider *dpp,
                                            optional_yield y) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

// }}}

// Object > omap {{{

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

// Object > ObjectStore {{{

const rgw_salcoll_t &SimpleFileObject::get_os_collection() const {
  const SimpleFileBucket *bucket =
      static_cast<SimpleFileBucket *>(get_bucket());
  ceph_assert(bucket != nullptr);
  return bucket->get_os_collection();
}

rgw_saloid_t SimpleFileObject::get_os_oid() {
  return rgw_saloid_t(get_key());
}

::ObjectStore::CollectionHandle SimpleFileObject::open_os_collection() const {
  return store.get_object_store()->open_collection(get_os_collection());
}

// }}}

// User > attrs, stats, usage {{{

int SimpleFileUser::read_attrs(const DoutPrefixProvider *dpp,
                               optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                          Attrs &new_attrs, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::read_stats(const DoutPrefixProvider *dpp, optional_yield y,
                               RGWStorageStats *stats,
                               ceph::real_time *last_stats_sync,
                               ceph::real_time *last_stats_update) {
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

// }}}

// User > load, store, remove {{{

int SimpleFileUser::load_user(const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
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

// }}}

// User > Buckets (list, create) {{{

int SimpleFileUser::list_buckets(const DoutPrefixProvider *dpp,
                                 const std::string &marker,
                                 const std::string &end_marker, uint64_t max,
                                 bool need_stats, BucketList &buckets,
                                 optional_yield y) {
  std::vector<rgw_salcoll_t> collections;
  store.get_object_store()->list_collections(collections);
  for (const auto &collection : collections) {
    ldpp_dout(dpp, 10) << __func__ << ": found collection " << collection
                       << dendl;
    auto bucket =
	std::unique_ptr<Bucket>(new SimpleFileBucket{collection, store});
    bucket->load_bucket(dpp, y);
    buckets.add(std::move(bucket));
  }
  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " marker=" << marker << " end_marker=" << end_marker
                     << " max=" << max << " buckets=" << buckets.get_buckets()
                     << dendl;
  return 0;
}

int SimpleFileUser::create_bucket(
    const DoutPrefixProvider *dpp, const rgw_bucket &b,
    const std::string &zonegroup_id, rgw_placement_rule &placement_rule,
    std::string &swift_ver_location, const RGWQuotaInfo *pquota_info,
    const RGWAccessControlPolicy &policy, Attrs &attrs, RGWBucketInfo &info,
    obj_version &ep_objv, bool exclusive, bool obj_lock_enabled, bool *existed,
    req_info &req_info, std::unique_ptr<Bucket> *bucket_out, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " b=" << b << " info=" << info << dendl;
  rgw_salcoll_t cid(b);
  std::unique_ptr<Bucket> bucket =
      std::make_unique<SimpleFileBucket>(cid, store, b, nullptr);

  *existed = store.get_object_store()->collection_exists(cid);

  bucket->set_attrs(attrs);
  placement_rule.name = "default";
  placement_rule.storage_class = RGW_STORAGE_CLASS_STANDARD;

  info.placement_rule = placement_rule;
  info.creation_time = real_clock::now();
  info.bucket = b;
  info.owner = this->get_info().user_id;
  info.zonegroup = zonegroup_id;
  if (obj_lock_enabled) info.flags = BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
  bucket->set_version(ep_objv);
  bucket->get_info() = info;

  const rgw_saloid_t hoid(rgw_bucket_metadata_object_name);
  ::ObjectStore::Transaction t;

  if (!*existed) {
    t.create_collection(cid, 0);
  }

  // TODO store metadata somewhere
  // std::map<std::string, bufferlist> keys;
  // placement_rule.encode(keys["rgw_placement_rule"]);
  // if (pquota_info) {
  //   pquota_info->encode(keys["RGWQuotoInfo"]);
  // }
  // policy.encode(keys["RGWAccessControlPolicy"]);
  // ceph::encode(attrs, keys["Attrs"]);
  // info.encode(keys["RGWBucketInfo"]);

  t.create(cid, hoid);
  // t.omap_setkeys(cid, hoid, keys);

  const int ret = store.transact(b, std::move(t));
  ldpp_dout(dpp, 10) << "bucket created:"
                     << " cid=" << cid << " hoid=" << hoid << " info=" << info
		     << " ret=" << ret
                     << dendl;

  bucket_out->swap(bucket);
  return ret;
}

// }}}

// Bucket > ObjectStore {{{

::ObjectStore::CollectionHandle SimpleFileBucket::open_os_collection() const {
  return store.get_object_store()->open_collection(get_os_collection());
}

// }}}

// Bucket > get, list, remove, load {{{

std::unique_ptr<Object> SimpleFileBucket::get_object(const rgw_obj_key &key) {
  ldout(store.ceph_context(), 10) << __func__ << ": key=" << key << dendl;
  return std::make_unique<SimpleFileObject>(store, key, this);
}

int SimpleFileBucket::list(const DoutPrefixProvider *dpp, ListParams &, int,
                           ListResults &results, optional_yield y) {
  auto ch = open_os_collection();

  // TODO filter and "pagination" logic
  std::vector<rgw_saloid_t> objects;
  rgw_saloid_t next;

  int r = store.get_object_store()->collection_list(
      ch, rgw_saloid_t(), rgw_saloid_t::get_max(), 1000, &objects, &next);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "Failed to get bucket list: " << cpp_strerror(r)
                       << dendl;
    return r;
  }

  ldpp_dout(dpp, 10) << __func__ << ": iterating " << objects << dendl;
  for (const auto &hoid : objects) {
    struct stat st;
    store.get_object_store()->stat(ch, hoid, &st);

    // TODO create sensible dentry
    rgw_bucket_dir_entry dentry;
    dentry.key.name = hoid.name;
    dentry.key.instance = "foo";
    dentry.meta.owner = "meeee!";
    dentry.meta.accounted_size = st.st_size;
    dentry.meta.size = st.st_size;
    results.objs.push_back(dentry);
  }

  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " nresults=" << results.objs.size() << dendl;
  return 0;
}

int SimpleFileBucket::remove_bucket(const DoutPrefixProvider *dpp,
                                    bool delete_children,
                                    bool forward_to_master, req_info *req_info,
                                    optional_yield y) {
  // XXX this assumes we want to delete all children
  auto ch = open_os_collection();
  const auto cid = get_os_collection();
  ::ObjectStore::Transaction t;

  std::vector<rgw_saloid_t> objects;
  rgw_saloid_t next;
  int r = store.get_object_store()->collection_list(
      ch, rgw_saloid_t(), rgw_saloid_t::get_max(), 1000, &objects, &next);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "Failed to get bucket list: " << cpp_strerror(r)
                       << dendl;
    return r;
  }
  for (const auto &hoid : objects) {
    t.remove(cid, hoid);
  }
  t.remove_collection(get_os_collection());

  const int ret = store.transact(get_key(), std::move(t));
  ldpp_dout(dpp, 10) << __func__ << ": " << ret << dendl;
  return ret;
}

int SimpleFileBucket::remove_bucket_bypass_gc(int concurrent_max,
                                              bool keep_index_consistent,
                                              optional_yield y,
                                              const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": ->remove_bucket()" << dendl;
  return remove_bucket(dpp, true, true, nullptr, y);
}

int SimpleFileBucket::load_bucket(const DoutPrefixProvider *dpp,
                                  optional_yield y, bool get_stats) {
  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " bucket=" << info.bucket
                     << " coll=" << get_os_collection()
                     << " object=" << get_os_metadata_ghobject() << dendl;
  auto ch = store.get_object_store()->open_collection(get_os_collection());
  if (!ch) {
    return -ENOENT;
  }

  // TODO
  // std::map<std::string, bufferlist> omap_out;
  // const auto ret = store.get_object_store()->omap_get_values(
  //     ch, get_os_metadata_ghobject(), {"RGWBucketInfo"}, &omap_out);
  // if (ret < 0) {
  //   return ret;
  // } else {
  //   auto bl_iter = omap_out["RGWBucketInfo"].cbegin();
  //   info.decode(bl_iter);
  //   return 0;
  // }
  return 0;
}

int SimpleFileBucket::put_info(const DoutPrefixProvider *dpp, bool exclusive,
                               ceph::real_time mtime) {
  rgw_salcoll_t cid(info.bucket);

  ::ObjectStore::Transaction t;
  t.create_collection(cid, 0);

  // TODO
  // std::map<std::string, bufferlist> keys;
  // info.encode(keys["RGWBucketInfo"]);
  t.create(cid, rgw_saloid_t(rgw_bucket_metadata_object_name));
  // t.omap_setkeys(cid, rgw_bucket_metadata_ghobject_t(info.bucket), keys);

  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " transact=" << t << dendl;

  return store.transact(info.bucket, std::move(t));
}

int SimpleFileBucket::chown(const DoutPrefixProvider *dpp, User *new_user,
                            User *old_user, optional_yield y,
                            const std::string *marker) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

bool SimpleFileBucket::is_owner(User *user) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO return true" << dendl;
  return true;
}

int SimpleFileBucket::check_empty(const DoutPrefixProvider *dpp,
                                  optional_yield y) {
  bool empty;

  // XXX does not work! even without s3 objects there are metadata objects in the coll
  auto ch = open_os_collection();
  const int ret = store.get_object_store()->collection_empty(ch, &empty);
  ldpp_dout(dpp, 10) << __func__ << ": " << ret << " " << empty << dendl;
  if (ret < 0) {
    return ret;
  } else if (!empty) {
    // return -ENOTEMPTY;
    return 0;
  } else {
    return 0;
  }
}

// }}}

// Bucket > Multipart upload {{{

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

// }}}

// Bucket > Init {{{

SimpleFileBucket::SimpleFileBucket(const rgw_salcoll_t &_collection,
                                   const SimpleFileStore &_store)
    : store(_store),
      collection(_collection),
      acls(),
      metadata_ghobject(rgw_saloid_t(rgw_bucket_metadata_object_name)) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
}

SimpleFileBucket::SimpleFileBucket(const rgw_salcoll_t &_collection,
                                   const SimpleFileStore &_store,
                                   const RGWBucketInfo &_bucket, User *_user)
    : Bucket(_bucket, _user),
      store(_store),
      collection(_collection),
      acls(),
      metadata_ghobject(rgw_saloid_t(rgw_bucket_metadata_object_name)) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
}

SimpleFileBucket::SimpleFileBucket(const rgw_salcoll_t &_collection,
                                   const SimpleFileStore &_store,
                                   const rgw_bucket &_bucket, User *_user)
    : Bucket(_bucket, _user),
      store(_store),
      collection(_collection),
      acls(),
      metadata_ghobject(rgw_saloid_t(rgw_bucket_metadata_object_name)) {
  ldout(store.ceph_context(), 10) << __func__ << ": TODO" << dendl;
}

// }}}

// Bucket > Boring  {{{

int SimpleFileBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                            Attrs &new_attrs,
                                            optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

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
                                  RGWQuota &quota, uint64_t obj_size,
                                  optional_yield y, bool check_size_only) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileBucket::read_stats(
    const DoutPrefixProvider *dpp,
    const bucket_index_layout_generation &idx_layout, int shard_id,
    std::string *bucket_ver, std::string *master_ver,
    std::map<RGWObjCategory, RGWStorageStats> &stats, std::string *max_marker,
    bool *syncstopped) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::read_stats_async(
    const DoutPrefixProvider *dpp,
    const bucket_index_layout_generation &idx_layout, int shard_id,
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

// }}}

// Store > User {{{

std::unique_ptr<User> SimpleFileStore::get_user(const rgw_user &u) {
  return std::make_unique<SimpleFileUser>(u, *this);
}
int SimpleFileStore::get_user_by_access_key(const DoutPrefixProvider *dpp,
                                            const std::string &key,
                                            optional_yield y,
                                            std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return dummy user" << dendl;
  user->reset(new SimpleFileUser(dummy_user, *this));
  return 0;
}

int SimpleFileStore::get_user_by_email(const DoutPrefixProvider *dpp,
                                       const std::string &email,
                                       optional_yield y,
                                       std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return dummy user" << dendl;
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
                                std::unique_ptr<Bucket> *result) {
  ldout(ctx(), 10) << __func__ << ":"
                   << " RGWBucketInfo=" << i << dendl;

  std::unique_ptr<Bucket> bucket =
      make_unique<SimpleFileBucket>(rgw_salcoll_t(i.bucket), *this, i, u);
  result->swap(bucket);
  return 0;
}

int SimpleFileStore::get_bucket(const DoutPrefixProvider *dpp, User *u,
                                const rgw_bucket &b,
                                std::unique_ptr<Bucket> *result,
                                optional_yield y) {
  rgw_salcoll_t coll(b);
  std::unique_ptr<Bucket> bucket =
      make_unique<SimpleFileBucket>(coll, *this, b, u);
  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " bucket=" << bucket->get_key() << " requested=" << b
                     << " coll=" << coll << dendl;
  const int ret = bucket->load_bucket(dpp, y);
  if (ret != 0) {
    return ret;
  }
  result->swap(bucket);
  return 0;
}

int SimpleFileStore::get_bucket(const DoutPrefixProvider *dpp, User *u,
                                const std::string &tenant,
                                const std::string &name,
                                std::unique_ptr<Bucket> *result,
                                optional_yield y) {
  const rgw_bucket bucket(tenant, name, "");
  return get_bucket(dpp, u, bucket, result, y);
}

// }}}

// Store > Object {{{

std::unique_ptr<Object> SimpleFileStore::get_object(const rgw_obj_key &k) {
  ldout(ctx(), 10) << __func__ << ": obj_key=" << k << dendl;
  return std::make_unique<SimpleFileObject>(*this, k);
}

// }}}

// Lifecycle {{{
std::unique_ptr<Lifecycle> SimpleFileStore::get_lifecycle(void) {
  ldout(ctx(), 10) << __func__ << ": TODO nullptr" << dendl;
  return nullptr;
}
RGWLC *SimpleFileStore::get_rgwlc(void) {
  ldout(ctx(), 10) << __func__ << ": TODO nullptr" << dendl;
  return nullptr;
}

// }}}

// Store > Completions {{{
std::unique_ptr<Completions> SimpleFileStore::get_completions(void) {
  ldout(ctx(), 10) << __func__ << ": TODO nullptr" << dendl;
  return nullptr;
}
// }}}

// Store > Notifications {{{
std::unique_ptr<Notification> SimpleFileStore::get_notification(
    rgw::sal::Object *obj, rgw::sal::Object *src_obj, struct req_state *s,
    rgw::notify::EventType event_type, const std::string *object_name) {
  ldout(ctx(), 10) << __func__ << ": TODO dummy" << dendl;
  return std::make_unique<SimpleFileNotification>(obj, src_obj, event_type);
}

std::unique_ptr<Notification> SimpleFileStore::get_notification(
    const DoutPrefixProvider *dpp, rgw::sal::Object *obj,
    rgw::sal::Object *src_obj, rgw::notify::EventType event_type,
    rgw::sal::Bucket *_bucket, std::string &_user_id, std::string &_user_tenant,
    std::string &_req_id, optional_yield y) {
  ldout(ctx(), 10) << __func__ << ": TODO dummy" << dendl;
  return std::make_unique<SimpleFileNotification>(obj, src_obj, event_type);
}

// }}}

// Store > Writer {{{
std::unique_ptr<Writer> SimpleFileStore::get_append_writer(
    const DoutPrefixProvider *dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user &owner,
    const rgw_placement_rule *ptail_placement_rule,
    const std::string &unique_tag, uint64_t position,
    uint64_t *cur_accounted_size) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO nullptr" << dendl;
  return nullptr;
}
/** Get a Writer that atomically writes an entire object */
std::unique_ptr<Writer> SimpleFileStore::get_atomic_writer(
    const DoutPrefixProvider *dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user &owner,
    const rgw_placement_rule *ptail_placement_rule, uint64_t olh_epoch,
    const std::string &unique_tag) {
  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " owner=" << owner << " unique_tag=" << unique_tag
                     << " head_obj_key=" << _head_obj->get_key()
                     << " head_obj_bucket=" << _head_obj->get_bucket() << dendl;

  std::unique_ptr<SimpleFileObject> head_obj;
  head_obj.reset(static_cast<SimpleFileObject *>(_head_obj.release()));

  return std::make_unique<SimpleFileAtomicWriter>(dpp, y, std::move(head_obj),
                                                  *this);
}

// }}}

// Writer > Atomic {{{

SimpleFileAtomicWriter::SimpleFileAtomicWriter(
    const DoutPrefixProvider *dpp, optional_yield y,
    std::unique_ptr<SimpleFileObject> _head_obj, const SimpleFileStore &_store)
    : Writer(dpp, y),
      store(_store),
      cid(_head_obj->get_os_collection()),
      oid(_head_obj->get_os_oid()),
      head_obj(std::move(_head_obj)),
      os_transaction() {
  ldpp_dout(dpp, 10) << __func__ << ":"
                     << " cid=" << cid << " oid=" << oid << dendl;
}

int SimpleFileAtomicWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": truncate(0)" << dendl;
  os_transaction.truncate(cid, oid, 0);
  return 0;
}

int SimpleFileAtomicWriter::process(bufferlist &&data, uint64_t offset) {
  ldpp_dout(dpp, 10) << __func__ << ": write"
                     << " data=" << data << " off=" << offset << dendl;
  os_transaction.write(cid, oid, offset, data.length(), data);
  return 0;
}

int SimpleFileAtomicWriter::complete(
    size_t accounted_size, const std::string &etag, ceph::real_time *mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist> &attrs,
    ceph::real_time delete_at, const char *if_match, const char *if_nomatch,
    const std::string *user_data, rgw_zone_set *zones_trace, bool *canceled,
    optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": transact" << os_transaction << dendl;
  return store.transact(head_obj->get_bucket()->get_key(),
                        std::move(os_transaction));
}

// }}}

// Store > Boring Methods {{{
std::unique_ptr<RGWOIDCProvider> SimpleFileStore::get_oidc_provider() {
  RGWOIDCProvider *p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int SimpleFileStore::forward_request_to_master(const DoutPrefixProvider *dpp,
                                               User *user, obj_version *objv,
                                               bufferlist &in_data,
                                               JSONParser *jp, req_info &info,
                                               optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileStore::forward_iam_request_to_master(
    const DoutPrefixProvider *dpp, const RGWAccessKey &key, obj_version *objv,
    bufferlist &in_data, RGWXMLDecoder::XMLParser *parser, req_info &info,
    optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::string SimpleFileStore::zone_unique_id(uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO return empty" << dendl;
  return "";
}
std::string SimpleFileStore::zone_unique_trans_id(const uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO return empty" << dendl;
  return "";
}

int SimpleFileStore::cluster_stat(RGWClusterStat &stats) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

void SimpleFileStore::wakeup_meta_sync_shards(std::set<int> &shard_ids) {
  ldout(ctx(), 10) << __func__ << ": TODO noop" << dendl;
  return;
}

void SimpleFileStore::wakeup_data_sync_shards(
    const DoutPrefixProvider *dpp, const rgw_zone_id &source_zone,
    boost::container::flat_map<
        int, boost::container::flat_set<rgw_data_notify_entry>> &shard_ids) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO noop" << dendl;
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

void SimpleFileStore::get_quota(RGWQuota &quota) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileStore::get_sync_policy_handler(
    const DoutPrefixProvider *dpp, std::optional<rgw_zone_id> zone,
    std::optional<rgw_bucket> bucket, RGWBucketSyncPolicyHandlerRef *phandler,
    optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

RGWDataSyncStatusManager *SimpleFileStore::get_data_sync_manager(
    const rgw_zone_id &source_zone) {
  ldout(ctx(), 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileStore::read_all_usage(
    const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry> &usage) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileStore::trim_all_usage(const DoutPrefixProvider *dpp,
                                    uint64_t start_epoch, uint64_t end_epoch) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileStore::get_config_key_val(string name, bufferlist *bl) {
  ldout(ctx(), 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileStore::meta_list_keys_init(const DoutPrefixProvider *dpp,
                                         const string &section,
                                         const string &marker, void **phandle) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileStore::meta_list_keys_next(const DoutPrefixProvider *dpp,
                                         void *handle, int max,
                                         list<string> &keys, bool *truncated) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

void SimpleFileStore::meta_list_keys_complete(void *handle) {
  ldout(ctx(), 10) << __func__ << ": TODO noop" << dendl;
  return;
}

std::string SimpleFileStore::meta_get_marker(void *handle) {
  ldout(ctx(), 10) << __func__ << ": TODO return empty" << dendl;
  return "";
}

int SimpleFileStore::meta_remove(const DoutPrefixProvider *dpp,
                                 string &metadata_key, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

const RGWSyncModuleInstanceRef &SimpleFileStore::get_sync_module() {
  ldout(ctx(), 10) << __func__ << ":" << dendl;
  return sync_module;
}

std::string SimpleFileStore::get_host_id() {
  ldout(ctx(), 10) << __func__ << ": TODO return empty" << dendl;
  return "";
}

std::unique_ptr<LuaScriptManager> SimpleFileStore::get_lua_script_manager() {
  ldout(ctx(), 10) << __func__ << ": TODO dummy" << dendl;
  return std::make_unique<UnsupportedLuaScriptManager>();
}

std::unique_ptr<RGWRole> SimpleFileStore::get_role(
    std::string name, std::string tenant, std::string path,
    std::string trust_policy, std::string max_session_duration_str,
    std::multimap<std::string, std::string> tags) {
  ldout(ctx(), 10) << __func__ << ": TODO dummy" << dendl;
  RGWRole *p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> SimpleFileStore::get_role(std::string id) {
  ldout(ctx(), 10) << __func__ << ": TODO dummy" << dendl;
  RGWRole *p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}
std::unique_ptr<RGWRole> SimpleFileStore::get_role(const RGWRoleInfo &info) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  RGWRole *p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

int SimpleFileStore::get_roles(const DoutPrefixProvider *dpp, optional_yield y,
                               const std::string &path_prefix,
                               const std::string &tenant,
                               vector<std::unique_ptr<RGWRole>> &roles) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

const std::string &SimpleFileStore::get_compression_type(
    const rgw_placement_rule &rule) {
  static std::string no_compression;
  return no_compression;
}
bool SimpleFileStore::valid_placement(const rgw_placement_rule &rule) {
  return true;
}

// }}}

// Store > Logging {{{

int SimpleFileStore::log_usage(
    const DoutPrefixProvider *dpp,
    map<rgw_user_bucket, RGWUsageBatch> &usage_info) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

int SimpleFileStore::log_op(const DoutPrefixProvider *dpp, string &oid,
                            bufferlist &bl) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO return 0" << dendl;
  return 0;
}

// }}}

// Store > ObjectStore Interaction {{{

int SimpleFileStore::transact(const rgw_bucket &bucket,
                              ::ObjectStore::Transaction &&t) const {
  auto ch = object_store->create_new_collection(rgw_salcoll_t(bucket));
  const int ret = object_store->queue_transaction(ch, std::move(t));
  if (ret) {
    ldout(ceph_context(), 0) << "transaction error:" << cpp_strerror(ret)
                             << " bucket=" << bucket << " ch=" << ch << dendl;
  }
  ch->flush();
  return ret;
}

// }}}

// Initialization {{{

int SimpleFileStore::initialize(CephContext *cct,
                                const DoutPrefixProvider *dpp) {
  ldout(cct, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

void SimpleFileStore::finalize(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

SimpleFileStore::SimpleFileStore(CephContext *c,
                                 std::unique_ptr<::ObjectStore> _object_store)
    : dummy_user(),
      sync_module(),
      zone(*this),
      cctx(c),
      object_store(std::move(_object_store)) {
  dummy_user.user_email = "simplefile@example.com";
  dummy_user.display_name = "Test User";
  dummy_user.max_buckets = 42;
  dummy_user.admin = 1;
  dummy_user.access_keys.insert({"test", RGWAccessKey("test", "test")});

  ldout(ctx(), 0) << "🖖" << dendl;
}

}  // namespace rgw::sal

extern "C" {
void *newSimpleFileStore(CephContext *cct) {
  ceph_abort("unsupported");
  return nullptr;
}
}

// }}}
