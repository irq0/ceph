#include "common/ceph_argparse.h"
#include "rgw_common.h"
#include "rgw_lua_background.h"
#include "rgw_lua_data_filter.h"
#include "rgw_lua_request.h"
#include "rgw_perf_counters.h"
#include "rgw_process_env.h"
#include "rgw_sal_config.h"
#include "rgw_sal_rados.h"
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

using namespace std;
using namespace rgw;
using boost::container::flat_set;
using rgw::auth::Identity;
using rgw::auth::Principal;

class FakeIdentity : public Identity {
public:
  FakeIdentity() = default;

  ACLOwner get_aclowner() const override { return {}; }

  uint32_t get_perms_from_aclspec(const DoutPrefixProvider *dpp,
                                  const aclspec_t &aclspec) const override {
    return 0;
  };

  bool is_admin() const override { return false; }

  bool is_owner_of(const rgw_owner &uid) const override { return false; }

  bool is_root() const override { return false; }

  virtual uint32_t get_perm_mask() const override { return 0; }

  uint32_t get_identity_type() const override { return TYPE_RGW; }

  std::optional<rgw::ARN> get_caller_identity() const override {
    return std::nullopt;
  }

  string get_acct_name() const override { return ""; }

  string get_subuser() const override { return ""; }

  const std::string &get_tenant() const override {
    static std::string empty;
    return empty;
  }

  const std::optional<RGWAccountInfo> &get_account() const override {
    static const std::optional<RGWAccountInfo> empty;
    return empty;
  }

  void to_str(std::ostream &out) const override { return; }

  bool is_identity(const Principal &p) const override { return false; }
};

class TestUser : public sal::StoreUser {
public:
  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new TestUser(*this));
  }

  virtual int list_buckets(const DoutPrefixProvider *dpp, const string &,
                           const string &, uint64_t, bool,
                           sal::BucketList &results, optional_yield y) {
    return 0;
  }

  virtual int read_attrs(const DoutPrefixProvider *dpp,
                         optional_yield y) override {
    return 0;
  }

  virtual int
  read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
             uint64_t end_epoch, uint32_t max_entries, bool *is_truncated,
             RGWUsageIter &usage_iter,
             map<rgw_user_bucket, rgw_usage_log_entry> &usage) override {
    return 0;
  }

  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
                         uint64_t end_epoch, optional_yield y) override {
    return 0;
  }

  virtual int load_user(const DoutPrefixProvider *dpp,
                        optional_yield y) override {
    return 0;
  }

  virtual int store_user(const DoutPrefixProvider *dpp, optional_yield y,
                         bool exclusive, RGWUserInfo *old_info) override {
    return 0;
  }

  virtual int remove_user(const DoutPrefixProvider *dpp,
                          optional_yield y) override {
    return 0;
  }
  virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                    rgw::sal::Attrs &attrs,
                                    optional_yield y) override {
    return 0;
  }
  virtual int verify_mfa(const std::string &mfa_str, bool *verified,
                         const DoutPrefixProvider *dpp,
                         optional_yield y) override {
    return 0;
  }
  int list_groups(const DoutPrefixProvider *dpp, optional_yield y,
                  std::string_view marker, uint32_t max_items,
                  rgw::sal::GroupList &listing) override {
    return 0;
  }
  virtual ~TestUser() = default;
};

class TestAccounter : public io::Accounter, public io::BasicClient {
  RGWEnv env;

protected:
  virtual int init_env(CephContext *cct) override { return 0; }

public:
  ~TestAccounter() = default;

  virtual void set_account(bool enabled) override {}

  virtual uint64_t get_bytes_sent() const override { return 0; }

  virtual uint64_t get_bytes_received() const override { return 0; }

  virtual RGWEnv &get_env() noexcept override { return env; }

  virtual size_t complete_request() override { return 0; }
};

tracing::Tracer tracer;

class TestLuaManager : public rgw::sal::StoreLuaManager {
public:
  std::string lua_script;
  unsigned read_time = 0;
  TestLuaManager() { rgw_perf_start(g_ceph_context); }
  int get_script(const DoutPrefixProvider *dpp, optional_yield y,
                 const std::string &key, std::string &script) override {
    std::this_thread::sleep_for(std::chrono::seconds(read_time));
    script = lua_script;
    return 0;
  }
  int put_script(const DoutPrefixProvider *dpp, optional_yield y,
                 const std::string &key, const std::string &script) override {
    return 0;
  }
  int del_script(const DoutPrefixProvider *dpp, optional_yield y,
                 const std::string &key) override {
    return 0;
  }
  int add_package(const DoutPrefixProvider *dpp, optional_yield y,
                  const std::string &package_name) override {
    return 0;
  }
  int remove_package(const DoutPrefixProvider *dpp, optional_yield y,
                     const std::string &package_name) override {
    return 0;
  }
  int list_packages(const DoutPrefixProvider *dpp, optional_yield y,
                    rgw::lua::packages_t &packages) override {
    return 0;
  }
  int reload_packages(const DoutPrefixProvider *dpp,
                      optional_yield y) override {
    return 0;
  }
  ~TestLuaManager() { rgw_perf_stop(g_ceph_context); }
};

void set_script(rgw::sal::LuaManager *manager, const std::string &script) {
  static_cast<TestLuaManager *>(manager)->lua_script = script;
}
void set_read_time(rgw::sal::LuaManager *manager, unsigned read_time) {
  static_cast<TestLuaManager *>(manager)->read_time = read_time;
}

#define DEFINE_REQ_STATE                                                       \
  RGWProcessEnv pe;                                                            \
  pe.lua.manager = std::make_unique<TestLuaManager>();                         \
  RGWEnv e;                                                                    \
  req_state s(g_ceph_context, pe, &e, 0);

#define INIT_TRACE                                                             \
  tracer.init(g_ceph_context, "test");                                         \
  s.trace = tracer.start_trace("test", true);

class TestBackground : public rgw::lua::Background {
public:
  TestBackground(rgw::sal::LuaManager *manager)
      : rgw::lua::Background(g_ceph_context, manager,
                             1 /* run every second */) {}

  ~TestBackground() override { shutdown(); }
};

constexpr auto wait_time = std::chrono::milliseconds(100);

template <typename T>
const T &get_table_value(const TestBackground &b, const std::string &index) {
  try {
    return std::get<T>(b.get_table_value(index));
  } catch (std::bad_variant_access const &ex) {
    std::cout << "expected RGW[" << index << "] to be: " << typeid(T).name()
              << std::endl;
    throw(ex);
  }
}

#define WAIT_FOR_BACKGROUND                                                    \
  {                                                                            \
    unsigned max_tries = 100;                                                  \
    do {                                                                       \
      std::this_thread::sleep_for(wait_time);                                  \
      --max_tries;                                                             \
    } while (perfcounter->get(l_rgw_lua_script_ok) +                           \
                     perfcounter->get(l_rgw_lua_script_fail) ==                \
                 0 &&                                                          \
             max_tries > 0);                                                   \
  }

void BM_RGWLua_Metadata(benchmark::State &state) {
  const std::string script = R"(
    assert(#Request.HTTP.Metadata == 3)
    for k, v in pairs(Request.HTTP.Metadata) do
      assert(k)
      assert(v)
    end
    assert(Request.HTTP.Metadata["hello"] == "world")
    assert(Request.HTTP.Metadata["kaboom"] == nil)
    Request.HTTP.Metadata["hello"] = "goodbye"
    Request.HTTP.Metadata["kaboom"] = "boom"
    assert(#Request.HTTP.Metadata == 4)
    assert(Request.HTTP.Metadata["hello"] == "goodbye")
    assert(Request.HTTP.Metadata["kaboom"] == "boom")
  )";
  static volatile int rc;
  DEFINE_REQ_STATE;
  s.info.x_meta_map["hello"] = "world";
  s.info.x_meta_map["foo"] = "bar";
  s.info.x_meta_map["ka"] = "boom";
  for (auto _ : state) {
    rc = lua::request::execute(nullptr, nullptr, &s, nullptr, script);
  }
}

void BM_RGWLua_WriteMetadata(benchmark::State &state) {
  const std::string script = R"(
    -- change existing entry
    Request.HTTP.Metadata["hello"] = "earth"
    -- add new entry
    Request.HTTP.Metadata["goodbye"] = "mars"
    -- delete existing entry
    Request.HTTP.Metadata["foo"] = nil
    -- delete missing entry
    Request.HTTP.Metadata["venus"] = nil

    assert(Request.HTTP.Metadata["hello"] == "earth")
    assert(Request.HTTP.Metadata["goodbye"] == "mars")
    assert(Request.HTTP.Metadata["foo"] == nil)
    assert(Request.HTTP.Metadata["venus"] == nil)
  )";

  static volatile int rc;
  DEFINE_REQ_STATE;
  s.info.x_meta_map["hello"] = "world";
  s.info.x_meta_map["foo"] = "bar";
  s.info.x_meta_map["ka"] = "boom";

  for (auto _ : state) {
    rc = lua::request::execute(nullptr, nullptr, &s, nullptr, script);
  }
}

void BM_RGWLua_Acl(benchmark::State &state) {
  const std::string script = R"(
    function print_grant(k, g)
--      print("Grant Key: " .. tostring(k))
--      print("Grant Type: " .. g.Type)
      if (g.GroupType) then
 --       print("Grant Group Type: " .. g.GroupType)
      end
      if (g.Referer) then
 --       print("Grant Referer: " .. g.Referer)
      end
      if (g.User) then
  --      print("Grant User.Tenant: " .. g.User.Tenant)
   --     print("Grant User.Id: " .. g.User.Id)
      end
    end

    assert(Request.UserAcl.Owner.DisplayName == "jack black", Request.UserAcl.Owner.DisplayName)
    assert(Request.UserAcl.Owner.User == "jack$black", Request.UserAcl.Owner.User)
    assert(#Request.UserAcl.Grants == 7)
    print_grant("", Request.UserAcl.Grants[""])
    for k, v in pairs(Request.UserAcl.Grants) do
      if tostring(k) == "john$doe" then
        assert(v.Permission == 4)
      elseif tostring(k) == "jane$doe" then
        assert(v.Permission == 1)
      elseif tostring(k) == "kill$bill" then
        assert(v.Permission == 6 or v.Permission == 7)
      elseif tostring(k) ~= "" then
        assert(false)
      end
    end
  )";

  static volatile int rc;
  DEFINE_REQ_STATE;
  const ACLOwner owner{.id = rgw_user("jack", "black"),
                       .display_name = "jack black"};
  s.user_acl.set_owner(owner);
  ACLGrant grant1, grant2, grant3, grant4, grant5, grant6_1, grant6_2;
  grant1.set_canon(rgw_user("jane", "doe"), "her grant", 1);
  grant2.set_group(ACL_GROUP_ALL_USERS, 2);
  grant3.set_referer("http://localhost/ref2", 3);
  grant4.set_canon(rgw_user("john", "doe"), "his grant", 4);
  grant5.set_group(ACL_GROUP_AUTHENTICATED_USERS, 5);
  grant6_1.set_canon(rgw_user("kill", "bill"), "his grant", 6);
  grant6_2.set_canon(rgw_user("kill", "bill"), "her grant", 7);
  s.user_acl.get_acl().add_grant(grant1);
  s.user_acl.get_acl().add_grant(grant2);
  s.user_acl.get_acl().add_grant(grant3);
  s.user_acl.get_acl().add_grant(grant4);
  s.user_acl.get_acl().add_grant(grant5);
  s.user_acl.get_acl().add_grant(grant6_1);
  s.user_acl.get_acl().add_grant(grant6_2);

  for (auto _ : state) {
    rc = lua::request::execute(nullptr, nullptr, &s, nullptr, script);
    ceph_assert(rc == 0);
  }
}

void BM_RGWLua_NestedLoop(benchmark::State &state) {
  const std::string script = R"(
  for k1, v1 in pairs(Request.Environment) do
    assert(k1)
    assert(v1)
    for k2, v2 in pairs(Request.HTTP.Metadata) do
      assert(k2)
      assert(v2)
    end
  end
  )";

  static volatile int rc;
  DEFINE_REQ_STATE;
  s.env.emplace("1", "a");
  s.env.emplace("2", "b");
  s.env.emplace("3", "c");
  s.info.x_meta_map["11"] = "aa";
  s.info.x_meta_map["22"] = "bb";
  s.info.x_meta_map["33"] = "cc";
  s.info.x_meta_map["44"] = "dd";

  for (auto _ : state) {
    rc = lua::request::execute(nullptr, nullptr, &s, nullptr, script);
    ceph_assert(rc == 0);
  }
}

void BM_RGWLua_Hello(benchmark::State &state) {
  const std::string script = R"(
    RGWDebugLog("hello from lua")
  )";

  static volatile int rc;
  DEFINE_REQ_STATE;

  for (auto _ : state) {
    rc = lua::request::execute(nullptr, nullptr, &s, nullptr, script);
    ceph_assert(rc == 0);
  }
}

void BM_RGWLua_Hello_NewState(benchmark::State &state) {
  const std::string script = R"(
    RGWDebugLog("hello from lua")
  )";

  static volatile int rc;

  for (auto _ : state) {
    DEFINE_REQ_STATE;
    rc = lua::request::execute(nullptr, nullptr, &s, nullptr, script);
    ceph_assert(rc == 0);
  }
}

BENCHMARK(BM_RGWLua_Hello);
BENCHMARK(BM_RGWLua_Hello_NewState);
BENCHMARK(BM_RGWLua_Metadata);
BENCHMARK(BM_RGWLua_WriteMetadata);
BENCHMARK(BM_RGWLua_Acl);
BENCHMARK(BM_RGWLua_NestedLoop);

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);

  char arg0_default[] = "benchmark";
  char *args_default = arg0_default;
  if (argv == nullptr) {
    argc = 1;
    argv = &args_default;
  }
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
