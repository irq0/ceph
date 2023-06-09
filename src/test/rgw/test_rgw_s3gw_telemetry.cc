// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <sstream>

#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "rgw_common.h"
#include "rgw_s3gw_telemetry.h"
#include "rgw_sal_sfs.h"

class MockSFStore : rgw::sal::SFStore {};

class TestS3GWTelemetry : public ::testing::Test {
 protected:
  CephContext* cct;
  std::unique_ptr<S3GWTelemetry> uut;

  void SetUp() override {
    cct = (new CephContext(CEPH_ENTITY_TYPE_ANY))->get();
    uut = std::make_unique<S3GWTelemetry>(cct, nullptr);
  }
};

TEST_F(TestS3GWTelemetry, parses_valid_response) {
  bufferlist bl;
  bl.append(R"json(
  {
    "requestIntervalInMinutes": 42,
    "versions": [
        {
            "ExtraInfo": null,
            "MinUpgradableVersion": "",
            "Name": "v0.23.42",
            "ReleaseDate": "2023-03-09T12:00:00Z",
            "Tags": [
                "v0.23.42",
                "latest"
            ]
        }
    ]
})json");

  std::vector<S3GWTelemetry::Version> versions;
  std::chrono::milliseconds interval;
  ASSERT_TRUE(uut->parse_upgrade_response(bl, interval, versions));
  ASSERT_TRUE(interval == std::chrono::minutes(42));
  ASSERT_EQ(versions.size(), 1);
  ASSERT_EQ(versions[0].name, "v0.23.42");
  ASSERT_GT(versions[0].release_date.time_since_epoch().count(), 0);
}

TEST_F(TestS3GWTelemetry, broken_json_responses_return_false) {
  bufferlist bl;
  bl.append("{{{{ ~~~ BROKEN JSON]]]");

  std::vector<S3GWTelemetry::Version> versions;
  std::chrono::milliseconds interval;
  ASSERT_FALSE(uut->parse_upgrade_response(bl, interval, versions));
}

TEST_F(TestS3GWTelemetry, invalid_json_responses_return_false) {
  std::vector<S3GWTelemetry::Version> versions;
  std::chrono::milliseconds interval;
  bufferlist bl;
  bl.append(R"json({
    "wat?": [23, 42]
})json");

  ASSERT_FALSE(uut->parse_upgrade_response(bl, interval, versions));

  bl.clear();
  bl.append(R"json({
    "versions": [23, 42]
})json");
  ASSERT_FALSE(uut->parse_upgrade_response(bl, interval, versions));
}

TEST_F(TestS3GWTelemetry, vaild_response_without_versions) {
  std::vector<S3GWTelemetry::Version> versions;
  std::chrono::milliseconds interval;
  bufferlist bl;
  bl.append(R"json(
  {
    "requestIntervalInMinutes": 23,
    "versions": [
    ]
})json");
  ASSERT_TRUE(uut->parse_upgrade_response(bl, interval, versions));
  ASSERT_TRUE(interval == std::chrono::minutes(23));
  ASSERT_EQ(versions.size(), 0);
}

TEST_F(TestS3GWTelemetry, request_interval_vaild_only_positive_integer) {
  std::vector<S3GWTelemetry::Version> versions;
  std::chrono::milliseconds interval;
  bufferlist bl;
  bl.append(R"json(
  {
    "requestIntervalInMinutes": -1,
    "versions": [
    ]
})json");
  ASSERT_FALSE(uut->parse_upgrade_response(bl, interval, versions));

  bl.clear();
  bl.append(R"json(
  {
    "requestIntervalInMinutes": "foo",
    "versions": [
    ]
})json");
  ASSERT_FALSE(uut->parse_upgrade_response(bl, interval, versions));
}

TEST_F(TestS3GWTelemetry, creates_valid_updateresponder_json_request) {
  JSONFormatter f;
  std::ostringstream os;
  uut->create_update_responder_request(&f);
  f.flush(os);
  std::string rendered = os.str();

  JSONParser parser;
  ASSERT_TRUE(parser.parse(rendered.c_str(), rendered.length()));
  ASSERT_TRUE(parser.is_object());

  auto appVersionIter = parser.find_first("appVersion");
  ASSERT_FALSE(appVersionIter.end());
  auto appVersion = *appVersionIter;
  ASSERT_FALSE(appVersion->get_data().empty());

  auto extraInfo = parser.find_obj("extraInfo");
  ASSERT_TRUE(extraInfo != nullptr);
}
