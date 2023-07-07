// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_s3gw_telemetry.h"

#include <asm-generic/errno-base.h>
#include <curl/curl.h>

#include <boost/beast/http/status.hpp>
#include <chrono>
#include <memory>
#include <thread>

#include "common/Formatter.h"
#include "common/Thread.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "common/version.h"
#include "global/global_context.h"
#include "include/buffer_fwd.h"
#include "include/util.h"

#if defined(__linux__)
#include <linux/magic.h>
#include <sys/vfs.h>
#endif

#define dout_subsys ceph_subsys_rgw

S3GWTelemetry::S3GWTelemetry(
    CephContext* _cct, const rgw::sal::SFStore* const _sfs
)
    : m_state(),
      m_cct(_cct),
      m_sfs(_sfs),
      m_shutdown(true),
      m_updater_cvar(),
      m_updater_mutex(make_mutex("S3GWTelemetry::Updater")) {}

void S3GWTelemetry::start() {
  const bool enabled = m_cct->_conf.get_val<bool>("rgw_s3gw_enable_telemetry");

  if (!enabled) {
    ldout(m_cct, 1) << __func__ << ": telemetry disabled by configuration."
                    << dendl;
  } else {
    m_shutdown = false;
    m_updater = make_named_thread(
        "s3gw_telemetry_updater", &S3GWTelemetry::updater_main, this
    );
  }
}

void S3GWTelemetry::wake_up() {
#ifdef CEPH_DEBUG_MUTEX
  m_updater_cvar.notify_all(true);
#else
  m_updater_cvar.notify_all();
#endif  // CEPH_DEBUG_MUTEX
}

void S3GWTelemetry::stop() {
  if (!m_shutdown) {
    m_shutdown = true;
    wake_up();
    m_updater.join();
  }
}

void S3GWTelemetry::update() {
  wake_up();
}

void S3GWTelemetry::updater_main() {
  while (true) {
    std::unique_lock lock(m_updater_mutex);
    ldout(m_cct, 19) << __func__ << ": updating telemetry. interval_millis="
                     << m_state.update_interval().count() << dendl;
    do_update();
    const auto shutdown_requested = m_updater_cvar.wait_for(
        lock, m_state.update_interval(), [&] { return m_shutdown; }
    );
    if (shutdown_requested) {
      ldout(m_cct, 10) << __func__ << ": shutting down telemetry updater"
                       << dendl;
      break;
    }
  }
}

S3GWTelemetry::Status S3GWTelemetry::status() const {
  return m_state.status();
}

std::vector<S3GWTelemetry::Version> S3GWTelemetry::available_versions() const {
  return m_state.versions();
}

bool S3GWTelemetry::post_to_update_responder(
    const std::string& body, bufferlist& response
) const {
  auto curl = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>(
      curl_easy_init(), curl_easy_cleanup
  );
  if (!curl) {
    return false;
  }
  const auto headers =
      std::unique_ptr<struct curl_slist, decltype(&curl_slist_free_all)>(
          []() {
            struct curl_slist* result =
                curl_slist_append(nullptr, "Accept: application/json");
            result =
                curl_slist_append(result, "Content-Type: application/json");
            result = curl_slist_append(result, "Expect:");
            return result;
          }(),
          curl_slist_free_all
      );
  const auto url = m_cct->_conf.get_val<std::string>(
      "rgw_s3gw_telemetry_upgrade_responder_url"
  );

  curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl.get(), CURLOPT_POST, 1L);
  curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
  curl_easy_setopt(curl.get(), CURLOPT_READDATA, &body);
  curl_easy_setopt(
      curl.get(), CURLOPT_READFUNCTION,
      +[](char* buffer, size_t size, size_t nmemb, void* userdata) -> size_t {
        const std::string* body = static_cast<const std::string*>(userdata);
        const size_t bytes_to_copy = size * nmemb;
        const size_t bytes_copied = body->copy(buffer, bytes_to_copy);
        return bytes_copied;
      }
  );
  curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);
  curl_easy_setopt(
      curl.get(), CURLOPT_WRITEFUNCTION,
      +[](char* buffer, size_t size, size_t nmemb, void* userdata) -> size_t {
        bufferlist* response = static_cast<bufferlist*>(userdata);
        const size_t bytes_to_copy = size * nmemb;
        response->append(buffer, bytes_to_copy);
        return bytes_to_copy;
      }
  );
  curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE, body.length());
  curl_easy_setopt(curl.get(), CURLOPT_VERBOSE, 0L);
  curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, 10);
  curl_easy_setopt(curl.get(), CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(
      curl.get(), CURLOPT_LOW_SPEED_TIME, m_cct->_conf->rgw_curl_low_speed_time
  );
  curl_easy_setopt(
      curl.get(), CURLOPT_LOW_SPEED_LIMIT,
      m_cct->_conf->rgw_curl_low_speed_limit
  );
  curl_easy_setopt(
      curl.get(), CURLOPT_TCP_KEEPALIVE, m_cct->_conf->rgw_curl_tcp_keepalive
  );
  curl_easy_setopt(
      curl.get(), CURLOPT_BUFFERSIZE, m_cct->_conf->rgw_curl_buffersize
  );

  CURLcode res = curl_easy_perform(curl.get());

  if (res != CURLE_OK) {
    long http_status;
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &http_status);
    ldout(m_cct, 2) << __func__ << ": upgrade responder POST unsuccessful. "
                    << " http status: " << http_status << dendl;
  }

  return (res == CURLE_OK);
}

void S3GWTelemetry::Version::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("Name", name, obj, true);
  JSONDecoder::decode_json("ReleaseDate", release_date, obj, true);
}

bool S3GWTelemetry::parse_upgrade_response(
    bufferlist& response, std::chrono::milliseconds& out_update_interval,
    std::vector<Version>& out_versions
) const {
  JSONParser parser;
  if (!parser.parse(response.c_str(), response.length())) {
    ldout(m_cct, 2) << __func__ << ": failed to parse update responder JSON."
                    << dendl;
    ldout(m_cct, 20) << __func__ << ": response data was: " << response.to_str()
                     << dendl;
    return false;
  }

  auto req_interval_iter = parser.find_first("requestIntervalInMinutes");
  if (req_interval_iter.end()) {
    ldout(m_cct, 2) << __func__ << ": failed to decode update responder JSON. "
                    << " no requestIntervalInMinutes found" << dendl;
    return false;
  }
  int parsed_req_interval_minutes = -1;
  try {
    JSONDecoder::decode_json(
        "requestIntervalInMinutes", parsed_req_interval_minutes, &parser, true
    );
  } catch (const JSONDecoder::err& ex) {
    ldout(m_cct, 2) << __func__ << ": failed to decode update responder JSON. "
                    << ex.what() << dendl;
    return false;
  }

  if (parsed_req_interval_minutes < 1) {
    ldout(m_cct, 2) << __func__ << ": failed to decode update responder JSON. "
                    << "invalid request interval "
                    << parsed_req_interval_minutes << dendl;
    return false;
  }
  out_update_interval = std::chrono::minutes(parsed_req_interval_minutes);

  auto versions_iter = parser.find_first("versions");
  if (versions_iter.end()) {
    ldout(m_cct, 2) << __func__ << ": failed to decode update responder JSON. "
                    << " no versions object found" << dendl;
    return false;
  }

  JSONObj* versions_json = *versions_iter;
  auto version_iter = versions_json->find_first();
  for (; !version_iter.end(); ++version_iter) {
    try {
      Version v;
      v.decode_json(*version_iter);
      out_versions.emplace_back(v);
    } catch (const JSONDecoder::err& ex) {
      ldout(m_cct, 2) << __func__
                      << ": failed to decode update responder JSON. "
                      << ex.what() << dendl;
      return false;
    }
  }

  return true;
}

void S3GWTelemetry::do_update() {
  JSONFormatter formatter;
  std::ostringstream os;
  bufferlist response;
  const auto now = ceph::real_clock::now();

  create_update_responder_request(&formatter);
  formatter.flush(os);

  m_state.update_attempt(now);
  const bool success = post_to_update_responder(os.str(), response);
  ldout(m_cct, 20) << __func__
                   << ": s3gw telemetry response: " << response.to_str()
                   << dendl;
  ldout(m_cct, 20) << __func__ << ": s3gw telemetry request: " << os.str()
                   << dendl;

  if (success) {
    std::vector<Version> versions;
    std::chrono::milliseconds next_req_interval_minutes;
    if (parse_upgrade_response(response, next_req_interval_minutes, versions)) {
      m_state.update_success(now, versions, next_req_interval_minutes);
    }
  }
}

void S3GWTelemetry::append_sfs_telemetry(JSONFormatter* f) const {
  if (!m_sfs) {
    return;
  }
  encode_json(
      "sfs_avail_kb",
      std::to_string(m_sfs->filesystem_stats_avail_bytes / 1024), f
  );
  encode_json(
      "sfs_total_kb",
      std::to_string(m_sfs->filesystem_stats_total_bytes / 1024), f
  );

#if defined(__linux__)
  const auto& data_path = m_sfs->get_data_path();
  struct statfs stat;
  int ret = statfs(data_path.c_str(), &stat);
  if (ret == 0) {
    switch (stat.f_type) {
      case EXT4_SUPER_MAGIC:
        encode_json("sfs_fs", "ext4", f);
        break;
      case XFS_SUPER_MAGIC:
        encode_json("sfs_fs", "xfs", f);
        break;
      default:
        encode_json(
            "sfs_fs", "unknown magic " + std::to_string(stat.f_type), f
        );
        break;
    }
  } else {
    encode_json("sfs_fs", "unknown", f);
  }
#else
  encode_json("sfs_fs", "unknown", f);
#endif
}

void S3GWTelemetry::create_update_responder_request(JSONFormatter* f) const {
  std::map<std::string, std::string> sys_info;
  collect_sys_info(&sys_info, m_cct);

  uint64_t cgroup_mem_limit;
  get_cgroup_memory_limit(&cgroup_mem_limit);

  f->open_object_section("version");
  encode_json("appVersion", ceph_version_to_str(), f);

  f->open_object_section("extraInfo");
  for (const char* key :
       {"ceph_version", "ceph_version_short", "ceph_release", "os",
        "kernel_version", "kernel_description", "arch", "mem_total_kb",
        "mem_swap_kb", "cpu", "container_image"}) {
    if (const auto maybe_val = sys_info.find(key);
        maybe_val != sys_info.end()) {
      encode_json(key, maybe_val->second, f);
    }
  }
  encode_json("cgroup_mem_limit", std::to_string(cgroup_mem_limit), f);
  append_sfs_telemetry(f);
  f->close_section();  // extraInfo
  f->close_section();  // version
}
