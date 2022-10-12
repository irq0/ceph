// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#ifndef RGW_STATUS_FRONTEND_H
#define RGW_STATUS_FRONTEND_H

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/version.hpp>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>

#include "rgw_frontend.h"
#include "rgw_status_page.h"

namespace beast = boost::beast;    // from <boost/beast.hpp>
namespace http = beast::http;      // from <boost/beast/http.hpp>
namespace net = boost::asio;       // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;  // from <boost/asio/ip/tcp.hpp>

class StatusPage;

class StatusHttpConnection
    : public std::enable_shared_from_this<StatusHttpConnection> {
 public:
  StatusHttpConnection(
      tcp::socket socket,
      const std::vector<std::unique_ptr<StatusPage>>& status_pages
  );
  void start();

 private:
  tcp::socket socket_;
  beast::flat_buffer buffer_{8192};
  http::request<http::dynamic_body> request_;
  http::response<http::dynamic_body> response_;
  net::steady_timer deadline_{socket_.get_executor(), std::chrono::seconds(60)};
  const std::vector<std::unique_ptr<StatusPage>>& status_pages;

  void read_request();
  void process_request();
  void write_response();
  void create_response();
  void check_deadline();
};

class RGWStatusFrontend : public RGWFrontend {
 private:
  const RGWProcessEnv& env;
  RGWFrontendConfig* conf;
  CephContext* cct;

  net::io_context ioc;
  tcp::acceptor acceptor;
  tcp::socket socket;

  std::vector<std::unique_ptr<StatusPage>> status_pages;

  class StatusServerThread : public Thread {
   private:
    RGWStatusFrontend* frontend;
    void* entry() override;

   public:
    StatusServerThread(RGWStatusFrontend* frontend) : frontend(frontend){};
  } server_thread;

 public:
  RGWStatusFrontend(
      const RGWProcessEnv& env, RGWFrontendConfig* conf, CephContext* cct
  );
  ~RGWStatusFrontend() override;

  int init() override;
  int run() override;
  void stop() override;
  void join() override;

  void register_status_page(std::unique_ptr<StatusPage> status_page);
  const std::vector<std::unique_ptr<StatusPage>>& get_status_pages() const;

  void pause_for_new_config() override;
  void unpause_with_new_config() override;

  friend class StatusServerThread;
};

#endif  // RGW_STATUS_FRONTEND_H
