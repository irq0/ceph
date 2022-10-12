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
#include "rgw_status_frontend.h"

#include <boost/asio/ip/address.hpp>
#include <cstdlib>
#include <string>

#include "common/debug.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"
#include "rgw_asio_frontend.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

StatusHttpConnection::StatusHttpConnection(
    tcp::socket socket,
    const std::vector<std::unique_ptr<StatusPage>>& status_pages
)
    : socket_(std::move(socket)), status_pages(status_pages) {}

void StatusHttpConnection::start() {
  read_request();
  check_deadline();
}

void StatusHttpConnection::read_request() {
  auto self = shared_from_this();

  http::async_read(
      socket_, buffer_, request_,
      [self](beast::error_code ec, std::size_t bytes_transferred) {
        boost::ignore_unused(bytes_transferred);
        if (!ec) self->process_request();
      }
  );
}

void StatusHttpConnection::process_request() {
  response_.version(request_.version());
  response_.keep_alive(false);

  switch (request_.method()) {
    case http::verb::get:
      response_.result(http::status::ok);
      response_.set(http::field::server, "RGW Status");
      create_response();
      break;

    default:
      response_.result(http::status::bad_request);
      response_.set(http::field::content_type, "text/plain");
      beast::ostream(response_.body())
          << "Invalid request-method '" << std::string(request_.method_string())
          << "'";
      break;
  }
  write_response();
}

void StatusHttpConnection::check_deadline() {
  auto self = shared_from_this();

  deadline_.async_wait([self](beast::error_code ec) {
    if (!ec) {
      self->socket_.close(ec);
    }
  });
}

void StatusHttpConnection::write_response() {
  auto self = shared_from_this();

  response_.content_length(response_.body().size());

  http::async_write(
      socket_, response_,
      [self](beast::error_code ec, std::size_t) {
        self->socket_.shutdown(tcp::socket::shutdown_send, ec);
        self->deadline_.cancel();
      }
  );
}

static void render_html_header(std::ostream& os) {
  os << "<!DOCTYPE html>\n"
     << "<html lang=\"en\">\n"
     << "<head>\n"
     << "<title>RGW Status</title>\n"
     << "<meta charset=\"utf-8\">\n"
     << "</head>\n"
     << "<body>\n";
}

static void render_html_footer(std::ostream& os) {
  os << "</body>\n"
     << "</html>\n";
}

void StatusHttpConnection::create_response() {
  auto os = beast::ostream(response_.body());

  if (request_.target() == "/") {
    response_.set(http::field::content_type, "text/html");
    render_html_header(os);
    os << "<h1>RGW Status Page Index</h1>\n"
       << "<ul>\n";
    for (const auto& status_page : status_pages) {
      os << "<li><a href=\"" << status_page->prefix() << "\">"
         << status_page->name() << "</a></li>\n";
    }
    os << "</ul>";
    render_html_footer(os);
  } else {
    for (const auto& status_page : status_pages) {
      if (status_page->prefix() == request_.target()) {
        response_.set(http::field::content_type, status_page->content_type());
        if (status_page->content_type() == "text/html") {
          render_html_header(os);
        }
        response_.result(status_page->render(os));
        if (status_page->content_type() == "text/html") {
          render_html_footer(os);
        }
        return;
      }
    }
    response_.result(http::status::not_found);
    response_.set(http::field::content_type, "text/plain");
    os << "File not found\r\n";
  }
}

RGWStatusFrontend::RGWStatusFrontend(
    const RGWProcessEnv& env, RGWFrontendConfig* conf, CephContext* cct
)
    : env(env),
      conf(conf),
      cct(cct),
      ioc(1),
      acceptor(
          ioc,
          {net::ip::make_address(conf->get_val("bind", "127.0.0.1")),
           static_cast<unsigned short>(std::stoi(conf->get_val("port", "9090"))
           )}
      ),
      socket(ioc),
      server_thread(this) {}
RGWStatusFrontend::~RGWStatusFrontend() {}

void http_server(
    tcp::acceptor& acceptor, tcp::socket& socket,
    const std::vector<std::unique_ptr<StatusPage>>& status_pages
) {
  acceptor.async_accept(socket, [&](beast::error_code ec) {
    if (!ec)
      std::make_shared<StatusHttpConnection>(std::move(socket), status_pages)
          ->start();
    http_server(acceptor, socket, status_pages);
  });
}

int RGWStatusFrontend::init() {
  try {
    http_server(acceptor, socket, get_status_pages());
  } catch (std::exception const& e) {
    ldout(cct, 0) << "Error: " << e.what() << dendl;
    return -1;
  }
  return 0;
}

void* RGWStatusFrontend::StatusServerThread::entry() {
  frontend->ioc.run();
  return nullptr;
}

int RGWStatusFrontend::run() {
  server_thread.create("status-server");
  return 0;
}

void RGWStatusFrontend::stop() {
  ioc.stop();
}

void RGWStatusFrontend::join() {
  server_thread.join(nullptr);
}

void RGWStatusFrontend::pause_for_new_config() {}

void RGWStatusFrontend::unpause_with_new_config() {}

void RGWStatusFrontend::register_status_page(
    std::unique_ptr<StatusPage> status_page
) {
  status_pages.emplace_back(std::move(status_page));
}

const std::vector<std::unique_ptr<StatusPage>>&
RGWStatusFrontend::get_status_pages() const {
  return status_pages;
}
