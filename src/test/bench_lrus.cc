// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 Clyso GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <benchmark/benchmark.h>
#include <uuid/uuid.h>
#include <xxhash.h>

#include <iterator>
#include <numeric>
#include <random>

#include "common/ceph_argparse.h"
#include "common/cohort_lru.h"
#include "common/random_string.h"
#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "global/global_context.h"
#include "global/global_init.h"

// Cache implementations in the Ceph codebase:
// ✅ cohort lru
// ✅ shared_cache.hpp SharedLRU
// ✅ simple_cache SimpleLRU
// Not benchmarked here (reason):
// ❌ LRUSet (not concurrent)
// ❌ intrusive_lru: lru implementation with embedded map and list hook (not concurrent)
// ❌ include/lru.h LRU - (not concurrent)

// Workloads
// (1) insert unique items > cache size
//   - excercises cache replacement algorithm
//   - with > 1 thread - test concurrency
// (2) Inserts using pareto distributed keys
//   - approximates real world workload

static std::string random_key() {
  return gen_rand_alphanumeric_plain(g_ceph_context, 16);
}

static std::string random_value() {
  return gen_rand_alphanumeric(g_ceph_context, 32);
}

static std::vector<std::string> key_pool(int len) {
  std::vector<std::string> result;
  result.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    result.push_back(random_key());
  }
  return result;
}

static std::vector<std::string_view> workload(
    const std::vector<std::string> pool, int length) {
  const double alpha = 1.5;
  std::vector<double> weights(pool.size());
  for (size_t i = 0; i < pool.size(); ++i) {
    weights[i] = std::pow(static_cast<double>(i + 1), -alpha);
  }
  const double weights_sum =
      std::accumulate(weights.begin(), weights.end(), 0.0);
  for (auto& w : weights) {
    w /= weights_sum;
  }
  std::vector<double> partial_sums(weights.size());
  std::partial_sum(weights.begin(), weights.end(), partial_sums.begin());

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<double> dis(0.0, 1.0);

  std::vector<std::string_view> result;
  result.reserve(length);

  for (size_t i = 0; i < length; ++i) {
    double u = dis(gen);
    auto it = std::lower_bound(partial_sums.begin(), partial_sums.end(), u);
    size_t idx = std::distance(partial_sums.begin(), it);
    result.push_back(pool[idx]);
  }
  return result;
}

static void BM_SharedLRU_Pareto(benchmark::State& state) {
  static SharedLRU<std::string, bufferlist>* lru = nullptr;
  const auto pool = key_pool(1000);

  if (state.thread_index() == 0) {
    lru =
        new SharedLRU<std::string, bufferlist>(g_ceph_context, state.range(0));
  }
  for (auto _ : state) {
    state.PauseTiming();
    const auto keys = workload(pool, state.range(1) / state.threads());
    ceph_assert(keys.size() > 10);
    state.counters["keys"] = keys.size();
    state.ResumeTiming();
    for (int i = 0; i < keys.size(); ++i) {
      auto bl = new bufferlist;
      bool existed;
      const auto ptr = lru->add(std::string(keys[i]), bl, &existed);
      if (existed) {
        state.counters["existed"]++;
        delete bl;
      }
    }
  }
  if (state.thread_index() == 0) {
    delete lru;
  }
}

static void BM_SharedLRU_UniqueAdd(benchmark::State& state) {
  static SharedLRU<std::string, bufferlist>* lru = nullptr;
  if (state.thread_index() == 0) {
    lru =
        new SharedLRU<std::string, bufferlist>(g_ceph_context, state.range(0));
  }
  for (auto _ : state) {
    for (int i = 0; i < state.range(1) / state.threads(); ++i) {
      const auto key = random_key();
      const auto val = random_value();
      auto bl = new bufferlist;
      bl->append(val);
      bool existed;
      const auto ptr = lru->add(key, bl, &existed);
      if (existed) {
        state.counters["existed"]++;
      }
    }
  }
  if (state.thread_index() == 0) {
    delete lru;
  }
}

static void BM_SimpleLRU_UniqueAdd(benchmark::State& state) {
  static SimpleLRU<std::string, bufferlist>* lru = nullptr;
  if (state.thread_index() == 0) {
    lru = new SimpleLRU<std::string, bufferlist>(state.range(0));
  }
  for (auto _ : state) {
    for (int i = 0; i < state.range(1); ++i) {
      const auto key = random_key();
      const auto val = random_value();
      bufferlist bl;
      bl.append(val);
      lru->add(key, bl);
    }
  }
  if (state.thread_index() == 0) {
    delete lru;
  }
}

class Factory;

struct Object : public cohort::lru::Object {
  std::string m_key;
  bufferlist m_value;

  Object(std::string key, bufferlist value)
      : cohort::lru::Object(), m_key(key), m_value(value) {}

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) override;

  ~Object() override {}
};

struct Factory : public cohort::lru::ObjectFactory {
  std::string m_key;
  bufferlist m_value;

  Factory(std::string key, bufferlist value)
      : cohort::lru::ObjectFactory(), m_key(key), m_value(value) {}
  ~Factory() override {};

  cohort::lru::Object* alloc(void) override {
    return new Object(m_key, m_value);
  }

  void recycle(cohort::lru::Object* o) override {
    auto oo = dynamic_cast<Object*>(o);
    oo->m_key = m_key;
    oo->m_value = m_value;
  }
};

bool Object::reclaim(const cohort::lru::ObjectFactory* newobj_fac) {
  auto factory = dynamic_cast<const Factory*>(newobj_fac);
  if (factory == nullptr) {
    return false;
  }
  return true;
}

template <class... Args>
static void BM_Cohort_UniqueAdd(benchmark::State& state, Args&&... args) {
  const auto args_tuple = std::make_tuple(std::move(args)...);
    const int lanes = std::get<0>(args_tuple);
    const int hiwat = state.range(0) / lanes;
  static cohort::lru::LRU<std::mutex>* lru;
  if (state.thread_index() == 0) {
    lru = new cohort::lru::LRU<std::mutex>(lanes, hiwat);
  }

  for (auto _ : state) {
    for (int i = 0; i < state.range(1); ++i) {
      const auto key = random_key();
      const auto val = random_value();
      bufferlist bl;
      bl.append(val);
      Factory prototype(key, bl);
      uint32_t iflags{cohort::lru::FLAG_INITIAL};
      auto o = static_cast<Object*>(
          lru->insert(&prototype, cohort::lru::Edge::MRU, iflags));
      // ceph_assert(o->m_key == key);
      // ceph_assert(o->m_value == bl);
    }
  }
  if (state.thread_index() == 0) {
    delete lru;
  }
}

template <class... Args>
static void BM_Cohort_Pareto(benchmark::State& state, Args&&... args) {
  const auto args_tuple = std::make_tuple(std::move(args)...);
  const int lanes = std::get<0>(args_tuple);
  const int hiwat = state.range(0) / lanes;
  static cohort::lru::LRU<std::mutex>* lru;
  const auto pool = key_pool(1000);

  if (state.thread_index() == 0) {
    lru = new cohort::lru::LRU<std::mutex>(lanes, hiwat);
  }
  for (auto _ : state) {
    state.PauseTiming();
    const auto keys = workload(pool, state.range(1) / state.threads());
    ceph_assert(keys.size() > 10);
    state.counters["keys"] = keys.size();
    state.ResumeTiming();
    for (int i = 0; i < keys.size(); ++i) {
      bufferlist bl;
      Factory prototype(std::string(keys[i]), bl);
      uint32_t iflags{cohort::lru::FLAG_INITIAL};
      auto o = static_cast<Object*>(
          lru->insert(&prototype, cohort::lru::Edge::MRU, iflags));
    }
  }
  if (state.thread_index() == 0) {
    delete lru;
  }
}

// Configure Benchmark Runs
// Parameters: <cache size> <number of inserts>

// Pareto distributed
BENCHMARK(BM_SharedLRU_Pareto)
    ->Args({100, 10000})
    ->Args({100, 100000})
    ->Args({1000, 100000})
    ->Threads(1)
    ->Threads(8)
    ->Threads(128);

BENCHMARK_CAPTURE(BM_Cohort_Pareto, , 5l, 5)
    ->Args({100, 10000})
    ->Args({100, 100000})
    ->Args({1000, 100000})
    ->Threads(1)
    ->Threads(8)
    ->Threads(128);

// Unique inserts
BENCHMARK(BM_SharedLRU_UniqueAdd)
    ->Args({100, 10000})
    ->Args({100, 100000})
    ->Args({1000, 100000})
    ->Threads(1)
    ->Threads(8)
    ->Threads(32)
    ->Threads(128);
BENCHMARK(BM_SimpleLRU_UniqueAdd)
    ->Args({100, 10000})
    ->Args({100, 100000})
    ->Args({1000, 100000})
    ->Threads(1)
    ->Threads(8)
    ->Threads(32)
    ->Threads(128);
BENCHMARK_CAPTURE(BM_Cohort_UniqueAdd, 1l, 5)
    ->Args({100, 10000})
    ->Args({100, 100000})
    ->Args({1000, 100000})
    ->Threads(1)
    ->Threads(8)
    ->Threads(32)
    ->Threads(128);
BENCHMARK_CAPTURE(BM_Cohort_UniqueAdd, 5l, 5)
    ->Args({100, 10000})
    ->Args({100, 100000})
    ->Args({1000, 100000})
    ->Threads(1)
    ->Threads(8)
    ->Threads(32)
    ->Threads(128);
BENCHMARK_CAPTURE(BM_Cohort_UniqueAdd, 32l, 32)
    ->Args({100, 10000})
    ->Args({100, 100000})
    ->Args({1000, 100000})
    ->Threads(1)
    ->Threads(8)
    ->Threads(32)
    ->Threads(128);

int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(
      nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);

  char arg0_default[] = "benchmark";
  char* args_default = arg0_default;
  if (!argv) {
    argc = 1;
    argv = &args_default;
  }
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
