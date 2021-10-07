//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_level_imp.h"
#include "rocksdb/env.h"
#include "util/stop_watch.h"

namespace rocksdb {
class PerfTimer {
 public:
  explicit PerfTimer(Statistics* statistics)
      : env_(Env::Default()),
        start_(0),
        statistics_(statistics) {}

  void Start() {
    if (statistics_ != nullptr) {
      start_ = time_now();
    }
  }

  void SetStartTime(uint64_t start_time) {
     start_ = start_time;
  }
 
  uint64_t GetStartTime() const {
    return start_;
  }

  uint64_t time_now() {
     return env_->NowNanos();
  }

  void Stop(uint64_t start_time, uint32_t ticker_type);

 private:
  Env* const env_;
  uint64_t start_;
  Statistics* statistics_;
};

}  // namespace rocksdb
