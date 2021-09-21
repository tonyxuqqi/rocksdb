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
        statistics_(statistics),
        ticker_type_(0) {}

  ~PerfTimer() {
    Stop();
  }

  void Start(uint32_t ticket_type) {
    ticker_type_ = ticket_type;
    if (statistics_ != nullptr) {
      start_ = time_now();
    }
  }

  uint64_t time_now() {
     return env_->NowCPUNanos();
  }

  void Stop() {
    if (start_) {
      uint64_t duration = time_now() - start_;
      if (statistics_ != nullptr) {
        RecordTick(statistics_, ticker_type_, duration);
      }
      start_ = 0;
    }
  }

 private:
  Env* const env_;
  uint64_t start_;
  Statistics* statistics_;
  uint32_t ticker_type_;
};

}  // namespace rocksdb
