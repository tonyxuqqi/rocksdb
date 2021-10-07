//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/thread_status.h"
#include "util/stop_watch.h"
#include "monitoring/perf_timer.h"

namespace rocksdb {
class InstrumentedCondVar;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 public:
  explicit InstrumentedMutex(bool adaptive = false, bool enable_owned_time = false)
      : mutex_(adaptive), stats_(nullptr), env_(nullptr),
        stats_code_(0),
        enable_owned_time_(enable_owned_time),
        time_recorder_(stats_),
        ticker_type_(0) {}

  InstrumentedMutex(
      Statistics* stats, Env* env,
      int stats_code, bool adaptive = false, bool enable_owned_time = false)
      : mutex_(adaptive), stats_(stats), env_(env),
        stats_code_(stats_code),
        enable_owned_time_(enable_owned_time),
        time_recorder_(stats_),
        ticker_type_(0) {}

  void Lock(uint32_t tick_type = DB_MUTEX_OWN_MICROS_BY_OTHER);

  void Unlock() {
    uint64_t start_record = time_recorder_.GetStartRecord();
    uint32_t ticker_type = ticker_type_;
    mutex_.Unlock();    
    if (enable_owned_time_ && ticker_type != DB_MUTEX_OWN_MICROS_BY_USER_API) {
        time_recorder_.Stop(start_record, ticker_type);
    }
  }

  void AssertHeld() {
    mutex_.AssertHeld();
  }

 private:
  void LockInternal(uint32_t tick_type = DB_MUTEX_OWN_MICROS_BY_OTHER);
  friend class InstrumentedCondVar;
  port::Mutex mutex_;
  Statistics* stats_;
  Env* env_;
  int stats_code_;
  bool enable_owned_time_;
  PerfTimer time_recorder_; // the time between lock and unlock
  uint32_t ticker_type_;
};

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutexLock {
 public:
  explicit InstrumentedMutexLock(InstrumentedMutex* mutex, uint32_t tick_type = DB_MUTEX_OWN_MICROS_BY_OTHER) : mutex_(mutex) {
    mutex_->Lock(tick_type);
  }

  ~InstrumentedMutexLock() {
    mutex_->Unlock();
  }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexLock(const InstrumentedMutexLock&) = delete;
  void operator=(const InstrumentedMutexLock&) = delete;
};

class InstrumentedCondVar {
 public:
  explicit InstrumentedCondVar(InstrumentedMutex* instrumented_mutex)
      : cond_(&(instrumented_mutex->mutex_)),
        stats_(instrumented_mutex->stats_),
        env_(instrumented_mutex->env_),
        stats_code_(instrumented_mutex->stats_code_) {}

  void Wait();

  bool TimedWait(uint64_t abs_time_us);

  void Signal() {
    cond_.Signal();
  }

  void SignalAll() {
    cond_.SignalAll();
  }

 private:
  void WaitInternal();
  bool TimedWaitInternal(uint64_t abs_time_us);
  port::CondVar cond_;
  Statistics* stats_;
  Env* env_;
  int stats_code_;
};

}  // namespace rocksdb
