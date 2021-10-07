//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/instrumented_mutex.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/statistics.h"
#include "test_util/sync_point.h"

namespace rocksdb {
namespace {
Statistics* stats_for_report(Env* env, Statistics* stats) {
  if (env != nullptr && stats != nullptr &&
      stats->get_stats_level() > kExceptTimeForMutex) {
    return stats;
  } else {
    return nullptr;
  }
}
}  // namespace

void InstrumentedMutex::Lock(uint32_t mutex_own_ticker_type) {
  uint64_t locked_start_time = 0;
  {
    PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
        db_mutex_lock_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
        stats_for_report(env_, stats_), stats_code_, &locked_start_time);
    LockInternal();
  }

  if (enable_owned_timer_ && mutex_own_ticker_type != DB_MUTEX_OWN_MICROS_BY_USER_API && env_ != nullptr) {
    if (!locked_start_time) {
      locked_start_time = env_->NowNanos(); 
    }  
  } else {
    locked_start_time = 0;
  }
  last_locked_time_ = locked_start_time;
  mutex_own_ticker_type_ = mutex_own_ticker_type; 
}

void InstrumentedMutex::LockInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  mutex_.Lock();
}

void InstrumentedCondVar::Wait() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_, nullptr);
  WaitInternal();
}

void InstrumentedCondVar::WaitInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  cond_.Wait();
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us) {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(env_, stats_), stats_code_, nullptr);
  return TimedWaitInternal(abs_time_us);
}

bool InstrumentedCondVar::TimedWaitInternal(uint64_t abs_time_us) {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif

  TEST_SYNC_POINT_CALLBACK("InstrumentedCondVar::TimedWaitInternal",
                           &abs_time_us);

  return cond_.TimedWait(abs_time_us);
}

}  // namespace rocksdb
