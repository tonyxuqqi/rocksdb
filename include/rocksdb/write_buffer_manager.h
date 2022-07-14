//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <mutex>

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManager;

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  virtual ~StallInterface() {}

  virtual void Block() = 0;

  virtual void Signal() = 0;
};

class WriteBufferManager final {
 public:
  // Parameters:
  // _buffer_size: _buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  //
  // cache_: if `cache` is provided, we'll put dummy entries in the cache and
  // cost the memory allocated to the cache. It can be used even if _buffer_size
  // = 0.
  //
  // allow_stall: if set true, it will enable stalling of writes when
  // memory_usage() exceeds buffer_size. It will wait for flush to complete and
  // memory usage to drop down.
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {},
                              bool allow_stall = false);
  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  ~WriteBufferManager();

  // Returns true if a non-zero buffer_limit is passed to limit the total
  // memory usage or cache is provided to charge write buffer memory.
  bool enabled() const {
    return buffer_size() > 0 || cache_res_mgr_ != nullptr;
  }

  // Returns the total memory used by memtables.
  // Only valid if enabled().
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }

  size_t dummy_entries_in_cache_usage() const;

  // Returns the buffer_size.
  size_t buffer_size() const {
    return buffer_size_.load(std::memory_order_relaxed);
  }

  void SetBufferSize(size_t new_size) {
    buffer_size_.store(new_size, std::memory_order_relaxed);
    mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);
    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
  }

  // Below functions should be called by RocksDB internally.

  void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  void FreeMem(size_t mem);

  // Should only be called from write thread
  bool ShouldFlush() const {
    size_t local_size = buffer_size();
    if (local_size > 0) {
      if (mutable_memtable_memory_usage() >
          mutable_limit_.load(std::memory_order_relaxed)) {
        return true;
      }
      if (memory_usage() >= local_size &&
          mutable_memtable_memory_usage() >= local_size / 2) {
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        return true;
      }
    }
    return false;
  }

  // Returns true if total memory usage exceeded buffer_size.
  // We stall the writes untill memory_usage drops below buffer_size. When the
  // function returns true, all writer threads (including one checking this
  // condition) across all DBs will be stalled. Stall is allowed only if user
  // pass allow_stall = true during WriteBufferManager instance creation.
  //
  // Should only be called by RocksDB internally .
  bool ShouldStall() const {
    return is_stall_active() ||
           (allow_stall_ && buffer_size() > 0 && is_stall_threshold_exceeded())
  }

  // Returns true if stall is active.
  bool is_stall_active() const {
    return stall_active_.load(std::memory_order_relaxed);
  }

  // Returns true if stalling condition is met. Only valid if buffer_size_ is
  // non-zero.
  bool is_stall_threshold_exceeded() const {
    return memory_usage() >= buffer_size();
  }

  // Add the DB instance to the queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  // Called when DB instance is closed.
  void RemoveDBFromQueue(StallInterface* wbm_stall);

 private:
  // Shared by buffer_size limit and cache charging.
  // When cache charging is enabled, this is updated under cache_res_mgr_.
  std::atomic<size_t> memory_used_;

  std::atomic<size_t> buffer_size_;
  // Derived from buffer_size.
  std::atomic<size_t> mutable_limit_;
  // Memory that hasn't been scheduled to free.
  // Only used when buffer_size is non-zero.
  std::atomic<size_t> memory_active_;

  const bool allow_stall_;
  std::list<StallInterface*> queue_;
  // Protects the queue_ and stall_active_.
  std::mutex mu_;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  // It is a cached value of `ShouldStall()`.
  std::atomic<bool> stall_active_;

  std::unique_ptr<CacheReservationManager> cache_res_mgr_;
  // Protects cache_res_mgr_
  std::mutex cache_res_mgr_mu_;

  void ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);
};
}  // namespace ROCKSDB_NAMESPACE
