//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
WriteBufferManager::WriteBufferManager(size_t _flush_size,
                                       std::shared_ptr<Cache> cache,
                                       float stall_ratio,
                                       bool flush_oldest_first)
    : head_(nullptr),
      memory_used_(0),
      flush_size_(_flush_size),
      memory_active_(0),
      flush_oldest_first_(flush_oldest_first),
      allow_stall_(stall_ratio >= 1.0),
      stall_ratio_(stall_ratio),
      stall_active_(false),
      cache_res_mgr_(nullptr) {
#ifndef ROCKSDB_LITE
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_.reset(
        new CacheReservationManager(cache, true /* delayed_decrease */));
  }
#else
  (void)cache;
#endif  // ROCKSDB_LITE
}

WriteBufferManager::~WriteBufferManager() {
#ifndef NDEBUG
  std::unique_lock<std::mutex> lock(stall_mu_);
  assert(queue_.empty());
#endif
}

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_res_mgr_ != nullptr) {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::ReserveMem(size_t mem) {
  size_t local_size = flush_size();
  if (cache_res_mgr_ != nullptr) {
    ReserveMemWithCache(mem);
  } else if (local_size > 0) {
    memory_used_.fetch_add(mem, std::memory_order_relaxed);
  }
  if (local_size > 0) {
    memory_active_.fetch_add(mem, std::memory_order_relaxed);
  }
}

// Should only be called from write thread
void WriteBufferManager::ReserveMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s =
      cache_res_mgr_->UpdateCacheReservation<CacheEntryRole::kWriteBuffer>(
          new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache reservation fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  if (flush_size() > 0) {
    memory_active_.fetch_sub(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  if (cache_res_mgr_ != nullptr) {
    FreeMemWithCache(mem);
  } else if (flush_size() > 0) {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }
  // Check if stall is active and can be ended.
  MaybeEndWriteStall();
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s =
      cache_res_mgr_->UpdateCacheReservation<CacheEntryRole::kWriteBuffer>(
          new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}
void WriteBufferManager::MaybeFlushLocked() {
  // If size trigger is reached, flush `kNumberFlushes` column families with
  // the highest score.
  constexpr size_t kNumberFlushes = 2;

  size_t local_size = flush_size();
  if (!ShouldFlush()) {
    return;
  }
  WriteBufferSentinel* current = head_.get();
  std::pair<WriteBufferSentinel*, size_t> candidates[kNumberFlushes];
  size_t n_candidates = 0;
  size_t total_active_mem = 0;
  while (current != nullptr) {
    uint64_t current_memory_bytes = std::numeric_limits<uint64_t>::max();
    uint64_t current_stats = std::numeric_limits<uint64_t>::max();
    current->db->GetApproximateActiveMemTableStats(
        current->cf, &current_memory_bytes, &current_stats);
    if (n_candidates < kNumberFlushes) {
      candidates[n_candidates] = std::make_pair(current, current_stats);
      n_candidates += 1;
    } else {
      if (!flush_oldest_first_) {
        current_stats = current_memory_bytes;
      }
      for (auto& c : candidates) {
        if (c.second < current_stats) {
          c.first = current;
          c.second = current_stats;
          break;
        }
      }
    }
    total_active_mem += current_memory_bytes;
    current = current->next.get();
  }

  if (total_active_mem >= local_size) {
    FlushOptions flush_opts;
    flush_opts.allow_write_stall = true;
    flush_opts.wait = false;
    for (auto& c : candidates) {
      flush_opts.min_size_to_flush = c.second;
      c.first->db->Flush(flush_opts, c.first->cf);
    }
  }
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);
  assert(allow_stall_);

  // Allocate outside of the lock.
  std::list<StallInterface*> new_node = {wbm_stall};

  {
    std::unique_lock<std::mutex> lock(stall_mu_);
    // Verify if the stall conditions are stil active.
    if (ShouldStall()) {
      stall_active_.store(true, std::memory_order_relaxed);
      queue_.splice(queue_.end(), std::move(new_node));
    }
  }

  // If the node was not consumed, the stall has ended already and we can signal
  // the caller.
  if (!new_node.empty()) {
    new_node.front()->Signal();
  }
}

// Called when memory is freed in FreeMem or the buffer size has changed.
void WriteBufferManager::MaybeEndWriteStall() {
  // Cannot early-exit on !enabled() because SetBufferSize(0) needs to unblock
  // the writers.
  if (!allow_stall_) {
    return;
  }

  if (is_stall_threshold_exceeded()) {
    return;  // Stall conditions have not resolved.
  }

  // Perform all deallocations outside of the lock.
  std::list<StallInterface*> cleanup;

  std::unique_lock<std::mutex> lock(stall_mu_);
  if (!stall_active_.load(std::memory_order_relaxed)) {
    return;  // Nothing to do.
  }

  // Unblock new writers.
  stall_active_.store(false, std::memory_order_relaxed);

  // Unblock the writers in the queue.
  for (StallInterface* wbm_stall : queue_) {
    wbm_stall->Signal();
  }
  cleanup = std::move(queue_);
}

void WriteBufferManager::RemoveDBFromStallQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Deallocate the removed nodes outside of the lock.
  std::list<StallInterface*> cleanup;

  if (allow_stall_) {
    std::unique_lock<std::mutex> lock(stall_mu_);
    for (auto it = queue_.begin(); it != queue_.end();) {
      auto next = std::next(it);
      if (*it == wbm_stall) {
        cleanup.splice(cleanup.end(), queue_, std::move(it));
      }
      it = next;
    }
  }
  wbm_stall->Signal();
}

}  // namespace ROCKSDB_NAMESPACE
