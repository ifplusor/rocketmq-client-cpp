/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ROCKETMQ_CONSUMER_PROCESSQUEUE_H_
#define ROCKETMQ_CONSUMER_PROCESSQUEUE_H_

#include <atomic>  // std::atomic
#include <limits>  // std::numeric_limits
#include <map>     // std::map
#include <memory>  // std::shared_ptr
#include <mutex>   // std::mutex
#include <vector>  // std::vector

#include "MessageExt.h"
#include "MessageQueue.hpp"

namespace rocketmq {

struct ProcessQueueInfo;

class ProcessQueue;
using ProcessQueuePtr = std::shared_ptr<ProcessQueue>;

class ProcessQueue {
 public:
  ProcessQueue(MessageQueue message_queue);

  bool PutMessages(const std::vector<MessageExtPtr>& messages);

  std::vector<MessageExtPtr> TakeMessages(int batch_size) {
    int64_t next_offset;
    return TakeMessages(batch_size, true, std::numeric_limits<int64_t>::max(), next_offset, nullptr);
  }
  std::vector<MessageExtPtr> TakeMessages(int batch_size,
                                          bool need_commit,
                                          int64_t offset_limit,
                                          int64_t& next_offset,
                                          bool* remained);

  void MakeMessagesToCosumeAgain(std::vector<MessageExtPtr>& messages);

  int64_t Commit();
  int64_t Commit(const std::vector<MessageExtPtr>& messages);

  int64_t RemoveMessages(const std::vector<MessageExtPtr>& messages);

  void ClearAllMessages();

  int GetCachedMessagesCount();
  int64_t GetCachedMinOffset();
  int64_t GetCachedMaxOffset();

  bool IsLockExpired() const;
  bool IsPullExpired() const;

  void FillProcessQueueInfo(ProcessQueueInfo& info);

 public:
  const MessageQueue& message_queue() const { return message_queue_; }

  bool dropped() const { return dropped_; }
  void set_dropped(bool dropped) { dropped_ = dropped; }

  bool locked() const { return locked_; }
  void set_locked(bool locked) { locked_ = locked; }

  bool paused() const { return paused_; }
  void set_paused(bool paused) { paused_ = paused; }

  int64_t pull_offset() const { return pull_offset_; }
  void set_pull_offset(int64_t pull_offset) { pull_offset_ = pull_offset; }

  int64_t consume_offset() const { return consume_offset_; }
  void set_consume_offset(int64_t consume_offset) { consume_offset_ = consume_offset; }

  int64_t seek_offset() const { return seek_offset_; }
  void set_seek_offset(int64_t seek_offset) { seek_offset_ = seek_offset; }

  std::timed_mutex& consume_mutex() { return consume_mutex_; }

  long try_unlock_times() const { return try_unlock_times_; }
  void inc_try_unlock_times() { try_unlock_times_ += 1; }

  uint64_t last_pull_timestamp() const { return last_pull_timestamp_; }
  void set_last_pull_timestamp(uint64_t last_pull_timestamp) { last_pull_timestamp_ = last_pull_timestamp; }

  uint64_t last_consume_timestamp() const { return last_consume_timestamp_; }
  void set_last_consume_timestamp(uint64_t last_consume_timestamp) { last_consume_timestamp_ = last_consume_timestamp; }

  uint64_t last_lock_timestamp() const { return last_lock_timestamp_; }
  void set_last_lock_timestamp(int64_t last_lock_timestamp) { last_lock_timestamp_ = last_lock_timestamp; }

 private:
  const MessageQueue message_queue_;

  // message cache
  std::mutex message_cache_mutex_;
  std::map<int64_t, MessageExtPtr> message_cache_;
  std::map<int64_t, MessageExtPtr> consuming_message_cache_;  // for orderly
  int64_t queue_offset_max_{-1};

  // flag
  std::atomic<bool> dropped_{false};
  std::atomic<bool> locked_{false};

  // state
  std::atomic<bool> paused_{false};
  std::atomic<int64_t> pull_offset_{-1};
  std::atomic<int64_t> consume_offset_{-1};
  std::atomic<int64_t> seek_offset_{-1};

  // consume lock
  std::timed_mutex consume_mutex_;
  std::atomic<long> try_unlock_times_{0};

  // timestamp record
  std::atomic<uint64_t> last_pull_timestamp_;
  std::atomic<uint64_t> last_consume_timestamp_;
  std::atomic<uint64_t> last_lock_timestamp_;  // ms
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_PROCESSQUEUE_H_
