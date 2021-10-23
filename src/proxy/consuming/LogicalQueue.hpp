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
#ifndef ROCKETMQ_PROXY_CONSUMING_CONSUMERPROXY_HPP_
#define ROCKETMQ_PROXY_CONSUMING_CONSUMERPROXY_HPP_

#include "MessageExt.h"
#include "MessageQueue.hpp"
#include "proxy/consuming/BasicQueue.hpp"

namespace rocketmq {

struct QueueOffsetAccessor {
  int64_t operator()(const MessageExtPtr& message) const { return message->queue_offset(); }
};

struct MessageSizeAccessor {
  size_t operator()(const MessageExtPtr& message) const { return message->body().size(); }
};

class LogicalQueue : public BasicQueue<MessageQueue, MessageExtPtr, int64_t, QueueOffsetAccessor, MessageSizeAccessor> {
 public:
  LogicalQueue(IdentityType identity) : BasicQueue(std::move(identity), -1) {}

  int64_t pull_offset() const { return pull_offset_; }
  void set_pull_offset(int64_t pull_offset) { pull_offset_ = pull_offset; }

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
  // state
  std::atomic<bool> paused_{false};
  std::atomic<int64_t> pull_offset_{-1};
  std::atomic<int64_t> consume_offset_{-1};
  std::atomic<int64_t> seek_offset_{-1};

  // consume lock
  std::timed_mutex consume_mutex_;
  std::atomic<long> try_unlock_times_{0};

  // timestamp record
  std::atomic<uint64_t> last_pull_timestamp_{0};
  std::atomic<uint64_t> last_consume_timestamp_{0};
  std::atomic<uint64_t> last_lock_timestamp_{0};  // ms
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_CONSUMERPROXY_HPP_
