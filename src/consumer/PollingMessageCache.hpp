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
#ifndef ROCKETMQ_CONSUMER_POLLINGMESSAGECACHE_HPP_
#define ROCKETMQ_CONSUMER_POLLINGMESSAGECACHE_HPP_

#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <vector>

#include "MessageCache.hpp"
#include "MessageExt.h"
#include "ProcessQueue.h"
#include "UtilAll.h"
#include "time.hpp"

namespace rocketmq {

class PollingMessageCache : public MessageCache {
 public:
  void PutMessages(const ProcessQueuePtr& process_queue, const std::vector<MessageExtPtr>& messages) override {
    if (process_queue->PutMessages(messages)) {
      PushReadyQueueBack(process_queue);
    }
  }

  void ClearMessages(const ProcessQueuePtr& process_queue) override {
    RemoveFromReadyQueue(process_queue);
    process_queue->ClearAllMessages();
  }

  std::vector<MessageExtPtr> TakeMessages(int64_t timeout, int batch_size) {
    int64_t end_time = UtilAll::currentTimeMillis() + timeout;

    auto process_queue = PopReadyQueueFront(timeout);
    while (process_queue->dropped()) {
      int64_t remaining_time = end_time - UtilAll::currentTimeMillis();
      if (remaining_time > 0) {
        process_queue = PopReadyQueueFront(remaining_time);
      } else {
        break;
      }
    }

    bool remained = false;
    auto messages = TakeMessagesImpl(process_queue, batch_size, std::numeric_limits<int64_t>::max(), &remained);
    if (remained) {
      // more messages to consume
      PushReadyQueueBack(process_queue);
    }
    return messages;
  }

 private:
  void PushReadyQueueBack(const ProcessQueuePtr& process_queue) {
    std::lock_guard<std::mutex> lock(ready_queue_mutex_);
    bool is_empty = ready_queue_.empty();
    ready_queue_.push_back(process_queue);
    if (is_empty) {
      ready_queue_not_empty_event_.notify_one();
    }
  }

  ProcessQueuePtr PopReadyQueueFront(long timeout) {
    auto deadline = until_time_point(timeout, time_unit::milliseconds);
    std::unique_lock<std::mutex> lock(ready_queue_mutex_);
    if (ready_queue_.empty()) {
      ready_queue_not_empty_event_.wait_until(lock, deadline, [this] { return !ready_queue_.empty(); });
    }
    if (!ready_queue_.empty()) {
      auto process_queue = ready_queue_.front();
      ready_queue_.pop_front();
      return process_queue;
    }
    return nullptr;
  }

  bool RemoveFromReadyQueue(const ProcessQueuePtr& process_queue) {
    std::unique_lock<std::mutex> lock(ready_queue_mutex_);
    for (auto it = ready_queue_.begin(); it != ready_queue_.end(); it++) {
      if (*it == process_queue) {
        ready_queue_.erase(it);
        return true;
      }
    }
    return false;
  }

 private:
  std::mutex ready_queue_mutex_;
  std::condition_variable ready_queue_not_empty_event_;
  std::deque<ProcessQueuePtr> ready_queue_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_POLLINGMESSAGECACHE_HPP_
