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
#include "ProcessQueue.h"

#include <utility>  // std::move

#include "Logging.h"
#include "MessageQueue.hpp"
#include "UtilAll.h"
#include "protocol/body/ProcessQueueInfo.hpp"

namespace {

const uint64_t kRebalanceLockMaxLiveTime = 30000;  // ms
const uint64_t kPullMaxIdleTime = 120000;          // ms

}  // namespace

namespace rocketmq {

ProcessQueue::ProcessQueue(MessageQueue message_queue)
    : message_queue_(std::move(message_queue)),
      last_pull_timestamp_(UtilAll::currentTimeMillis()),
      last_consume_timestamp_(UtilAll::currentTimeMillis()),
      last_lock_timestamp_(UtilAll::currentTimeMillis()) {}

bool ProcessQueue::PutMessages(const std::vector<MessageExtPtr>& messages) {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);

  bool is_empty = message_cache_.empty();
  for (const auto& message : messages) {
    int64_t offset = message->queue_offset();
    message_cache_[offset] = message;
    if (offset > queue_offset_max_) {
      queue_offset_max_ = offset;
    }
  }

  LOG_DEBUG_NEW("ProcessQueue: putMessages queue_offset_max:{}", queue_offset_max_);

  return is_empty;
}

std::vector<MessageExtPtr> ProcessQueue::TakeMessages(int batch_size,
                                                      bool need_commit,
                                                      int64_t offset_limit,
                                                      int64_t& next_offset,
                                                      bool* remained) {
  std::vector<MessageExtPtr> messages;

  std::lock_guard<std::mutex> lock(message_cache_mutex_);

  for (auto it = message_cache_.begin(); it != message_cache_.end() && (batch_size--) > 0;) {
    if (it->first >= offset_limit) {
      break;
    }
    messages.push_back(it->second);
    if (need_commit) {
      consuming_message_cache_[it->first] = it->second;
    }
    it = message_cache_.erase(it);
  }

  if (!message_cache_.empty()) {
    auto it = message_cache_.begin();
    next_offset = it->first;
    if (remained != nullptr) {
      *remained = true;
    }
  } else {
    next_offset = queue_offset_max_ + 1;
    if (remained != nullptr) {
      *remained = false;
    }
  }

  return messages;
}

void ProcessQueue::MakeMessagesToCosumeAgain(std::vector<MessageExtPtr>& messages) {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  for (const auto& message : messages) {
    message_cache_[message->queue_offset()] = message;
    consuming_message_cache_.erase(message->queue_offset());
  }
}

int64_t ProcessQueue::Commit() {
  const auto now = UtilAll::currentTimeMillis();

  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  last_consume_timestamp_ = now;

  if (!consuming_message_cache_.empty()) {
    int64_t offset = (--consuming_message_cache_.end())->first;
    consuming_message_cache_.clear();
    return offset + 1;
  } else {
    return -1;
  }
}

int64_t ProcessQueue::Commit(const std::vector<MessageExtPtr>& message) {
  const auto now = UtilAll::currentTimeMillis();

  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  last_consume_timestamp_ = now;

  if (!consuming_message_cache_.empty()) {
    for (auto& message : message) {
      LOG_DEBUG_NEW("remove these msg from consuming_message_cache, its offset:{}", message->queue_offset());
      consuming_message_cache_.erase(message->queue_offset());
    }

    int64_t result = -1;
    if (!consuming_message_cache_.empty()) {
      const auto& it = consuming_message_cache_.begin();
      result = it->first;
    } else if (!message_cache_.empty()) {
      const auto& it = message_cache_.begin();
      result = it->first;
    } else {
      result = queue_offset_max_ + 1;
    }

    LOG_DEBUG_NEW("offset result is:{}, queue_offset_max is:{}, msgs size:{}", result, queue_offset_max_,
                  message.size());
    return result;
  }

  return -1;
}

int64_t ProcessQueue::RemoveMessages(const std::vector<MessageExtPtr>& messages) {
  const auto now = UtilAll::currentTimeMillis();

  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  last_consume_timestamp_ = now;

  if (!message_cache_.empty()) {
    for (const auto& message : messages) {
      LOG_DEBUG_NEW("remove these msg from msg_tree_map, its offset:{}", message->queue_offset());
      message_cache_.erase(message->queue_offset());
    }

    int64_t result = -1;
    if (!message_cache_.empty()) {
      auto it = message_cache_.begin();
      result = it->first;
    } else {
      result = queue_offset_max_ + 1;
    }

    LOG_DEBUG_NEW("offset result is:{}, queue_offset_max is:{}, msgs size:{}", result, queue_offset_max_,
                  messages.size());
    return result;
  }

  return -1;
}

void ProcessQueue::ClearAllMessages() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);

  if (dropped()) {
    LOG_DEBUG_NEW("clear msg_tree_map as PullRequest had been dropped.");
    message_cache_.clear();
    consuming_message_cache_.clear();
    queue_offset_max_ = 0;
  }
}

int ProcessQueue::GetCachedMessagesCount() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  return static_cast<int>(message_cache_.size() + consuming_message_cache_.size());
}

int64_t ProcessQueue::GetCachedMinOffset() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  if (message_cache_.empty() && consuming_message_cache_.empty()) {
    return 0;
  } else if (!consuming_message_cache_.empty()) {
    return consuming_message_cache_.begin()->first;
  } else {
    return message_cache_.begin()->first;
  }
}

int64_t ProcessQueue::GetCachedMaxOffset() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  return queue_offset_max_;
}

bool ProcessQueue::IsLockExpired() const {
  return (UtilAll::currentTimeMillis() - last_lock_timestamp_) > kRebalanceLockMaxLiveTime;
}

bool ProcessQueue::IsPullExpired() const {
  return (UtilAll::currentTimeMillis() - last_pull_timestamp_) > kPullMaxIdleTime;
}

void ProcessQueue::FillProcessQueueInfo(ProcessQueueInfo& info) {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);

  if (!message_cache_.empty()) {
    info.cached_message_min_offset = message_cache_.begin()->first;
    info.cached_message_max_offset = queue_offset_max_;
    info.cached_message_count = message_cache_.size();
  }

  if (!consuming_message_cache_.empty()) {
    info.transaction_message_min_offset = consuming_message_cache_.begin()->first;
    info.transaction_message_max_offset = (--consuming_message_cache_.end())->first;
    info.transaction_message_count = consuming_message_cache_.size();
  }

  info.locked = locked_.load();
  info.try_unlock_times = try_unlock_times_.load();
  info.last_lock_timestamp = last_lock_timestamp_;

  info.droped = dropped_.load();
  info.last_pull_timestamp = last_pull_timestamp_;
  info.last_consume_timestamp = last_consume_timestamp_;
}

}  // namespace rocketmq
