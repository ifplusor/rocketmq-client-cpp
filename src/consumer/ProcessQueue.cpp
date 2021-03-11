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

#include "Logging.h"
#include "MQMessageQueue.h"
#include "UtilAll.h"
#include "protocol/body/ProcessQueueInfo.hpp"

static const uint64_t PULL_MAX_IDLE_TIME = 120000;  // ms

namespace rocketmq {

const uint64_t ProcessQueue::REBALANCE_LOCK_MAX_LIVE_TIME = 30000;
const uint64_t ProcessQueue::REBALANCE_LOCK_INTERVAL = 20000;

ProcessQueue::ProcessQueue(const MQMessageQueue& message_queue)
    : message_queue_(message_queue),
      last_pull_timestamp_(UtilAll::currentTimeMillis()),
      last_consume_timestamp_(UtilAll::currentTimeMillis()),
      last_lock_timestamp_(UtilAll::currentTimeMillis()) {}

ProcessQueue::~ProcessQueue() {
  message_cache_.clear();
  consuming_message_cache_.clear();
}

bool ProcessQueue::isLockExpired() const {
  return (UtilAll::currentTimeMillis() - last_lock_timestamp_) > REBALANCE_LOCK_MAX_LIVE_TIME;
}

bool ProcessQueue::isPullExpired() const {
  return (UtilAll::currentTimeMillis() - last_pull_timestamp_) > PULL_MAX_IDLE_TIME;
}

void ProcessQueue::putMessage(const std::vector<MessageExtPtr>& msgs) {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);

  for (const auto& msg : msgs) {
    int64_t offset = msg->queue_offset();
    message_cache_[offset] = msg;
    if (offset > queue_offset_max_) {
      queue_offset_max_ = offset;
    }
  }

  LOG_DEBUG_NEW("ProcessQueue: putMessage queue_offset_max:{}", queue_offset_max_);
}

int64_t ProcessQueue::removeMessage(const std::vector<MessageExtPtr>& msgs) {
  int64_t result = -1;
  const auto now = UtilAll::currentTimeMillis();

  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  last_consume_timestamp_ = now;

  if (!message_cache_.empty()) {
    result = queue_offset_max_ + 1;
    LOG_DEBUG_NEW("offset result is:{}, queue_offset_max is:{}, msgs size:{}", result, queue_offset_max_, msgs.size());

    for (auto& msg : msgs) {
      LOG_DEBUG_NEW("remove these msg from msg_tree_map, its offset:{}", msg->queue_offset());
      message_cache_.erase(msg->queue_offset());
    }

    if (!message_cache_.empty()) {
      auto it = message_cache_.begin();
      result = it->first;
    }
  }

  return result;
}

int ProcessQueue::getCacheMsgCount() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  return static_cast<int>(message_cache_.size() + consuming_message_cache_.size());
}

int64_t ProcessQueue::getCacheMinOffset() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  if (message_cache_.empty() && consuming_message_cache_.empty()) {
    return 0;
  } else if (!consuming_message_cache_.empty()) {
    return consuming_message_cache_.begin()->first;
  } else {
    return message_cache_.begin()->first;
  }
}

int64_t ProcessQueue::getCacheMaxOffset() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  return queue_offset_max_;
}

int64_t ProcessQueue::commit() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  if (!consuming_message_cache_.empty()) {
    int64_t offset = (--consuming_message_cache_.end())->first;
    consuming_message_cache_.clear();
    return offset + 1;
  } else {
    return -1;
  }
}

void ProcessQueue::makeMessageToCosumeAgain(std::vector<MessageExtPtr>& msgs) {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  for (const auto& msg : msgs) {
    message_cache_[msg->queue_offset()] = msg;
    consuming_message_cache_.erase(msg->queue_offset());
  }
}

void ProcessQueue::takeMessages(std::vector<MessageExtPtr>& out_msgs, int batchSize) {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);
  for (auto it = message_cache_.begin(); it != message_cache_.end() && batchSize--;) {
    out_msgs.push_back(it->second);
    consuming_message_cache_[it->first] = it->second;
    it = message_cache_.erase(it);
  }
}

void ProcessQueue::clearAllMsgs() {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);

  if (dropped()) {
    LOG_DEBUG_NEW("clear msg_tree_map as PullRequest had been dropped.");
    message_cache_.clear();
    consuming_message_cache_.clear();
    queue_offset_max_ = 0;
  }
}

void ProcessQueue::fillProcessQueueInfo(ProcessQueueInfo& info) {
  std::lock_guard<std::mutex> lock(message_cache_mutex_);

  if (!message_cache_.empty()) {
    info.cachedMsgMinOffset = message_cache_.begin()->first;
    info.cachedMsgMaxOffset = queue_offset_max_;
    info.cachedMsgCount = message_cache_.size();
  }

  if (!consuming_message_cache_.empty()) {
    info.transactionMsgMinOffset = consuming_message_cache_.begin()->first;
    info.transactionMsgMaxOffset = (--consuming_message_cache_.end())->first;
    info.transactionMsgCount = consuming_message_cache_.size();
  }

  info.setLocked(locked_);
  info.tryUnlockTimes = try_unlock_times_.load();
  info.lastLockTimestamp = last_lock_timestamp_;

  info.setDroped(dropped_);
  info.lastPullTimestamp = last_pull_timestamp_;
  info.lastConsumeTimestamp = last_consume_timestamp_;
}

}  // namespace rocketmq
