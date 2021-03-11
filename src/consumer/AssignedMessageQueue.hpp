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
#ifndef ROCKETMQ_CONSUMER_ASSIGNEDMESSAGEQUEUE_H_
#define ROCKETMQ_CONSUMER_ASSIGNEDMESSAGEQUEUE_H_

#include <algorithm>  // std::move, std::binary_search
#include <memory>
#include <mutex>  // std::mutex

#include "MQMessageQueue.h"
#include "ProcessQueue.h"
#include "RebalanceLitePullImpl.h"

namespace rocketmq {

class AssignedMessageQueue {
 public:
  std::vector<MQMessageQueue> messageQueues() {
    std::vector<MQMessageQueue> mqs;
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    for (const auto& it : assigned_message_queue_state_) {
      mqs.push_back(it.first);
    }
    return mqs;
  }

  bool isPaused(const MQMessageQueue& message_queue) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      auto& pq = it->second;
      return pq->paused();
    }
    return true;
  }

  void pause(const std::vector<MQMessageQueue>& message_queues) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    for (const auto& message_queue : message_queues) {
      auto it = assigned_message_queue_state_.find(message_queue);
      if (it != assigned_message_queue_state_.end()) {
        auto& pq = it->second;
        pq->set_paused(true);
      }
    }
  }

  void resume(const std::vector<MQMessageQueue>& message_queues) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    for (const auto& message_queue : message_queues) {
      auto it = assigned_message_queue_state_.find(message_queue);
      if (it != assigned_message_queue_state_.end()) {
        auto& pq = it->second;
        pq->set_paused(false);
      }
    }
  }

  ProcessQueuePtr getProcessQueue(const MQMessageQueue& message_queue) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      return it->second;
    }
    return nullptr;
  }

  int64_t getPullOffset(const MQMessageQueue& message_queue) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      auto& pq = it->second;
      return pq->pull_offset();
    }
    return -1;
  }

  void updatePullOffset(const MQMessageQueue& message_queue, int64_t offset) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      auto& pq = it->second;
      return pq->set_pull_offset(offset);
    }
  }

  int64_t getConsumerOffset(const MQMessageQueue& message_queue) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      auto& pq = it->second;
      return pq->consume_offset();
    }
    return -1;
  }

  void updateConsumeOffset(const MQMessageQueue& message_queue, int64_t offset) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      auto& pq = it->second;
      return pq->set_consume_offset(offset);
    }
  }

  int64_t getSeekOffset(const MQMessageQueue& message_queue) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      auto& pq = it->second;
      return pq->seek_offset();
    }
    return -1;
  }

  void setSeekOffset(const MQMessageQueue& message_queue, int64_t offset) {
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    auto it = assigned_message_queue_state_.find(message_queue);
    if (it != assigned_message_queue_state_.end()) {
      auto& pq = it->second;
      return pq->set_seek_offset(offset);
    }
  }

  std::vector<PullRequestPtr> updateAssignedMessageQueue(const std::string& topic,
                                                         std::vector<MQMessageQueue>& assigned) {
    std::sort(assigned.begin(), assigned.end());
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    for (auto it = assigned_message_queue_state_.begin(); it != assigned_message_queue_state_.end();) {
      auto& mq = it->first;
      if (mq.topic() == topic) {
        if (!std::binary_search(assigned.begin(), assigned.end(), mq)) {
          auto& pq = it->second;
          pq->set_dropped(true);
          if (rebalance_impl_ != nullptr) {
            rebalance_impl_->removeUnnecessaryMessageQueue(mq, pq);
          }
          it = assigned_message_queue_state_.erase(it);
          continue;
        }
      }
      it++;
    }
    return addAssignedMessageQueue(assigned);
  }

  std::vector<PullRequestPtr> updateAssignedMessageQueue(std::vector<MQMessageQueue>& assigned) {
    std::sort(assigned.begin(), assigned.end());
    std::lock_guard<std::mutex> lock(assigned_message_queue_state_mutex_);
    for (auto it = assigned_message_queue_state_.begin(); it != assigned_message_queue_state_.end();) {
      auto& mq = it->first;
      if (!std::binary_search(assigned.begin(), assigned.end(), mq)) {
        auto& pq = it->second;
        pq->set_dropped(true);
        if (rebalance_impl_ != nullptr) {
          rebalance_impl_->removeUnnecessaryMessageQueue(mq, pq);
        }
        it = assigned_message_queue_state_.erase(it);
        continue;
      }
      it++;
    }
    return addAssignedMessageQueue(assigned);
  }

 private:
  std::vector<PullRequestPtr> addAssignedMessageQueue(const std::vector<MQMessageQueue>& assigned) {
    std::vector<PullRequestPtr> pull_request_list;
    for (const auto& message_queue : assigned) {
      if (assigned_message_queue_state_.find(message_queue) == assigned_message_queue_state_.end()) {
        ProcessQueuePtr process_queue = std::make_shared<ProcessQueue>(message_queue);
        if (rebalance_impl_ != nullptr) {
          rebalance_impl_->removeDirtyOffset(message_queue);
        }
        assigned_message_queue_state_.emplace(message_queue, process_queue);

        auto pull_request = std::make_shared<PullRequest>();
        pull_request->set_message_queue(message_queue);
        pull_request->set_process_queue(process_queue);
        pull_request_list.push_back(std::move(pull_request));
      }
    }
    return pull_request_list;
  }

 public:
  inline void set_rebalance_impl(RebalanceLitePullImpl* rebalance_impl) { rebalance_impl_ = rebalance_impl; }

 private:
  std::map<MQMessageQueue, ProcessQueuePtr> assigned_message_queue_state_;
  std::mutex assigned_message_queue_state_mutex_;

  RebalanceLitePullImpl* rebalance_impl_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_ASSIGNEDMESSAGEQUEUE_H_
