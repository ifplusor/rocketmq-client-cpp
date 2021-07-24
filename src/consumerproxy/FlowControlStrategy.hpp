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
#ifndef ROCKETMQ_CONSUMERPROXY_FLOWCONTROLSTRATEGY_HPP_
#define ROCKETMQ_CONSUMERPROXY_FLOWCONTROLSTRATEGY_HPP_

#include <iterator>  // std::make_move_iterator
#include <memory>    // std::shared_ptr
#include <mutex>     // std::mutex
#include <vector>    // std::vector

#include "consumerproxy/FlowControl.hpp"

namespace rocketmq {

template <typename QueueSet, typename Queue>
class PartialCompleteFlowControlStrategy : private FlowControlNode {
 public:
  void AfterPut(const std::shared_ptr<Queue>& queue, const FlowTracker& tracker) {
    bool partial_suppressed = !queue->MaintainLT(queue_message_count_threshold_, queue_cache_size_threshold_);

    std::unique_lock<std::mutex> suppressed_queues_lock(suppressed_queues_mutex_);
    Update(tracker);
    bool complete_suppressed = !MaintainLT(total_message_count_threshold_, total_cache_size_threshold_);

    // partial suppressed is more priority than complete suppressed
    if (partial_suppressed) {
      return;
    }
    if (complete_suppressed) {
      suppressed_queues_.push_back(queue);
      return;
    }

    suppressed_queues_lock.unlock();
    static_cast<QueueSet*>(this)->PushPullQueue(queue);
  }

  void AfterCommit(const std::shared_ptr<Queue>& queue, const FlowTracker& tracker) {
    bool resume_from_partial_suppressed =
        queue->MaintainET(queue_message_count_threshold_, queue_cache_size_threshold_);

    std::unique_lock<std::mutex> suppressed_queues_lock(suppressed_queues_mutex_);
    Update(tracker);
    bool resume_from_complete_suppressed = MaintainET(total_message_count_threshold_, total_cache_size_threshold_);

    if (resume_from_partial_suppressed && state() == FlowControlState::kSuppressed) {
      // in complete suppressed
      suppressed_queues_.push_back(queue);
      return;
    }
    if (resume_from_complete_suppressed) {
      ResumeSuppressedQueue();
    }
    if (resume_from_partial_suppressed) {
      suppressed_queues_lock.unlock();
      static_cast<QueueSet*>(this)->PushPullQueue(queue);
    }
  }

  void AfterDrop(const std::shared_ptr<Queue>& queue) {
    std::unique_lock<std::mutex> suppressed_queues_lock(suppressed_queues_mutex_);
    Release(queue->count(), queue->cache_size());
    if (MaintainET(total_message_count_threshold_, total_cache_size_threshold_)) {
      // resume from complete suppressed
      ResumeSuppressedQueue();
    }
  }

 private:
  void ResumeSuppressedQueue() {
    static_cast<QueueSet*>(this)->PushPullQueue(std::make_move_iterator(suppressed_queues_.begin()),
                                                std::make_move_iterator(suppressed_queues_.end()));
    suppressed_queues_.clear();
  }

 private:
  std::vector<std::shared_ptr<Queue>> suppressed_queues_;
  std::mutex suppressed_queues_mutex_;

  ssize_t total_message_count_threshold_{-1};
  ssize_t queue_message_count_threshold_{1000};

  ssize_t total_cache_size_threshold_{-1};
  ssize_t queue_cache_size_threshold_{100 * 1024 * 1024};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMERPROXY_FLOWCONTROLSTRATEGY_HPP_
