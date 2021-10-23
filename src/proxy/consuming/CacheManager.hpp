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
#ifndef ROCKETMQ_PROXY_CONSUMING_CACHEMANAGER_HPP_
#define ROCKETMQ_PROXY_CONSUMING_CACHEMANAGER_HPP_

#include <memory>  // std::shared_ptr
#include <mutex>   // std::mutex
#include <vector>  // std::vector

#include "proxy/consuming/FlowControl.hpp"

namespace rocketmq {
namespace detail {

template <typename QueueSet, typename FlowControlStrategy>
class CacheManager {
 public:
  using Queue = typename QueueSet::QueueType;
  using Message = typename QueueSet::MessageType;

  void Put(std::shared_ptr<Queue> queue, std::vector<Message> messages) {
    std::unique_lock<std::mutex> queue_lock(queue->mutex());
    if (queue->dropped()) {
      return;
    }

    if (messages.empty()) {
      queue_set_->OnQueuePullable(std::move(queue));
      return;
    }

    FlowTracker tracker(*queue);
    if (queue->Put(messages.begin(), messages.end())) {
      // no message before, consumable now
      queue_set_->OnQueueConsumable(queue);
    }

    // flow control
    flow_control_strategy_->AfterPut(queue, tracker);
  }

  void Clear(std::shared_ptr<Queue> queue) {
    // flow control
    flow_control_strategy_->AfterDrop(queue);
  }

 private:
  FlowControlStrategy* flow_control_strategy_;

  QueueSet* queue_set_;
};

}  // namespace detail
}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_CACHEMANAGER_HPP_
