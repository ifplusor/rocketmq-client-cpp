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
#ifndef ROCKETMQ_PROXY_CONSUMING_ASSIGNEDQUEUESET_HPP_
#define ROCKETMQ_PROXY_CONSUMING_ASSIGNEDQUEUESET_HPP_

#include <map>     // std::map
#include <mutex>   // std::lock_guard, std::mutex
#include <vector>  // std::vector

namespace rocketmq {

template <typename QueueSet, typename PullStrategy, typename FlowControlStrategy>
class AssignedQueueSet {
 public:
  using QueueType = typename QueueSet::Queue;
  using IdentityType = typename QueueType::IdentityType;
  using PullStrategyType = PullStrategy;
  using FlowControlStrategyType = FlowControlStrategy;

  using QueueChangHook = std::function<bool(const std::shared_ptr<QueueType>&)>;

  void Assign(std::vector<IdentityType> assigned,
              const QueueChangHook& before_insert,
              const QueueChangHook& before_remove) {
    std::lock_guard<std::mutex> assigned_queues_lock(assigned_queues_mutex_);
    Update(Diff(std::move(assigned)), before_insert, before_remove);
  }

  std::shared_ptr<QueueType> At(const IdentityType& identity) {
    std::lock_guard<std::mutex> lock(assigned_queues_mutex_);
    return assigned_queues_.at(identity);
  }

 private:
  std::tuple<std::vector<IdentityType>, std::vector<IdentityType>> Diff(std::vector<IdentityType> assigned) {
    std::sort(assigned.begin(), assigned.end());

    // remove expried queue
    std::vector<IdentityType> removed_queues;
    for (auto it = assigned_queues_.begin(); it != assigned_queues_.end();) {
      const auto& identity = it->first;
      if (!std::binary_search(assigned.begin(), assigned.end(), identity)) {
        removed_queues.push_back(identity);
      }
    }

    // add new queue
    std::vector<IdentityType> added_queues;
    for (auto it = assigned.begin(); it != assigned.end();) {
      auto& identity = *it;
      if (assigned_queues_.find(identity) == assigned_queues_.end()) {
        added_queues.push_back(std::move(identity));
        it = assigned_queues_.erase(it);
      } else {
        ++it;
      }
    }

    return std::make_tuple(std::move(removed_queues), std::move(added_queues));
  }

  void Update(std::tuple<std::vector<IdentityType>, std::vector<IdentityType>> removed_and_added,
              const QueueChangHook& before_insert,
              const QueueChangHook& before_remove) {
    // remove expried queue
    const auto& removed_queues = std::get<0>(removed_and_added);
    for (const auto& identity : removed_queues) {
      // TODO: optimize flow control
      Erase(identity, before_remove);
    }

    // add new queue
    const auto& added_queues = std::get<1>(removed_and_added);
    for (const auto& identity : added_queues) {
      Insert(identity, before_insert);
    }
  }

  void Insert(IdentityType identity, const QueueChangHook& before_insert) {
    auto it = assigned_queues_.find(identity);
    if (it != assigned_queues_.end()) {
      return;
    }

    auto queue = std::make_shared<QueueType>(identity);
    if (!before_insert || before_insert(queue)) {
      assigned_queues_.emplace(std::move(identity), queue);
      queue_set_->OnQueueAdded(std::move(queue));
    }
  }

  void Erase(const IdentityType& identity, const QueueChangHook& before_erase) {
    auto it = assigned_queues_.find(identity);
    if (it == assigned_queues_.end()) {
      return;
    }

    auto queue = std::move(it.second);
    std::unique_lock<std::mutex> queue_lock(queue->mutex());
    queue->Drop();

    if (!before_erase || before_erase(queue)) {
      assigned_queues_.erase(it);
      queue_set_->OnQueueRemoved(std::move(queue));
    }
  }

 private:
  QueueSet queue_set_;

  std::map<IdentityType, std::shared_ptr<QueueType>> assigned_queues_;
  std::mutex assigned_queues_mutex_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_ASSIGNEDQUEUESET_HPP_
