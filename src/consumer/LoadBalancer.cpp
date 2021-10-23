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
#include "consumer/LoadBalancer.hpp"

#include <mutex>   // std::lock_guard, std::mutex
#include <string>  // std::string
#include <vector>  // std::vector

#include "MessageQueue.hpp"
#include "utility/MapAccessor.hpp"

namespace rocketmq {

void LoadBalancer::Rebalance() {
  if (mode_ == LoadBalanceMode::kAutomatic) {
    // for subscription
  }

  AfterRebalance();
}

void LoadBalancer::DoRebalance(const std::string& topic) {
  std::string client_id;

  std::vector<MessageQueue> all_mqs;
  std::vector<std::string> all_cids;

  std::vector<MessageQueue> allocated_mqs = allocate_mq_strategy_(client_id, all_mqs, all_cids);

  MapAccessor::InsertOrAssign(allocated_message_queues_table_, topic, allocated_mqs,
                              allocated_message_queues_table_mutex_);
}

void LoadBalancer::AfterRebalance() {
  if (rebalance_callback_) {
    rebalance_callback_(GetAllocatedMessageQueues());
  }
}

std::vector<MessageQueue> LoadBalancer::GetAllocatedMessageQueues() {
  std::vector<MessageQueue> allocated_message_queues;

  std::lock_guard<std::mutex> lock(allocated_message_queues_table_mutex_);
  for (const auto& it : allocated_message_queues_table_) {
    const auto& message_queues = it.second;
    allocated_message_queues.insert(allocated_message_queues.end(), message_queues.begin(), message_queues.end());
  }

  return allocated_message_queues;
}

// void LoadBalancer::Update() {
//   bool changed = false;
//   LogicalQueueSet queue_set;

//   queue_set.Assign(
//       allocated_mqs,
//       /* before_insert */
//       [this, orderly, &changed](const std::shared_ptr<LogicalQueue>& queue) -> bool {
//         const auto& message_queue = queue->identity();
//         if (orderly && !Lock(message_queue)) {
//           LOG_WARN_NEW("doRebalance, {}, add a new mq failed, {}, because lock failed", consumer_group(),
//                        message_queue.ToString());
//           return false;
//         }

//         removeDirtyOffset(message_queue);

//         int64_t nextOffset = computePullFromWhere(message_queue);
//         if (nextOffset < 0) {
//           LOG_WARN_NEW("doRebalance, {}, add new mq failed, {}", consumer_group(), message_queue.ToString());
//           return false;
//         }

//         queue->set_pull_offset(nextOffset);
//         changed = true;
//         LOG_INFO_NEW("doRebalance, {}, add a new mq, {}", consumer_group(), message_queue.ToString());
//         return true;
//       },
//       /* before_remove */
//       [this, &changed](const std::shared_ptr<LogicalQueue>& queue) -> bool {
//         bool can_remove = RemoveUnnecessaryMessageQueue(queue);
//         if (can_remove) {
//           changed = true;
//           LOG_INFO_NEW("doRebalance, {}, remove unnecessary mq, {}", consumer_group(), queue->identity().ToString());
//         }
//         return can_remove;
//       });

//   return changed;
// }

// bool LoadBalancer::RemoveUnnecessaryMessageQueue(const std::shared_ptr<LogicalQueue>& queue) {
//   const auto& message_queue = queue->identity();
//   auto* offset_store = default_mq_push_consumer_impl_->offset_store();

//   offset_store->persist(message_queue);
//   offset_store->removeOffset(message_queue);

//   if (default_mq_push_consumer_impl_->consume_orderly() &&
//       CLUSTERING == default_mq_push_consumer_impl_->messageModel()) {
//     try {
//       if (UtilAll::try_lock_for(queue->consume_mutex(), 1000)) {
//         std::lock_guard<std::timed_mutex> lock(queue->consume_mutex(), std::adopt_lock);
//         // TODO: unlockDelay
//         Unlock(message_queue);
//         return true;
//       }

//       LOG_WARN_NEW("[WRONG] mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
//                    message_queue.ToString(), queue->try_unlock_times());
//       queue->inc_try_unlock_times();
//     } catch (const std::exception& e) {
//       LOG_ERROR_NEW("removeUnnecessaryMessageQueue Exception: {}", e.what());
//     }

//     return false;
//   }

//   return true;
// }

}  // namespace rocketmq
