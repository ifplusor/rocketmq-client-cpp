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
#ifndef ROCKETMQ_CONSUMER_LOADBALANCER_H_
#define ROCKETMQ_CONSUMER_LOADBALANCER_H_

#include <map>     // std::map
#include <mutex>   // std::mutex
#include <string>  // std::string
#include <vector>  // std::vector

#include <AllocateMQStrategy.h>

namespace rocketmq {

class MessageQueue;
class LogicalQueue;

enum class LoadBalanceMode { kNone, kAutomatic, kManual };

class LoadBalancer {
 public:
  using RebalanceCallback = std::function<void(std::vector<MessageQueue>)>;

  LoadBalancer(RebalanceCallback rebalance_callback) : rebalance_callback_(std::move(rebalance_callback)) {}

  ~LoadBalancer() = default;

  // disable copy
  LoadBalancer(const LoadBalancer&) = delete;
  LoadBalancer& operator=(const LoadBalancer&) = delete;

  // disable copy
  LoadBalancer(LoadBalancer&&) = delete;
  LoadBalancer& operator=(LoadBalancer&&) = delete;

  void Rebalance();

 private:
  void DoRebalance(const std::string& topic);

  void AfterRebalance();

  std::vector<MessageQueue> GetAllocatedMessageQueues();

 private:
  AllocateMQStrategy allocate_mq_strategy_;
  RebalanceCallback rebalance_callback_;

  std::map<std::string, std::vector<MessageQueue>> allocated_message_queues_table_;
  std::mutex allocated_message_queues_table_mutex_;

  LoadBalanceMode mode_{LoadBalanceMode::kNone};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_LOADBALANCER_H_
