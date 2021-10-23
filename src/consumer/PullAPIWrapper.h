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
#ifndef ROCKETMQ_CONSUMER_PULLAPIWRAPPER_H_
#define ROCKETMQ_CONSUMER_PULLAPIWRAPPER_H_

#include <mutex>  // std::mutex

#include "CommunicationMode.h"
#include "MQClientInstance.h"
#include "MessageQueue.hpp"
#include "PullResultExt.hpp"
#include "common/ResultState.hpp"
#include "protocol/body/SubscriptionData.hpp"

namespace rocketmq {

class PullAPIWrapper {
 public:
  using PullCallback = std::function<void(ResultState<std::unique_ptr<PullResultExt>>) /* noexcept */>;

 public:
  PullAPIWrapper(MQClientInstance* client_instance, std::string consumer_group);

  std::unique_ptr<PullResultExt> PullKernelImpl(const MessageQueue& message_queue,
                                                const std::string& expression,
                                                const std::string& expression_type,
                                                int64_t version,
                                                int64_t offset,
                                                int max_nums,
                                                int system_flag,
                                                int64_t commit_offset,
                                                int broker_suspend_max_time_millis,
                                                int timeout_millis,
                                                CommunicationMode communication_mode,
                                                PullCallback pull_callback);

  std::unique_ptr<PullResult> ProcessPullResult(const MessageQueue& message_queue,
                                                const std::unique_ptr<PullResultExt>& pull_result_ext,
                                                const SubscriptionData* subscription_data);

 private:
  int RecalculatePullFromWhichNode(const MessageQueue& message_queue);

  void UpdatePullFromWhichNode(const MessageQueue& message_queue, int broker_id);

 private:
  MQClientInstance* client_instance_;
  std::string consumer_group_;
  std::mutex lock_;
  std::map<MessageQueue, int /* brokerId */> pull_from_which_node_table_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_PULLAPIWRAPPER_H_
