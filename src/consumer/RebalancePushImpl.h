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
#ifndef ROCKETMQ_CONSUMER_REBALANCEPUSHIMPL_H_
#define ROCKETMQ_CONSUMER_REBALANCEPUSHIMPL_H_

#include "DefaultMQPushConsumerImpl.h"
#include "MessageQueue.hpp"
#include "PullRequest.h"
#include "RebalanceImpl.h"

namespace rocketmq {

using MQ2PQ = std::map<MessageQueue, ProcessQueuePtr>;

class RebalancePushImpl : public RebalanceImpl {
 public:
  RebalancePushImpl(DefaultMQPushConsumerImpl* consumerImpl);

 public:
  bool Lock(const MessageQueue& mq);
  void LockAll();

  void Unlock(const MessageQueue& mq, bool oneway = false);
  void UnlockAll(bool oneway = false);

 private:
  std::shared_ptr<BROKER2MQS> buildProcessQueueTableByBrokerName();

 public:
  void shutdown() override;
  ConsumeType consumeType() override final { return CONSUME_PASSIVELY; }

 protected:
  bool updateMessageQueueInRebalance(const std::string& topic,
                                     std::vector<MessageQueue>& allocated_mqs,
                                     bool orderly) override;

 private:
  bool updateProcessQueueTableInRebalance(const std::string& topic,
                                          std::vector<MessageQueue>& allocated_mqs,
                                          bool orderly);

 public:
  bool removeUnnecessaryMessageQueue(const MessageQueue& mq, ProcessQueuePtr pq);
  void removeDirtyOffset(const MessageQueue& mq);
  int64_t computePullFromWhere(const MessageQueue& mq);
  std::vector<MessageQueue> getAllocatedMQ();

 protected:
  void messageQueueChanged(const std::string& topic,
                           std::vector<MessageQueue>& all_mqs,
                           std::vector<MessageQueue>& allocated_mqs) override;
  void truncateMessageQueueNotMyTopic() override;

 public:
  void removeProcessQueue(const MessageQueue& mq);
  ProcessQueuePtr removeProcessQueueDirectly(const MessageQueue& mq);
  ProcessQueuePtr putProcessQueueIfAbsent(const MessageQueue& mq, ProcessQueuePtr pq);
  ProcessQueuePtr getProcessQueue(const MessageQueue& mq);
  MQ2PQ getProcessQueueTable();

 private:
  MQ2PQ process_queue_table_;
  std::mutex process_queue_table_mutex_;

  DefaultMQPushConsumerImpl* default_mq_push_consumer_impl_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_REBALANCEPUSHIMPL_H_
