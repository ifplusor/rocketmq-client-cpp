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
#include "PullRequest.h"
#include "RebalanceImpl.h"

namespace rocketmq {

typedef std::map<MQMessageQueue, ProcessQueuePtr> MQ2PQ;

class RebalancePushImpl : public RebalanceImpl {
 public:
  RebalancePushImpl(DefaultMQPushConsumerImpl* consumerImpl);

 public:
  bool lock(const MQMessageQueue& mq);
  void lockAll();

  void unlock(const MQMessageQueue& mq, const bool oneway = false);
  void unlockAll(const bool oneway = false);

 private:
  std::shared_ptr<BROKER2MQS> buildProcessQueueTableByBrokerName();

 public:
  void shutdown() override;
  ConsumeType consumeType() override final { return CONSUME_PASSIVELY; }
  std::vector<MQMessageQueue> getAllocatedMQ() override;

 protected:
  bool updateMessageQueueInRebalance(const std::string& topic,
                                     std::vector<MQMessageQueue>& allocated_mqs,
                                     const bool orderly) override;

 private:
  bool updateProcessQueueTableInRebalance(const std::string& topic,
                                          std::vector<MQMessageQueue>& mqSet,
                                          const bool isOrder);

 public:
  bool removeUnnecessaryMessageQueue(const MQMessageQueue& mq, ProcessQueuePtr pq);
  void removeDirtyOffset(const MQMessageQueue& mq);
  int64_t computePullFromWhere(const MQMessageQueue& mq);

 private:
  void dispatchPullRequest(const std::vector<PullRequestPtr>& pullRequestList);

 protected:
  void messageQueueChanged(const std::string& topic,
                           std::vector<MQMessageQueue>& all_mqs,
                           std::vector<MQMessageQueue>& allocated_mqs) override;
  void truncateMessageQueueNotMyTopic() override;

 public:
  void removeProcessQueue(const MQMessageQueue& mq);
  ProcessQueuePtr removeProcessQueueDirectly(const MQMessageQueue& mq);
  ProcessQueuePtr putProcessQueueIfAbsent(const MQMessageQueue& mq, ProcessQueuePtr pq);
  ProcessQueuePtr getProcessQueue(const MQMessageQueue& mq);
  MQ2PQ getProcessQueueTable();

 private:
  MQ2PQ process_queue_table_;
  std::mutex process_queue_table_mutex_;

  DefaultMQPushConsumerImpl* default_mq_push_consumer_impl_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_REBALANCEPUSHIMPL_H_
