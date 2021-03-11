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
#ifndef ROCKETMQ_CONSUMER_REBALANCEIMPL_H_
#define ROCKETMQ_CONSUMER_REBALANCEIMPL_H_

#include <mutex>

#include "AllocateMQStrategy.h"
#include "ConsumeType.h"
#include "MQAdmin.h"
#include "MQClientInstance.h"
#include "MQException.h"
#include "MQMessageQueue.h"
#include "OffsetStore.h"
#include "ProcessQueue.h"
#include "protocol/heartbeat/SubscriptionData.hpp"

namespace rocketmq {

typedef std::map<std::string, std::vector<MQMessageQueue>> TOPIC2MQS;
typedef std::map<std::string, SubscriptionData*> TOPIC2SD;
typedef std::map<std::string, std::vector<MQMessageQueue>> BROKER2MQS;

class RebalanceImpl {
 public:
  RebalanceImpl(const std::string& consumerGroup,
                MessageModel messageModel,
                const AllocateMQStrategy& allocateMqStrategy,
                MQClientInstance* clientInstance);
  virtual ~RebalanceImpl();

 public:
  virtual void shutdown(){};
  virtual ConsumeType consumeType() = 0;
  virtual std::vector<MQMessageQueue> getAllocatedMQ() = 0;

 public:
  void doRebalance(bool orderly = false);

 private:
  void rebalanceByTopic(const std::string& topic, bool orderly);

 protected:
  virtual bool updateMessageQueueInRebalance(const std::string& topic,
                                             std::vector<MQMessageQueue>& allocated_mqs,
                                             bool orderly) = 0;
  virtual void messageQueueChanged(const std::string& topic,
                                   std::vector<MQMessageQueue>& all_mqs,
                                   std::vector<MQMessageQueue>& allocated_mqs) = 0;
  virtual void truncateMessageQueueNotMyTopic() = 0;

 protected:
  int64_t computePullFromWhereImpl(const MQMessageQueue& mq,
                                   ConsumeFromWhere consume_from_where,
                                   const std::string& consume_timestamp,
                                   OffsetStore& offset_store,
                                   MQAdmin& admin);

 public:
  TOPIC2SD& getSubscriptionInner();
  SubscriptionData* getSubscriptionData(const std::string& topic);
  void setSubscriptionData(const std::string& topic, SubscriptionData* sd) noexcept;

  bool getTopicSubscribeInfo(const std::string& topic, std::vector<MQMessageQueue>& mqs);
  void setTopicSubscribeInfo(const std::string& topic, std::vector<MQMessageQueue>& mqs);

 public:
  inline void set_consumer_group(const std::string& groupname) { consumer_group_ = groupname; }
  inline void set_message_model(MessageModel messageModel) { message_model_ = messageModel; }
  inline void set_allocate_mq_strategy(const AllocateMQStrategy& allocateMqStrategy) {
    allocate_mq_strategy_ = allocateMqStrategy;
  }
  inline void set_client_instance(MQClientInstance* instance) { client_instance_ = instance; }

 protected:
  TOPIC2MQS topic_subscribe_info_table_;
  std::mutex topic_subscribe_info_table_mutex_;

  TOPIC2SD subscription_inner_;  // don't modify subscription_inner_ after the consumer started.

  std::string consumer_group_;
  MessageModel message_model_;

  AllocateMQStrategy allocate_mq_strategy_;

  MQClientInstance* client_instance_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_REBALANCEIMPL_H_
