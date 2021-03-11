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
#include "RebalanceLitePullImpl.h"

#include "AllocateMQAveragely.h"
#include "OffsetStore.h"
#include "UtilAll.h"

namespace rocketmq {

RebalanceLitePullImpl::RebalanceLitePullImpl(DefaultLitePullConsumerImpl* consumerImpl)
    : RebalanceImpl(null, CLUSTERING, AllocateMQAveragely, nullptr), lite_pull_consumer_impl_(consumerImpl) {}

void RebalanceLitePullImpl::shutdown() {}

bool RebalanceLitePullImpl::updateMessageQueueInRebalance(const std::string& topic,
                                                          std::vector<MQMessageQueue>& allocated_mqs,
                                                          bool orderly) {
  return true;
}

bool RebalanceLitePullImpl::removeUnnecessaryMessageQueue(const MQMessageQueue& mq, ProcessQueuePtr pq) {
  lite_pull_consumer_impl_->getOffsetStore()->persist(mq);
  lite_pull_consumer_impl_->getOffsetStore()->removeOffset(mq);
  return true;
}

void RebalanceLitePullImpl::removeDirtyOffset(const MQMessageQueue& mq) {
  lite_pull_consumer_impl_->getOffsetStore()->removeOffset(mq);
}

int64_t RebalanceLitePullImpl::computePullFromWhere(const MQMessageQueue& mq) {
  return RebalanceImpl::computePullFromWhereImpl(
      mq, lite_pull_consumer_impl_->getDefaultLitePullConsumerConfig()->consume_from_where(),
      lite_pull_consumer_impl_->getDefaultLitePullConsumerConfig()->consume_timestamp(),
      *lite_pull_consumer_impl_->getOffsetStore(), *lite_pull_consumer_impl_);
}

void RebalanceLitePullImpl::messageQueueChanged(const std::string& topic,
                                                std::vector<MQMessageQueue>& mqAll,
                                                std::vector<MQMessageQueue>& mqDivided) {
  auto* messageQueueListener = lite_pull_consumer_impl_->getMessageQueueListener();
  if (messageQueueListener != nullptr) {
    try {
      messageQueueListener->messageQueueChanged(topic, mqAll, mqDivided);
    } catch (std::exception& e) {
      LOG_ERROR_NEW("messageQueueChanged exception {}", e.what());
    }
  }
}

void RebalanceLitePullImpl::truncateMessageQueueNotMyTopic() {}

std::vector<MQMessageQueue> RebalanceLitePullImpl::getAllocatedMQ() {
  std::vector<MQMessageQueue> mqs;
  return mqs;
}

}  // namespace rocketmq
