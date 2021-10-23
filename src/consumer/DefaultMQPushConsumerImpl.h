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
#ifndef ROCKETMQ_CONSUMER_DEFAULTMQPUSHCONSUMERIMPL_H_
#define ROCKETMQ_CONSUMER_DEFAULTMQPUSHCONSUMERIMPL_H_

#include <memory>
#include <string>
#include <thread>

#include "ConsumeResult.h"
#include "DefaultMQPushConsumerConfigImpl.hpp"
#include "MQClientImpl.h"
#include "MQConsumerInner.h"
#include "PullRequest.h"
#include "concurrent/executor.hpp"

namespace rocketmq {

class AsyncPullCallback;
class ConsumeMsgService;
class OffsetStore;
class PullAPIWrapper;
class RebalancePushImpl;
struct SubscriptionData;

class DefaultMQPushConsumerImpl;
using DefaultMQPushConsumerImplPtr = std::shared_ptr<DefaultMQPushConsumerImpl>;

class DefaultMQPushConsumerImpl final : public std::enable_shared_from_this<DefaultMQPushConsumerImpl>,
                                        public MQClientImpl,
                                        public MQConsumerInner {
 private:
  class AsyncPullCallback;
  using Task = std::function<void()>;

 public:
  enum class MessageListenerType { kConcurrently, kOrderly };
  using MessageListener = std::function<ConsumeStatus(std::vector<MessageExtPtr>&) /* noexcept */>;

 public:
  /**
   * create() - Factory method for DefaultMQPushConsumerImpl, used to ensure that all objects of
   * DefaultMQPushConsumerImpl are managed by std::share_ptr
   */
  static DefaultMQPushConsumerImplPtr Create(const std::shared_ptr<DefaultMQPushConsumerConfigImpl>& config,
                                             RPCHookPtr rpc_hook = nullptr) {
    return DefaultMQPushConsumerImplPtr{new DefaultMQPushConsumerImpl(config, std::move(rpc_hook))};
  }

  ~DefaultMQPushConsumerImpl();

  // disable copy
  DefaultMQPushConsumerImpl(const DefaultMQPushConsumerImpl&) = delete;
  DefaultMQPushConsumerImpl& operator=(const DefaultMQPushConsumerImpl&) = delete;

  // disable move
  DefaultMQPushConsumerImpl(DefaultMQPushConsumerImpl&&) = delete;
  DefaultMQPushConsumerImpl& operator=(DefaultMQPushConsumerImpl&&) = delete;

 private:
  DefaultMQPushConsumerImpl(const std::shared_ptr<DefaultMQPushConsumerConfigImpl>& config, RPCHookPtr rpc_hook);

 public:  // MQPushConsumer
  void start() override;
  void shutdown() override;

  void Suspend();
  void Resume();

  void RegisterMessageListener(MessageListener message_listener, MessageListenerType message_listener_type);

  void Subscribe(const std::string& topic, const std::string& expression);

  bool SendMessageBack(const MessageExtPtr& message, int delay_level);
  bool SendMessageBack(const MessageExtPtr& message, int delay_level, const std::string& broker_name);

 public:  // MQConsumerInner
  const std::string& groupName() const override;
  MessageModel messageModel() const override;
  ConsumeType consumeType() const override;
  ConsumeFromWhere consumeFromWhere() const override;

  std::vector<SubscriptionData> subscriptions() const override;

  // service discovery
  void updateTopicSubscribeInfo(const std::string& topic, const std::vector<MessageQueue>& info) override;

  // load balancing
  void doRebalance() override;

  // offset persistence
  void persistConsumerOffset() override;

  void pullMessage(PullRequestPtr pull_request) override;

  std::unique_ptr<ConsumerRunningInfo> consumerRunningInfo() override;

 public:
  void DispatchPullRequest(const std::vector<PullRequestPtr>& pull_request_list);

  void ResetRetryAndNamespace(const std::vector<MessageExtPtr>& messages);

  void UpdateConsumeOffset(const MessageQueue& message_queue, int64_t offset);

 private:
  void CheckConfig();
  void CopySubscription();
  void UpdateTopicSubscribeInfoWhenSubscriptionChanged();

 public:
  bool pause() const { return pause_; };
  void set_pause(bool pause) { pause_ = pause; }

  bool consume_orderly() const { return consume_orderly_; }

  RebalancePushImpl* rebalance_impl() const { return rebalance_impl_.get(); }

  OffsetStore* offset_store() const { return offset_store_.get(); }

  DefaultMQPushConsumerConfigImpl& config() const {
    return static_cast<DefaultMQPushConsumerConfigImpl&>(*client_config_);
  }

 private:
  uint64_t start_time_{0};

  volatile bool pause_{false};
  bool consume_orderly_{false};

  std::map<std::string, std::string> subscription_;

  MessageListener message_listener_;
  std::unique_ptr<ConsumeMsgService> consume_service_;

  std::unique_ptr<RebalancePushImpl> rebalance_impl_;
  std::unique_ptr<PullAPIWrapper> pull_api_wrapper_;
  std::unique_ptr<OffsetStore> offset_store_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_DEFAULTMQPUSHCONSUMERIMPL_H_
