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
#include "DefaultMQPushConsumerImpl.h"

#ifndef WIN32
#include <signal.h>
#endif

#include <exception>
#include <memory>
#include <utility>

#include "CommunicationMode.h"
#include "ConsumeMsgService.h"
#include "FilterAPI.hpp"
#include "LocalFileOffsetStore.h"
#include "Logging.h"
#include "MQAdminImpl.h"
#include "MQClientAPIImpl.h"
#include "MQClientInstance.h"
#include "MQClientManager.h"
#include "MQProtos.h"
#include "NamespaceUtil.h"
#include "OffsetStore.h"
#include "PullAPIWrapper.h"
#include "PullMessageService.hpp"
#include "PullSysFlag.h"
#include "RebalancePushImpl.h"
#include "RemoteBrokerOffsetStore.h"
#include "ResultState.hpp"
#include "SocketUtil.h"
#include "UtilAll.h"
#include "Validators.h"
#include "protocol/body/ConsumerRunningInfo.hpp"
#include "route/TopicSubscribeInfo.hpp"
#include "utility/MakeUnique.hpp"

namespace {

constexpr long kBrockerSuspendMaxTimeMillis = 1000 * 15;
constexpr long kConsumerTimeoutMillisWhenSuspend = 1000 * 30;

}  // namespace

namespace rocketmq {

DefaultMQPushConsumerImpl::DefaultMQPushConsumerImpl(const std::shared_ptr<DefaultMQPushConsumerConfigImpl>& config,
                                                     RPCHookPtr rpc_hook)
    : MQClientImpl(std::static_pointer_cast<MQClientConfig>(config), std::move(rpc_hook)),
      rebalance_impl_(new RebalancePushImpl(this)) {}

DefaultMQPushConsumerImpl::~DefaultMQPushConsumerImpl() = default;

void DefaultMQPushConsumerImpl::start() {
#ifndef WIN32
  /* Ignore the SIGPIPE */
  struct sigaction sa;
  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_handler = SIG_IGN;
  sa.sa_flags = 0;
  ::sigaction(SIGPIPE, &sa, 0);
#endif

  switch (service_state_) {
    case ServiceState::kCreateJust: {
      start_time_ = UtilAll::currentTimeMillis();

      // wrap namespace
      client_config_->set_group_name(
          NamespaceUtil::wrapNamespace(client_config_->name_space(), client_config_->group_name()));

      LOG_INFO_NEW("the consumer [{}] start beginning.", client_config_->group_name());

      service_state_ = ServiceState::kStartFailed;

      CheckConfig();

      CopySubscription();

      if (messageModel() == MessageModel::CLUSTERING) {
        client_config_->changeInstanceNameToPID();
      }

      // init client_instance_
      MQClientImpl::start();

      // init rebalance_impl_
      rebalance_impl_->set_consumer_group(client_config_->group_name());
      rebalance_impl_->set_message_model(config().message_model());
      rebalance_impl_->set_client_instance(client_instance_.get());
      if (config().allocate_mq_strategy() != nullptr) {
        rebalance_impl_->set_allocate_mq_strategy(config().allocate_mq_strategy());
      }

      // init pull_api_wrapper_
      pull_api_wrapper_ = MakeUnique<PullAPIWrapper>(client_instance_.get(), client_config_->group_name());
      // TODO: registerFilterMessageHook

      // init offset_store_
      switch (config().message_model()) {
        case MessageModel::BROADCASTING:
          offset_store_ = MakeUnique<LocalFileOffsetStore>(client_instance_.get(), client_config_->group_name());
          break;
        case MessageModel::CLUSTERING:
          offset_store_ = MakeUnique<RemoteBrokerOffsetStore>(client_instance_.get(), client_config_->group_name());
          break;
      }
      offset_store_->load();

      // checkConfig() guarantee message_listener_ is not nullptr
      if (consume_orderly_) {
        LOG_INFO_NEW("start orderly consume service: {}", client_config_->group_name());
        consume_service_ = MakeUnique<ConsumeMessageOrderlyService>(this, config().consume_thread_nums(),
                                                                    std::move(message_listener_));
      } else {
        LOG_INFO_NEW("start concurrently consume service: {}", client_config_->group_name());
        consume_orderly_ = false;
        consume_service_ = MakeUnique<ConsumeMessageConcurrentlyService>(this, config().consume_thread_nums(),
                                                                         std::move(message_listener_));
      }
      consume_service_->start();

      // register consumer
      bool registerOK = client_instance_->RegisterConsumer(client_config_->group_name(), this);
      if (!registerOK) {
        service_state_ = ServiceState::kCreateJust;
        consume_service_->shutdown();
        THROW_MQEXCEPTION(MQClientException,
                          "The cousumer group[" + client_config_->group_name() +
                              "] has been created before, specify another name please.",
                          -1);
      }

      client_instance_->Start();

      LOG_INFO_NEW("the consumer [{}] start OK", client_config_->group_name());
      service_state_ = ServiceState::kRunning;
      break;
    }
    case ServiceState::kRunning:
    case ServiceState::kStartFailed:
    case ServiceState::kShutdownAlready:
      THROW_MQEXCEPTION(MQClientException, "The PushConsumer service state not OK, maybe started once", -1);
      break;
    default:
      break;
  }

  UpdateTopicSubscribeInfoWhenSubscriptionChanged();
  client_instance_->SendHeartbeatToAllBrokerWithLock();
  client_instance_->RebalanceImmediately();
}

void DefaultMQPushConsumerImpl::CheckConfig() {
  std::string groupname = client_config_->group_name();

  // check consumerGroup
  Validators::checkGroup(groupname);

  // consumerGroup
  if (DEFAULT_CONSUMER_GROUP == groupname) {
    THROW_MQEXCEPTION(MQClientException,
                      "consumerGroup can not equal " + DEFAULT_CONSUMER_GROUP + ", please specify another one.", -1);
  }

  if (config().message_model() != BROADCASTING && config().message_model() != CLUSTERING) {
    THROW_MQEXCEPTION(MQClientException, "messageModel is valid", -1);
  }

  // subscription
  if (subscription_.empty()) {
    THROW_MQEXCEPTION(MQClientException, "subscription is empty", -1);
  }

  // messageListener
  if (message_listener_ == nullptr) {
    THROW_MQEXCEPTION(MQClientException, "messageListener is null", -1);
  }
}

void DefaultMQPushConsumerImpl::CopySubscription() {
  for (const auto& it : subscription_) {
    LOG_INFO_NEW("buildSubscriptionData: {}, {}", it.first, it.second);
    rebalance_impl_->setSubscriptionData(it.first, FilterAPI::buildSubscriptionData(it.first, it.second));
  }

  switch (config().message_model()) {
    case BROADCASTING:
      break;
    case CLUSTERING: {
      // auto subscript retry topic
      std::string retryTopic = UtilAll::getRetryTopic(client_config_->group_name());
      rebalance_impl_->setSubscriptionData(retryTopic, FilterAPI::buildSubscriptionData(retryTopic, SUB_ALL));
      break;
    }
    default:
      break;
  }
}

void DefaultMQPushConsumerImpl::UpdateTopicSubscribeInfoWhenSubscriptionChanged() {
  auto& subTable = rebalance_impl_->getSubscriptionInner();
  for (const auto& it : subTable) {
    const auto& topic = it.first;
    auto topic_route_data = client_instance_->GetTopicRouteData(topic);
    if (topic_route_data != nullptr) {
      auto subscribeInfo = TopicSubscribeInfo(topic, topic_route_data).message_queues();
      updateTopicSubscribeInfo(topic, subscribeInfo);
    } else {
      bool ret = client_instance_->UpdateTopicRouteInfoFromNameServer(topic);
      if (!ret) {
        LOG_WARN_NEW("The topic[{}] not exist, or its route data not changed", topic);
      }
    }
  }
}

void DefaultMQPushConsumerImpl::shutdown() {
  switch (service_state_) {
    case ServiceState::kRunning: {
      consume_service_->shutdown();
      persistConsumerOffset();
      client_instance_->UnregisterConsumer(client_config_->group_name());
      client_instance_->Shutdown();
      rebalance_impl_->shutdown();
      service_state_ = ServiceState::kShutdownAlready;
      LOG_INFO_NEW("the consumer [{}] shutdown OK", client_config_->group_name());
      break;
    }
    case ServiceState::kCreateJust:
    case ServiceState::kShutdownAlready:
    default:
      break;
  }
}

void DefaultMQPushConsumerImpl::Suspend() {
  pause_ = true;
  LOG_INFO_NEW("suspend this consumer, {}", client_config_->group_name());
}

void DefaultMQPushConsumerImpl::Resume() {
  pause_ = false;
  doRebalance();
  LOG_INFO_NEW("resume this consumer, {}", client_config_->group_name());
}

void DefaultMQPushConsumerImpl::RegisterMessageListener(MessageListener message_listener,
                                                        MessageListenerType message_listener_type) {
  if (nullptr != message_listener) {
    message_listener_ = std::move(message_listener);
    consume_orderly_ = message_listener_type == MessageListenerType::kOrderly;
  }
}

void DefaultMQPushConsumerImpl::Subscribe(const std::string& topic, const std::string& expression) {
  // TODO: change substation after start
  subscription_[topic] = expression;
}

std::vector<SubscriptionData> DefaultMQPushConsumerImpl::subscriptions() const {
  std::vector<SubscriptionData> result;
  const auto& subscription_table = rebalance_impl_->getSubscriptionInner();
  result.reserve(subscription_table.size());
  for (const auto& it : subscription_table) {
    result.push_back(*(it.second));
  }
  return result;
}

void DefaultMQPushConsumerImpl::updateTopicSubscribeInfo(const std::string& topic, const std::vector<MessageQueue>& info) {
  rebalance_impl_->setTopicSubscribeInfo(topic, info);
}

void DefaultMQPushConsumerImpl::doRebalance() {
  if (!pause_) {
    rebalance_impl_->doRebalance(consume_orderly());
  }
}

namespace {

inline void CorrectTagsOffset(OffsetStore& offset_store, const PullRequestPtr& pull_request) {
  if (0L == pull_request->process_queue()->GetCachedMessagesCount()) {
    offset_store.updateOffset(pull_request->message_queue(), pull_request->next_offset(), true);
  }
}

inline void ExecutePullRequestLater(MQClientInstance& client_instance, PullRequestPtr pull_request, long delay) {
  client_instance.GetPullMessageService()->executePullRequestLater(std::move(pull_request), delay);
}

inline void ExecutePullRequestImmediately(MQClientInstance& client_instance, PullRequestPtr pull_request) {
  client_instance.GetPullMessageService()->executePullRequestImmediately(std::move(pull_request));
}

inline void ExecuteTaskLater(MQClientInstance& client_instance, PullMessageService::Task task, long delay) {
  client_instance.GetPullMessageService()->executeTaskLater(std::move(task), delay);
}

}  // namespace

void DefaultMQPushConsumerImpl::DispatchPullRequest(const std::vector<PullRequestPtr>& pull_request_list) {
  for (const auto& pull_request : pull_request_list) {
    ExecutePullRequestImmediately(*client_instance_, pull_request);
    LOG_INFO_NEW("doRebalance, {}, add a new pull request {}", client_config_->group_name(), pull_request->toString());
  }
}

void DefaultMQPushConsumerImpl::pullMessage(PullRequestPtr pull_request) {
  if (nullptr == pull_request) {
    LOG_ERROR("PullRequest is NULL, return");
    return;
  }

  auto process_queue = pull_request->process_queue();
  if (process_queue->dropped()) {
    LOG_WARN_NEW("the pull request[{}] is dropped.", pull_request->toString());
    return;
  }

  process_queue->set_last_pull_timestamp(UtilAll::currentTimeMillis());

  int cachedMessageCount = process_queue->GetCachedMessagesCount();
  if (cachedMessageCount > config().pull_threshold_for_queue()) {
    // too many message in cache, wait to process
    ExecutePullRequestLater(*client_instance_, pull_request, 1000);
    return;
  }

  if (consume_orderly()) {
    if (process_queue->locked()) {
      if (!pull_request->locked_first()) {
        const auto offset = rebalance_impl_->computePullFromWhere(pull_request->message_queue());
        bool brokerBusy = offset < pull_request->next_offset();
        LOG_INFO_NEW(
            "the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
            pull_request->toString(), offset, UtilAll::to_string(brokerBusy));
        if (brokerBusy) {
          LOG_INFO_NEW(
              "[NOTIFYME] the first time to pull message, but pull request offset larger than broker consume offset. "
              "pullRequest: {} NewOffset: {}",
              pull_request->toString(), offset);
        }

        pull_request->set_locked_first(true);
        pull_request->set_next_offset(offset);
      }
    } else {
      ExecutePullRequestLater(*client_instance_, pull_request, config().pull_time_delay_millis_when_exception());
      LOG_INFO_NEW("pull message later because not locked in broker, {}", pull_request->toString());
      return;
    }
  }

  const auto& message_queue = pull_request->message_queue();
  SubscriptionData* subscription_data = rebalance_impl_->getSubscriptionData(message_queue.topic());
  if (nullptr == subscription_data) {
    ExecutePullRequestLater(*client_instance_, pull_request, config().pull_time_delay_millis_when_exception());
    LOG_WARN_NEW("find the consumer's subscription failed, {}", pull_request->toString());
    return;
  }
  const auto& expression = subscription_data->expression;

  bool commit_offset_enable = false;
  int64_t commit_offset_value = 0;
  if (CLUSTERING == config().message_model()) {
    commit_offset_value = offset_store_->readOffset(message_queue, READ_FROM_MEMORY);
    if (commit_offset_value > 0) {
      commit_offset_enable = true;
    }
  }

  int system_flag = PullSysFlag::buildSysFlag(commit_offset_enable,
                                              /* suspend */ true,
                                              /* subscription */ !expression.empty(),
                                              /* class_filter */ false,
                                              /* lite_pull */ false);

  std::weak_ptr<DefaultMQPushConsumerImpl> consumer_ptr{shared_from_this()};
  auto pull_callback = [consumer_ptr, pull_request,
                        subscription_data](ResultState<std::unique_ptr<PullResultExt>> state) {
    auto consumer = consumer_ptr.lock();
    if (nullptr == consumer) {
      LOG_WARN_NEW("DefaultMQPushConsumerImpl is released.");
      return;
    }

    try {
      auto pull_result = consumer->pull_api_wrapper_->ProcessPullResult(pull_request->message_queue(),
                                                                        state.GetResult(), subscription_data);
      switch (pull_result->pull_status()) {
        case PullStatus::kFound: {
          int64_t prev_request_offset = pull_request->next_offset();
          pull_request->set_next_offset(pull_result->next_begin_offset());

          int64_t first_msg_offset = (std::numeric_limits<int64_t>::max)();
          if (!pull_result->found_message_list().empty()) {
            first_msg_offset = pull_result->found_message_list()[0]->queue_offset();

            pull_request->process_queue()->PutMessages(pull_result->found_message_list());
            consumer->consume_service_->submitConsumeRequest(pull_result->found_message_list(),
                                                             pull_request->process_queue(), true);
          }

          ExecutePullRequestImmediately(*consumer->client_instance_, pull_request);

          if (pull_result->next_begin_offset() < prev_request_offset || first_msg_offset < prev_request_offset) {
            LOG_WARN_NEW(
                "[BUG] pull message result maybe data wrong, nextBeginOffset:{} firstMsgOffset:{} "
                "prevRequestOffset:{}",
                pull_result->next_begin_offset(), first_msg_offset, prev_request_offset);
          }
        } break;
        case PullStatus::kNoNewMessage:
        case PullStatus::kNoMatchedMessage:
          pull_request->set_next_offset(pull_result->next_begin_offset());
          CorrectTagsOffset(*consumer->offset_store_, pull_request);
          ExecutePullRequestImmediately(*consumer->client_instance_, pull_request);
          break;
        case PullStatus::kNoLatestMessage:
          pull_request->set_next_offset(pull_result->next_begin_offset());
          CorrectTagsOffset(*consumer->offset_store_, pull_request);
          ExecutePullRequestLater(*consumer->client_instance_, pull_request,
                                  consumer->config().pull_time_delay_millis_when_exception());
          break;
        case PullStatus::kOffsetIllegal: {
          LOG_WARN_NEW("the pull request offset illegal, {} {}", pull_request->toString(), pull_result->ToString());

          pull_request->set_next_offset(pull_result->next_begin_offset());
          pull_request->process_queue()->set_dropped(true);

          // update and persist offset, then removeProcessQueue
          ExecuteTaskLater(
              *consumer->client_instance_,
              [consumer, pull_request]() {
                try {
                  consumer->offset_store()->updateOffset(pull_request->message_queue(), pull_request->next_offset(),
                                                         false);
                  consumer->offset_store()->persist(pull_request->message_queue());
                  consumer->rebalance_impl()->removeProcessQueue(pull_request->message_queue());

                  LOG_WARN_NEW("fix the pull request offset, {}", pull_request->toString());
                } catch (std::exception& e) {
                  LOG_ERROR_NEW("executeTaskLater Exception: {}", e.what());
                }
              },
              10000);
        } break;
        default:
          break;
      }
    } catch (const std::exception& e) {
      if (!UtilAll::isRetryTopic(pull_request->message_queue().topic())) {
        LOG_WARN_NEW("execute the pull request exception: {}", e.what());
      }

      // TODO
      ExecutePullRequestLater(*consumer->client_instance_, pull_request,
                              consumer->config().pull_time_delay_millis_when_exception());
    }
  };

  try {
    pull_api_wrapper_->PullKernelImpl(message_queue,                      // mq
                                      expression,                         // subExpression
                                      subscription_data->type,            // expressionType
                                      subscription_data->version,         // subVersion
                                      pull_request->next_offset(),        // offset
                                      config().pull_batch_size(),         // maxNums
                                      system_flag,                        // sysFlag
                                      commit_offset_value,                // commitOffset
                                      kBrockerSuspendMaxTimeMillis,       // brokerSuspendMaxTimeMillis
                                      kConsumerTimeoutMillisWhenSuspend,  // timeoutMillis
                                      CommunicationMode::kAsync,          // communicationMode
                                      pull_callback);                     // pullCallback
  } catch (MQException& e) {
    LOG_ERROR_NEW("pullKernelImpl exception: {}", e.what());
    ExecutePullRequestLater(*client_instance_, pull_request, config().pull_time_delay_millis_when_exception());
  }
}

void DefaultMQPushConsumerImpl::ResetRetryAndNamespace(const std::vector<MessageExtPtr>& messages) {
  std::string retry_topic = UtilAll::getRetryTopic(groupName());
  for (const auto& message : messages) {
    std::string group_topic = message->getProperty(MQMessageConst::PROPERTY_RETRY_TOPIC);
    if (!group_topic.empty() && retry_topic == message->topic()) {
      message->set_topic(group_topic);
    }
  }

  const auto& name_space = client_config_->name_space();
  if (!name_space.empty()) {
    for (const auto& message : messages) {
      message->set_topic(NamespaceUtil::withoutNamespace(message->topic(), name_space));
    }
  }
}

bool DefaultMQPushConsumerImpl::SendMessageBack(const MessageExtPtr& message, int delay_level) {
  return SendMessageBack(message, delay_level, null);
}

bool DefaultMQPushConsumerImpl::SendMessageBack(const MessageExtPtr& message,
                                                int delay_level,
                                                const std::string& broker_name) {
  try {
    message->set_topic(NamespaceUtil::wrapNamespace(client_config_->name_space(), message->topic()));

    std::string broker_addr =
        broker_name.empty() ? message->store_host_string() : client_instance_->FindBrokerAddressInPublish(broker_name);

    client_instance_->GetMQClientAPIImpl()->ConsumerSendMessageBack(broker_addr, message, config().group_name(),
                                                                    delay_level, config().max_reconsume_times(), 5000);
    return true;
  } catch (const std::exception& e) {
    LOG_ERROR_NEW("sendMessageBack exception, group: {}, msg: {}. {}", config().group_name(), message->toString(),
                  e.what());
  }
  return false;
}

void DefaultMQPushConsumerImpl::persistConsumerOffset() {
  if (isServiceStateOk()) {
    std::vector<MessageQueue> message_queues = rebalance_impl_->getAllocatedMQ();
    offset_store_->persistAll(message_queues);
  }
}

void DefaultMQPushConsumerImpl::UpdateConsumeOffset(const MessageQueue& message_queue, int64_t offset) {
  if (offset >= 0) {
    offset_store_->updateOffset(message_queue, offset, false);
  } else {
    LOG_ERROR_NEW("updateConsumeOffset of mq:{} error", message_queue.ToString());
  }
}

std::unique_ptr<ConsumerRunningInfo> DefaultMQPushConsumerImpl::consumerRunningInfo() {
  std::unique_ptr<ConsumerRunningInfo> info(new ConsumerRunningInfo());

  info->properties.emplace(ConsumerRunningInfo::PROP_CONSUME_ORDERLY, UtilAll::to_string(consume_orderly_));
  info->properties.emplace(ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE,
                           UtilAll::to_string(config().consume_thread_nums()));
  info->properties.emplace(ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP, UtilAll::to_string(start_time_));
  info->subscription_set = subscriptions();

  auto processQueueTable = rebalance_impl_->getProcessQueueTable();
  for (const auto& it : processQueueTable) {
    const auto& message_queue = it.first;
    const auto& process_queue = it.second;

    ProcessQueueInfo process_queue_info;
    process_queue_info.commit_offset = offset_store_->readOffset(message_queue, MEMORY_FIRST_THEN_STORE);
    process_queue->FillProcessQueueInfo(process_queue_info);
    info->message_queue_table.emplace(message_queue, process_queue_info);
  }

  // TODO: ConsumeStatus

  return info;
}

const std::string& DefaultMQPushConsumerImpl::groupName() const {
  return client_config_->group_name();
}

MessageModel DefaultMQPushConsumerImpl::messageModel() const {
  return config().message_model();
};

ConsumeType DefaultMQPushConsumerImpl::consumeType() const {
  return CONSUME_PASSIVELY;
}

ConsumeFromWhere DefaultMQPushConsumerImpl::consumeFromWhere() const {
  return config().consume_from_where();
}

}  // namespace rocketmq
