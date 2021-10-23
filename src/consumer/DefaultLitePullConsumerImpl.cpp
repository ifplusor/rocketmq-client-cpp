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
#include "DefaultLitePullConsumerImpl.h"

#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#ifndef WIN32
#include <signal.h>
#endif

#include "AssignedMessageQueue.hpp"
#include "FilterAPI.hpp"
#include "LocalFileOffsetStore.h"
#include "MQAdminImpl.h"
#include "MQClientAPIImpl.h"
#include "MQClientInstance.h"
#include "MQException.h"
#include "NamespaceUtil.h"
#include "ProcessQueue.h"
#include "PullAPIWrapper.h"
#include "PullMessageService.hpp"
#include "PullRequest.h"
#include "PullSysFlag.h"
#include "RebalanceLitePullImpl.h"
#include "RemoteBrokerOffsetStore.h"
#include "UtilAll.h"
#include "Validators.h"
#include "protocol/body/ConsumerRunningInfo.hpp"
#include "route/TopicSubscribeInfo.hpp"
#include "utility/MakeUnique.hpp"

static const long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;
static const long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;

namespace rocketmq {

class DefaultLitePullConsumerImpl::MessageQueueListenerImpl : public MessageQueueListener {
 public:
  MessageQueueListenerImpl(const DefaultLitePullConsumerImplPtr& pull_consumer)
      : default_lite_pull_consumer_(pull_consumer) {}

  ~MessageQueueListenerImpl() = default;

  void messageQueueChanged(const std::string& topic,
                           std::vector<MessageQueue>& all_message_queues,
                           std::vector<MessageQueue>& allocated_message_queus) override {
    auto consumer = default_lite_pull_consumer_.lock();
    if (nullptr == consumer) {
      return;
    }
    switch (consumer->messageModel()) {
      case BROADCASTING:
        consumer->UpdateAssignedMessageQueue(topic, all_message_queues);
        break;
      case CLUSTERING:
        consumer->UpdateAssignedMessageQueue(topic, allocated_message_queus);
        break;
      default:
        break;
    }
  }

 private:
  std::weak_ptr<DefaultLitePullConsumerImpl> default_lite_pull_consumer_;
};

DefaultLitePullConsumerImpl::DefaultLitePullConsumerImpl(
    const std::shared_ptr<DefaultLitePullConsumerConfigImpl>& config,
    RPCHookPtr rpc_hook)
    : MQClientImpl(std::static_pointer_cast<MQClientConfig>(config), std::move(rpc_hook)),
      assigned_message_queue_(new AssignedMessageQueue()),
      scheduled_executor_service_("MonitorMessageQueueChangeThread", false),
      rebalance_impl_(new RebalanceLitePullImpl(this)) {}

DefaultLitePullConsumerImpl::~DefaultLitePullConsumerImpl() = default;

void DefaultLitePullConsumerImpl::start() {
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

      scheduled_executor_service_.startup();

      // register consumer
      bool registerOK = client_instance_->RegisterConsumer(client_config_->group_name(), this);
      if (!registerOK) {
        service_state_ = ServiceState::kCreateJust;
        THROW_MQEXCEPTION(MQClientException,
                          "The cousumer group[" + client_config_->group_name() +
                              "] has been created before, specify another name please.",
                          -1);
      }

      client_instance_->Start();

      StartScheduleTask();

      LOG_INFO_NEW("the consumer [{}] start OK", client_config_->group_name());
      service_state_ = ServiceState::kRunning;

      OperateAfterRunning();
      break;
    }
    case ServiceState::kRunning:
    case ServiceState::kStartFailed:
    case ServiceState::kShutdownAlready:
      THROW_MQEXCEPTION(MQClientException, "The PullConsumer service state not OK, maybe started once", -1);
      break;
    default:
      break;
  };
}

void DefaultLitePullConsumerImpl::CheckConfig() {
  const auto& groupname = client_config_->group_name();

  // check consumerGroup
  Validators::checkGroup(groupname);

  // consumerGroup
  if (DEFAULT_CONSUMER_GROUP == groupname) {
    THROW_MQEXCEPTION(MQClientException,
                      "consumerGroup can not equal " + DEFAULT_CONSUMER_GROUP + ", please specify another one.", -1);
  }

  // message_model
  if (config().message_model() != BROADCASTING && config().message_model() != CLUSTERING) {
    THROW_MQEXCEPTION(MQClientException, "messageModel is valid", -1);
  }

  if (config().consumer_timeout_millis_when_suspend() <= config().broker_suspend_max_time_millis()) {
    THROW_MQEXCEPTION(MQClientException,
                      "Long polling mode, the consumer_timeout_millis_when_suspend must greater than "
                      "broker_suspend_max_time_millis ",
                      -1);
  }
}

void DefaultLitePullConsumerImpl::StartScheduleTask() {
  scheduled_executor_service_.schedule([this] { FetchTopicMessageQueuesAndComparePeriodically(); }, 1000 * 10,
                                       time_unit::milliseconds);
}

void DefaultLitePullConsumerImpl::FetchTopicMessageQueuesAndComparePeriodically() {
  try {
    FetchTopicMessageQueuesAndCompare();
  } catch (std::exception& e) {
    LOG_ERROR_NEW("ScheduledTask fetchMessageQueuesAndCompare exception: {}", e.what());
  }

  // next round
  scheduled_executor_service_.schedule([this] { FetchTopicMessageQueuesAndComparePeriodically(); },
                                       config().topic_metadata_check_interval_millis(), time_unit::milliseconds);
}

namespace {

bool IsSetEqual(std::vector<MessageQueue>& new_message_queues, std::vector<MessageQueue>& old_message_queues) {
  if (new_message_queues.size() != old_message_queues.size()) {
    return false;
  }
  std::sort(new_message_queues.begin(), new_message_queues.end());
  std::sort(old_message_queues.begin(), old_message_queues.end());
  return new_message_queues == old_message_queues;
}

}  // namespace

void DefaultLitePullConsumerImpl::FetchTopicMessageQueuesAndCompare() {
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  for (const auto& it : topic_message_queues_changed_listener_map_) {
    const auto& topic = it.first;
    const auto& topic_message_queues_changed_listener = it.second;
    auto& old_message_queues = message_queues_for_topic_[topic];
    auto new_message_queues = FetchMessageQueues(topic);
    bool isChanged = !IsSetEqual(new_message_queues, old_message_queues);
    if (isChanged) {
      auto& message_queues = message_queues_for_topic_[topic] = std::move(new_message_queues);
      if (topic_message_queues_changed_listener != nullptr) {
        topic_message_queues_changed_listener(topic, message_queues);
      }
    }
  }
}

void DefaultLitePullConsumerImpl::OperateAfterRunning() {
  // If subscribe function invoke before start function, then update topic subscribe info after initialization.
  if (subscription_type_ == SubscriptionType::kSubscribe) {
    UpdateTopicSubscribeInfoWhenSubscriptionChanged();
  }
  // If assign function invoke before start function, then update pull task after initialization.
  else if (subscription_type_ == SubscriptionType::kAssign) {
    Resume(assigned_message_queue_->GetMessageQueues());
  }

  for (const auto& it : topic_message_queues_changed_listener_map_) {
    const auto& topic = it.first;
    auto message_queues = FetchMessageQueues(topic);
    message_queues_for_topic_[topic] = std::move(message_queues);
  }
  // client_instance_->checkClientInBroker();
}

void DefaultLitePullConsumerImpl::UpdateTopicSubscribeInfoWhenSubscriptionChanged() {
  auto& subscription_table = rebalance_impl_->getSubscriptionInner();
  for (const auto& it : subscription_table) {
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

void DefaultLitePullConsumerImpl::shutdown() {
  switch (service_state_) {
    case ServiceState::kCreateJust:
      break;
    case ServiceState::kRunning:
      persistConsumerOffset();
      client_instance_->UnregisterConsumer(client_config_->group_name());
      scheduled_executor_service_.shutdown();
      client_instance_->Shutdown();
      rebalance_impl_->shutdown();
      service_state_ = ServiceState::kShutdownAlready;
      LOG_INFO_NEW("the consumer [{}] shutdown OK", client_config_->group_name());
      break;
    default:
      break;
  }
}

std::vector<MQMessageExt> DefaultLitePullConsumerImpl::Poll(int64_t timeout) {
  // checkServiceState();
  if (auto_commit_) {
    MaybeAutoCommit();
  }

  auto messages = message_cache_.TakeMessages(timeout, config().pull_batch_size());
  // if namespace not empty, reset Topic without namespace.
  ResetTopic(messages);
  return MQMessageExt::Wrap(messages);
}

void DefaultLitePullConsumerImpl::ResetTopic(std::vector<MessageExtPtr>& messages) {
  if (messages.empty()) {
    return;
  }

  // If namespace not null , reset Topic without namespace.
  const auto& name_space = config().name_space();
  if (!name_space.empty()) {
    for (auto& message : messages) {
      message->set_topic(NamespaceUtil::withoutNamespace(message->topic(), name_space));
    }
  }
}

void DefaultLitePullConsumerImpl::Subscribe(const std::string& topic, const std::string& expression) {
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  try {
    if (topic.empty()) {
      THROW_MQEXCEPTION(MQClientException, "Topic can not be null or empty.", -1);
    }

    // record subscription data
    set_subscription_type(SubscriptionType::kSubscribe);
    rebalance_impl_->setSubscriptionData(topic, FilterAPI::buildSubscriptionData(topic, expression));

    message_queue_listener_ = MakeUnique<MessageQueueListenerImpl>(shared_from_this());
    assigned_message_queue_->set_rebalance_impl(rebalance_impl_.get());

    if (service_state_ == ServiceState::kRunning) {
      client_instance_->SendHeartbeatToAllBrokerWithLock();
      UpdateTopicSubscribeInfoWhenSubscriptionChanged();
    }
  } catch (std::exception& e) {
    THROW_MQEXCEPTION2(MQClientException, "subscribe exception", -1, std::make_exception_ptr(e));
  }
}

void DefaultLitePullConsumerImpl::Subscribe(const std::string& topic, const MessageSelector& selector) {
  // TODO:
}

void DefaultLitePullConsumerImpl::Unsubscribe(const std::string& topic) {
  // TODO:
}

std::vector<SubscriptionData> DefaultLitePullConsumerImpl::subscriptions() const {
  std::vector<SubscriptionData> result;
  const auto& subscription_table = rebalance_impl_->getSubscriptionInner();
  result.reserve(subscription_table.size());
  for (const auto& it : subscription_table) {
    result.push_back(*(it.second));
  }
  return result;
}

void DefaultLitePullConsumerImpl::updateTopicSubscribeInfo(const std::string& topic,
                                                           const std::vector<MessageQueue>& info) {
  rebalance_impl_->setTopicSubscribeInfo(topic, info);
}

void DefaultLitePullConsumerImpl::doRebalance() {
  if (rebalance_impl_ != nullptr) {
    rebalance_impl_->doRebalance(false);
  }
}

void DefaultLitePullConsumerImpl::UpdateAssignedMessageQueue(const std::string& topic,
                                                             std::vector<MessageQueue>& assigned_message_queues) {
  auto pull_request_list = assigned_message_queue_->UpdateAssignedMessageQueue(topic, assigned_message_queues);
  DispatchAssigndPullRequest(pull_request_list);
}

void DefaultLitePullConsumerImpl::UpdateAssignedMessageQueue(std::vector<MessageQueue>& assigned_message_queues) {
  auto pull_request_list = assigned_message_queue_->UpdateAssignedMessageQueue(assigned_message_queues);
  DispatchAssigndPullRequest(pull_request_list);
}

void DefaultLitePullConsumerImpl::DispatchAssigndPullRequest(std::vector<PullRequestPtr>& pull_request_list) {
  for (const auto& pull_request : pull_request_list) {
    if (service_state_ != ServiceState::kRunning) {
      pull_request->process_queue()->set_paused(true);
    }
    ExecutePullRequestImmediately(pull_request);
  }
}

void DefaultLitePullConsumerImpl::ExecutePullRequestLater(PullRequestPtr pull_request, long delay) {
  client_instance_->GetPullMessageService()->executePullRequestLater(std::move(pull_request), delay);
}

void DefaultLitePullConsumerImpl::ExecutePullRequestImmediately(PullRequestPtr pull_request) {
  client_instance_->GetPullMessageService()->executePullRequestImmediately(std::move(pull_request));
}

void DefaultLitePullConsumerImpl::pullMessage(PullRequestPtr pull_request) {
  if (nullptr == pull_request) {
    LOG_ERROR("PullRequest is NULL, return");
    return;
  }

  auto process_queue = pull_request->process_queue();
  if (process_queue->dropped()) {
    LOG_WARN_NEW("the pull request[{}] is dropped.", pull_request->toString());
    return;
  }

  const auto& message_queue = pull_request->message_queue();

  if (process_queue->paused()) {
    ExecutePullRequestLater(pull_request, PULL_TIME_DELAY_MILLS_WHEN_PAUSE);
    LOG_DEBUG_NEW("Message Queue: {} has been paused!", message_queue.ToString());
    return;
  }

  // FIXME
  // if (consume_request_cache_.size() * config->pull_batch_size() > config->pull_threshold_for_all()) {
  //  executePullRequestLater(pull_request, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
  //  if ((consume_request_flow_control_times_++ % 1000) == 0) {
  //    LOG_WARN_NEW(
  //        "The consume request count exceeds threshold {}, so do flow control, consume request count={}, "
  //        "flowControlTimes={}",
  //        config->pull_threshold_for_all(), consume_request_cache_.size(), consume_request_flow_control_times_);
  //  }
  //  return;
  //}

  // FIXME
  auto cached_message_count = process_queue->GetCachedMessagesCount();
  if (cached_message_count > config().pull_threshold_for_queue()) {
    ExecutePullRequestLater(std::move(pull_request), PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
    if ((queue_flow_control_times_++ % 1000) == 0) {
      LOG_WARN_NEW(
          "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, "
          "count={}, size={} MiB, flowControlTimes={}",
          config().pull_threshold_for_queue(), process_queue->GetCachedMinOffset(), process_queue->GetCachedMaxOffset(),
          cached_message_count, "unknown", queue_flow_control_times_);
    }
    return;
  }

  // long cachedMessageSizeInMiB = processQueue->getMsgSize() / (1024 * 1024);
  // if (cachedMessageSizeInMiB > consumer.getPullThresholdSizeForQueue()) {
  //   scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
  //   if ((queueFlowControlTimes++ % 1000) == 0) {
  //     log.warn(
  //         "The cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={},
  //         "
  //         "count={}, size={} MiB, flowControlTimes={}",
  //         consumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(),
  //         processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB,
  //         queueFlowControlTimes);
  //   }
  //   return;
  // }

  // if (processQueue.getMaxSpan() > consumer.getConsumeMaxSpan()) {
  //   scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
  //   if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
  //     log.warn(
  //         "The queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, "
  //         "flowControlTimes={}",
  //         processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(),
  //         processQueue.getMaxSpan(),
  //         queueMaxSpanFlowControlTimes);
  //   }
  //   return;
  // }

  bool need_delete = subscription_type_ != SubscriptionType::kSubscribe;
  std::shared_ptr<SubscriptionData> subscription_data(
      [this, &message_queue]() {
        if (subscription_type_ == SubscriptionType::kSubscribe) {
          return rebalance_impl_->getSubscriptionData(message_queue.topic());
        }
        return FilterAPI::buildSubscriptionData(message_queue.topic(), SUB_ALL).release();
      }(),
      [need_delete](SubscriptionData* subscription_data) {
        if (need_delete) {
          delete subscription_data;
        }
      });

  bool is_tag_type = ExpressionType::isTagType(subscription_data->type);

  int system_flag = PullSysFlag::buildSysFlag(/* commit_offset */ false,
                                              /* suspend */ true,
                                              /* subscription */ true,
                                              /* class_filter */ false,
                                              /* lite_pull */ true);

  std::weak_ptr<DefaultLitePullConsumerImpl> consumer_ptr{shared_from_this()};
  auto pull_callback = [consumer_ptr, pull_request,
                        subscription_data](ResultState<std::unique_ptr<PullResultExt>> state) {
    auto process_queue = pull_request->process_queue();
    if (process_queue->dropped()) {
      LOG_WARN_NEW("the pull request[{}] is dropped.", pull_request->toString());
      return;
    }

    auto consumer = consumer_ptr.lock();
    if (nullptr == consumer) {
      LOG_WARN_NEW("DefaultLitePullConsumerImpl is released.");
      return;
    }

    try {
      auto pull_result = consumer->pull_api_wrapper_->ProcessPullResult(
          pull_request->message_queue(), std::move(state.GetResult()), subscription_data.get());

      {
        std::lock_guard<std::timed_mutex> lock(process_queue->consume_mutex());
        if (process_queue->seek_offset() == -1) {
          process_queue->set_pull_offset(pull_result->next_begin_offset());
          if (pull_result->pull_status() == PullStatus::kFound && !pull_result->found_message_list().empty()) {
            consumer->message_cache().PutMessages(process_queue, pull_result->found_message_list());
          }
        }
      }

      if (pull_result->pull_status() == PullStatus::kNoLatestMessage) {
        consumer->ExecutePullRequestLater(pull_request, consumer->config().pull_time_delay_millis_when_exception());
      } else {
        if (pull_result->pull_status() == PullStatus::kOffsetIllegal) {
          LOG_WARN_NEW("The pull request offset illegal, {}", pull_result->ToString());
        }
        consumer->ExecutePullRequestImmediately(pull_request);
      }
    } catch (const std::exception& e) {
      LOG_ERROR_NEW("An error occurred in pull message process. {}", e.what());
      consumer->ExecutePullRequestLater(pull_request, consumer->config().pull_time_delay_millis_when_exception());
    }
  };

  try {
    pull_api_wrapper_->PullKernelImpl(message_queue,                                    // mq
                                      subscription_data->expression,                    // subExpression
                                      subscription_data->type,                          // expressionType
                                      is_tag_type ? 0L : subscription_data->version,    // subVersion
                                      NextPullOffset(process_queue),                    // offset
                                      config().pull_batch_size(),                       // maxNums
                                      system_flag,                                      // sysFlag
                                      0,                                                // commitOffset
                                      config().broker_suspend_max_time_millis(),        // brokerSuspendMaxTimeMillis
                                      config().consumer_timeout_millis_when_suspend(),  // timeoutMillis
                                      CommunicationMode::kAsync,                        // communicationMode
                                      std::move(pull_callback));                        // pullCallback
  } catch (std::exception& e) {
    LOG_ERROR_NEW("An error occurred in pull message process. {}", e.what());
    ExecutePullRequestLater(pull_request, config().pull_time_delay_millis_when_exception());
  }
}

int64_t DefaultLitePullConsumerImpl::NextPullOffset(const ProcessQueuePtr& process_queue) {
  int64_t offset = -1;

  std::lock_guard<std::timed_mutex> lock(process_queue->consume_mutex());
  int64_t seek_offset = process_queue->seek_offset();
  if (seek_offset != -1) {
    offset = seek_offset;
    process_queue->set_consume_offset(offset);
    process_queue->set_seek_offset(-1);
  } else {
    offset = process_queue->pull_offset();
    if (offset == -1) {
      offset = FetchConsumeOffset(process_queue->message_queue());
    }
  }

  return offset;
}

int64_t DefaultLitePullConsumerImpl::FetchConsumeOffset(const MessageQueue& message_queue) {
  // checkServiceState();
  return rebalance_impl_->computePullFromWhere(message_queue);
}

void DefaultLitePullConsumerImpl::MaybeAutoCommit() {
  auto now = UtilAll::currentTimeMillis();
  if (now >= next_auto_commit_deadline_) {
    next_auto_commit_deadline_ = now + config().auto_commit_interval_millis();
    CommitAll();
  }
}

void DefaultLitePullConsumerImpl::CommitAll() {
  // TODO: lock
  try {
    std::vector<MessageQueue> message_queues = assigned_message_queue_->GetMessageQueues();
    for (const auto& message_queue : message_queues) {
      auto process_queue = assigned_message_queue_->GetProcessQueue(message_queue);
      if (process_queue != nullptr && !process_queue->dropped()) {
        UpdateConsumeOffset(message_queue, process_queue->consume_offset());
      }
    }
    if (config().message_model() == MessageModel::BROADCASTING) {
      offset_store_->persistAll(message_queues);
    }
  } catch (std::exception& e) {
    LOG_ERROR_NEW("An error occurred when update consume offset Automatically.");
  }
}

void DefaultLitePullConsumerImpl::UpdateConsumeOffset(const MessageQueue& mq, int64_t offset) {
  // checkServiceState();
  offset_store_->updateOffset(mq, offset, false);
}

void DefaultLitePullConsumerImpl::persistConsumerOffset() {
  if (isServiceStateOk()) {
    std::vector<MessageQueue> allocated_mqs = assigned_message_queue_->GetMessageQueues();
    offset_store_->persistAll(allocated_mqs);
  }
}

std::vector<MessageQueue> DefaultLitePullConsumerImpl::FetchMessageQueues(const std::string& topic) {
  std::vector<MessageQueue> result;
  if (isServiceStateOk()) {
    client_instance_->GetMQAdminImpl()->fetchSubscribeMessageQueues(topic, result);
    ParseMessageQueues(result);
  }
  return result;
}

void DefaultLitePullConsumerImpl::ParseMessageQueues(std::vector<MessageQueue>& queueSet) {
  const auto& name_space = client_config_->name_space();
  if (name_space.empty()) {
    return;
  }
  for (auto& message_queue : queueSet) {
    auto user_topic = NamespaceUtil::withoutNamespace(message_queue.topic(), name_space);
    message_queue.set_topic(std::move(user_topic));
  }
}

void DefaultLitePullConsumerImpl::Assign(std::vector<MessageQueue>& message_queues) {
  if (message_queues.empty()) {
    THROW_MQEXCEPTION(MQClientException, "Message queues can not be empty.", -1);
  }
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  set_subscription_type(SubscriptionType::kAssign);
  UpdateAssignedMessageQueue(message_queues);
}

void DefaultLitePullConsumerImpl::Seek(const MessageQueue& message_queue, int64_t offset) {
  auto process_queue = assigned_message_queue_->GetProcessQueue(message_queue);
  if (process_queue == nullptr || process_queue->dropped()) {
    if (subscription_type_ == SubscriptionType::kSubscribe) {
      THROW_MQEXCEPTION(
          MQClientException,
          "The message queue is not in assigned list, may be rebalancing, message queue: " + message_queue.ToString(),
          -1);
    }
    THROW_MQEXCEPTION(MQClientException,
                      "The message queue is not in assigned list, message queue: " + message_queue.ToString(), -1);
  }
  long min_offset = minOffset(message_queue);
  long max_offset = maxOffset(message_queue);
  if (offset < min_offset || offset > max_offset) {
    THROW_MQEXCEPTION(MQClientException,
                      "Seek offset illegal, seek offset = " + std::to_string(offset) + ", min offset = " +
                          std::to_string(min_offset) + ", max offset = " + std::to_string(max_offset),
                      -1);
  }
  std::lock_guard<std::timed_mutex> lock(process_queue->consume_mutex());
  process_queue->set_seek_offset(offset);
  message_cache_.ClearMessages(process_queue);
}

void DefaultLitePullConsumerImpl::SeekToBegin(const MessageQueue& message_queue) {
  int64_t begin = minOffset(message_queue);
  Seek(message_queue, begin);
}

void DefaultLitePullConsumerImpl::SeekToEnd(const MessageQueue& message_queue) {
  int64_t end = maxOffset(message_queue);
  Seek(message_queue, end);
}

int64_t DefaultLitePullConsumerImpl::OffsetForTimestamp(const MessageQueue& message_queue, int64_t timestamp) {
  return searchOffset(message_queue, timestamp);
}

void DefaultLitePullConsumerImpl::Pause(const std::vector<MessageQueue>& message_queues) {
  assigned_message_queue_->Pause(message_queues);
}

void DefaultLitePullConsumerImpl::Resume(const std::vector<MessageQueue>& message_queues) {
  assigned_message_queue_->Resume(message_queues);
}

void DefaultLitePullConsumerImpl::CommitSync() {
  CommitAll();
}

int64_t DefaultLitePullConsumerImpl::Committed(const MessageQueue& message_queue) {
  // checkServiceState();
  auto offset = offset_store_->readOffset(message_queue, ReadOffsetType::MEMORY_FIRST_THEN_STORE);
  if (offset == -2) {
    THROW_MQEXCEPTION(MQClientException, "Fetch consume offset from broker exception", -1);
  }
  return offset;
}

void DefaultLitePullConsumerImpl::RegisterTopicMessageQueuesChangedListener(
    const std::string& topic,
    TopicMessageQueuesChangedListener topic_message_queues_changed_listener) {
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  if (topic.empty() || nullptr == topic_message_queues_changed_listener) {
    THROW_MQEXCEPTION(MQClientException, "Topic or listener is null", -1);
  }
  if (topic_message_queues_changed_listener_map_.find(topic) != topic_message_queues_changed_listener_map_.end()) {
    LOG_WARN_NEW("Topic {} had been registered, new listener will overwrite the old one", topic);
  }

  topic_message_queues_changed_listener_map_[topic] = std::move(topic_message_queues_changed_listener);
  if (service_state_ == ServiceState::kRunning) {
    auto message_queues = FetchMessageQueues(topic);
    message_queues_for_topic_[topic] = std::move(message_queues);
  }
}

std::unique_ptr<ConsumerRunningInfo> DefaultLitePullConsumerImpl::consumerRunningInfo() {
  std::unique_ptr<ConsumerRunningInfo> info(new ConsumerRunningInfo());

  info->properties.emplace(ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP, UtilAll::to_string(start_time_));
  info->subscription_set = subscriptions();

  auto mqs = assigned_message_queue_->GetMessageQueues();
  for (const auto& mq : mqs) {
    auto pq = assigned_message_queue_->GetProcessQueue(mq);
    if (pq != nullptr && !pq->dropped()) {
      ProcessQueueInfo pq_info;
      pq_info.commit_offset = offset_store_->readOffset(mq, MEMORY_FIRST_THEN_STORE);
      pq->FillProcessQueueInfo(pq_info);
      info->message_queue_table.emplace(mq, pq_info);
    }
  }

  return info;
}

bool DefaultLitePullConsumerImpl::isAutoCommit() const {
  return auto_commit_;
}

void DefaultLitePullConsumerImpl::setAutoCommit(bool auto_commit) {
  auto_commit_ = auto_commit;
}

const std::string& DefaultLitePullConsumerImpl::groupName() const {
  return client_config_->group_name();
}

MessageModel DefaultLitePullConsumerImpl::messageModel() const {
  return config().message_model();
};

ConsumeType DefaultLitePullConsumerImpl::consumeType() const {
  return CONSUME_ACTIVELY;
}

ConsumeFromWhere DefaultLitePullConsumerImpl::consumeFromWhere() const {
  return config().consume_from_where();
}

void DefaultLitePullConsumerImpl::set_subscription_type(SubscriptionType subscription_type) {
  if (subscription_type_ == SubscriptionType::kNone) {
    subscription_type_ = subscription_type;
  } else if (subscription_type_ != subscription_type) {
    THROW_MQEXCEPTION(MQClientException, "Subscribe and assign are mutually exclusive.", -1);
  }
}

}  // namespace rocketmq
