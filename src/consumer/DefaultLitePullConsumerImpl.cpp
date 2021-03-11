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

#include <mutex>
#include <string>

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

static const long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;
static const long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;

namespace rocketmq {

class DefaultLitePullConsumerImpl::MessageQueueListenerImpl : public MessageQueueListener {
 public:
  MessageQueueListenerImpl(DefaultLitePullConsumerImplPtr pull_consumer) : default_lite_pull_consumer_(pull_consumer) {}

  ~MessageQueueListenerImpl() = default;

  void messageQueueChanged(const std::string& topic,
                           std::vector<MQMessageQueue>& all_mqs,
                           std::vector<MQMessageQueue>& allocated_mqs) override {
    auto consumer = default_lite_pull_consumer_.lock();
    if (nullptr == consumer) {
      return;
    }
    switch (consumer->messageModel()) {
      case BROADCASTING:
        consumer->updateAssignedMessageQueue(topic, all_mqs);
        break;
      case CLUSTERING:
        consumer->updateAssignedMessageQueue(topic, allocated_mqs);
        break;
      default:
        break;
    }
  }

 private:
  std::weak_ptr<DefaultLitePullConsumerImpl> default_lite_pull_consumer_;
};

class DefaultLitePullConsumerImpl::ConsumeRequest {
 public:
  ConsumeRequest(std::vector<MessageExtPtr>&& message_exts,
                 const MQMessageQueue& message_queue,
                 ProcessQueuePtr process_queue)
      : message_exts_(std::move(message_exts)), message_queue_(message_queue), process_queue_(process_queue) {}

 public:
  std::vector<MessageExtPtr>& message_exts() { return message_exts_; }

  MQMessageQueue& message_queue() { return message_queue_; }

  ProcessQueuePtr process_queue() { return process_queue_; }

 private:
  std::vector<MessageExtPtr> message_exts_;
  MQMessageQueue message_queue_;
  ProcessQueuePtr process_queue_;
};

class DefaultLitePullConsumerImpl::AsyncPullCallback : public AutoDeletePullCallback {
 public:
  AsyncPullCallback(DefaultLitePullConsumerImplPtr pull_consumer,
                    PullRequestPtr request,
                    SubscriptionData* subscription_data,
                    bool own_subscription_data)
      : default_lite_pull_consumer_(pull_consumer), pull_request_(request), subscription_data_(subscription_data) {}

  ~AsyncPullCallback() {
    if (own_subscription_data_) {
      delete subscription_data_;
    }
  }

  void onSuccess(std::unique_ptr<PullResult> pull_result) override {
    auto process_queue = pull_request_->process_queue();
    if (process_queue->dropped()) {
      LOG_WARN_NEW("the pull request[{}] is dropped.", pull_request_->toString());
      return;
    }

    auto consumer = default_lite_pull_consumer_.lock();
    if (nullptr == consumer) {
      LOG_WARN_NEW("AsyncPullCallback::onSuccess: DefaultLitePullConsumerImpl is released.");
      return;
    }

    pull_result.reset(consumer->pull_api_wrapper_->processPullResult(pull_request_->message_queue(),
                                                                     std::move(pull_result), subscription_data_));

    auto& message_queue = pull_request_->message_queue();
    switch (pull_result->pull_status()) {
      case PullStatus::FOUND: {
        std::lock_guard<std::timed_mutex> lock(process_queue->consume_mutex());
        if (!pull_result->msg_found_list().empty() &&
            consumer->assigned_message_queue_->getSeekOffset(message_queue) == -1) {
          process_queue->putMessage(pull_result->msg_found_list());
          consumer->submitConsumeRequest(
              new ConsumeRequest(std::move(pull_result->msg_found_list()), message_queue, process_queue));
        }
      } break;
      case PullStatus::OFFSET_ILLEGAL:
        LOG_WARN_NEW("The pull request offset illegal, {}", pull_result->toString());
      case PullStatus::NO_NEW_MSG:
      case PullStatus::NO_MATCHED_MSG:
        consumer->updatePullOffset(message_queue, pull_result->next_begin_offset());
        consumer->executePullRequestImmediately(pull_request_);
        break;
      case PullStatus::NO_LATEST_MSG:
        consumer->updatePullOffset(message_queue, pull_result->next_begin_offset());
        consumer->executePullRequestLater(
            pull_request_, consumer->getDefaultLitePullConsumerConfig()->pull_time_delay_millis_when_exception());
        break;
      default:
        break;
    }
  }

  void onException(MQException& e) noexcept override {
    auto consumer = default_lite_pull_consumer_.lock();
    if (nullptr == consumer) {
      LOG_WARN_NEW("AsyncPullCallback::onException: DefaultLitePullConsumerImpl is released.");
      return;
    }

    LOG_ERROR_NEW("An error occurred in pull message process. {}", e.what());

    consumer->executePullRequestLater(
        pull_request_, consumer->getDefaultLitePullConsumerConfig()->pull_time_delay_millis_when_exception());
  }

 private:
  std::weak_ptr<DefaultLitePullConsumerImpl> default_lite_pull_consumer_;
  PullRequestPtr pull_request_;
  SubscriptionData* subscription_data_;
  bool own_subscription_data_;
};

DefaultLitePullConsumerImpl::DefaultLitePullConsumerImpl(DefaultLitePullConsumerConfigPtr config)
    : DefaultLitePullConsumerImpl(config, nullptr) {}

DefaultLitePullConsumerImpl::DefaultLitePullConsumerImpl(DefaultLitePullConsumerConfigPtr config, RPCHookPtr rpcHook)
    : MQClientImpl(config, rpcHook),
      start_time_(UtilAll::currentTimeMillis()),
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
    case CREATE_JUST: {
      // wrap namespace
      client_config_->set_group_name(
          NamespaceUtil::wrapNamespace(client_config_->name_space(), client_config_->group_name()));

      LOG_INFO_NEW("the consumer [{}] start beginning.", client_config_->group_name());

      service_state_ = START_FAILED;

      checkConfig();

      if (messageModel() == MessageModel::CLUSTERING) {
        client_config_->changeInstanceNameToPID();
      }

      // init client_instance_
      MQClientImpl::start();

      // init rebalance_impl_
      rebalance_impl_->set_consumer_group(client_config_->group_name());
      rebalance_impl_->set_message_model(getDefaultLitePullConsumerConfig()->message_model());
      rebalance_impl_->set_client_instance(client_instance_.get());
      if (getDefaultLitePullConsumerConfig()->allocate_mq_strategy() != nullptr) {
        rebalance_impl_->set_allocate_mq_strategy(getDefaultLitePullConsumerConfig()->allocate_mq_strategy());
      }

      // init pull_api_wrapper_
      pull_api_wrapper_.reset(new PullAPIWrapper(client_instance_.get(), client_config_->group_name()));
      // TODO: registerFilterMessageHook

      // init offset_store_
      switch (getDefaultLitePullConsumerConfig()->message_model()) {
        case MessageModel::BROADCASTING:
          offset_store_.reset(new LocalFileOffsetStore(client_instance_.get(), client_config_->group_name()));
          break;
        case MessageModel::CLUSTERING:
          offset_store_.reset(new RemoteBrokerOffsetStore(client_instance_.get(), client_config_->group_name()));
          break;
      }
      offset_store_->load();

      scheduled_executor_service_.startup();

      // register consumer
      bool registerOK = client_instance_->registerConsumer(client_config_->group_name(), this);
      if (!registerOK) {
        service_state_ = CREATE_JUST;
        THROW_MQEXCEPTION(MQClientException,
                          "The cousumer group[" + client_config_->group_name() +
                              "] has been created before, specify another name please.",
                          -1);
      }

      client_instance_->start();

      startScheduleTask();

      LOG_INFO_NEW("the consumer [{}] start OK", client_config_->group_name());
      service_state_ = RUNNING;

      operateAfterRunning();
      break;
    }
    case RUNNING:
    case START_FAILED:
    case SHUTDOWN_ALREADY:
      THROW_MQEXCEPTION(MQClientException, "The PullConsumer service state not OK, maybe started once", -1);
      break;
    default:
      break;
  };
}

void DefaultLitePullConsumerImpl::checkConfig() {
  const auto& groupname = client_config_->group_name();

  // check consumerGroup
  Validators::checkGroup(groupname);

  // consumerGroup
  if (DEFAULT_CONSUMER_GROUP == groupname) {
    THROW_MQEXCEPTION(MQClientException,
                      "consumerGroup can not equal " + DEFAULT_CONSUMER_GROUP + ", please specify another one.", -1);
  }

  auto* config = getDefaultLitePullConsumerConfig();

  // messageModel
  if (config->message_model() != BROADCASTING && config->message_model() != CLUSTERING) {
    THROW_MQEXCEPTION(MQClientException, "messageModel is valid", -1);
  }

  if (config->consumer_timeout_millis_when_suspend() <= config->broker_suspend_max_time_millis()) {
    THROW_MQEXCEPTION(MQClientException,
                      "Long polling mode, the consumer_timeout_millis_when_suspend must greater than "
                      "broker_suspend_max_time_millis ",
                      -1);
  }
}

void DefaultLitePullConsumerImpl::startScheduleTask() {
  scheduled_executor_service_.schedule(
      std::bind(&DefaultLitePullConsumerImpl::fetchTopicMessageQueuesAndComparePeriodically, this), 1000 * 10,
      time_unit::milliseconds);
}

void DefaultLitePullConsumerImpl::fetchTopicMessageQueuesAndComparePeriodically() {
  try {
    fetchTopicMessageQueuesAndCompare();
  } catch (std::exception& e) {
    LOG_ERROR_NEW("ScheduledTask fetchMessageQueuesAndCompare exception: {}", e.what());
  }

  // next round
  scheduled_executor_service_.schedule(
      std::bind(&DefaultLitePullConsumerImpl::fetchTopicMessageQueuesAndComparePeriodically, this),
      getDefaultLitePullConsumerConfig()->topic_metadata_check_interval_millis(), time_unit::milliseconds);
}

void DefaultLitePullConsumerImpl::fetchTopicMessageQueuesAndCompare() {
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  for (const auto& it : topic_message_queue_change_listener_map_) {
    const auto& topic = it.first;
    auto* topic_message_queue_change_listener = it.second;
    std::vector<MQMessageQueue> old_message_queues = message_queues_for_topic_[topic];
    std::vector<MQMessageQueue> new_message_queues = fetchMessageQueues(topic);
    bool isChanged = !isSetEqual(new_message_queues, old_message_queues);
    if (isChanged) {
      message_queues_for_topic_[topic] = new_message_queues;
      if (topic_message_queue_change_listener != nullptr) {
        topic_message_queue_change_listener->onChanged(topic, new_message_queues);
      }
    }
  }
}

bool DefaultLitePullConsumerImpl::isSetEqual(std::vector<MQMessageQueue>& new_message_queues,
                                             std::vector<MQMessageQueue>& old_message_queues) {
  if (new_message_queues.size() != old_message_queues.size()) {
    return false;
  }
  std::sort(new_message_queues.begin(), new_message_queues.end());
  std::sort(old_message_queues.begin(), old_message_queues.end());
  return new_message_queues == old_message_queues;
}

void DefaultLitePullConsumerImpl::operateAfterRunning() {
  // If subscribe function invoke before start function, then update topic subscribe info after initialization.
  if (subscription_type_ == SubscriptionType::SUBSCRIBE) {
    updateTopicSubscribeInfoWhenSubscriptionChanged();
  }
  // If assign function invoke before start function, then update pull task after initialization.
  else if (subscription_type_ == SubscriptionType::ASSIGN) {
    resume(assigned_message_queue_->messageQueues());
  }

  for (const auto& it : topic_message_queue_change_listener_map_) {
    const auto& topic = it.first;
    auto message_queues = fetchMessageQueues(topic);
    message_queues_for_topic_[topic] = std::move(message_queues);
  }
  // client_instance_->checkClientInBroker();
}

void DefaultLitePullConsumerImpl::updateTopicSubscribeInfoWhenSubscriptionChanged() {
  auto& subscription_table = rebalance_impl_->getSubscriptionInner();
  for (const auto& it : subscription_table) {
    const auto& topic = it.first;
    bool ret = client_instance_->updateTopicRouteInfoFromNameServer(topic);
    if (!ret) {
      LOG_WARN_NEW("The topic:[{}] not exist", topic);
    }
  }
}

void DefaultLitePullConsumerImpl::shutdown() {
  switch (service_state_) {
    case CREATE_JUST:
      break;
    case RUNNING:
      persistConsumerOffset();
      client_instance_->unregisterConsumer(client_config_->group_name());
      scheduled_executor_service_.shutdown();
      client_instance_->shutdown();
      rebalance_impl_->shutdown();
      service_state_ = ServiceState::SHUTDOWN_ALREADY;
      LOG_INFO_NEW("the consumer [{}] shutdown OK", client_config_->group_name());
      break;
    default:
      break;
  }
}

std::vector<MQMessageExt> DefaultLitePullConsumerImpl::poll() {
  return poll(getDefaultLitePullConsumerConfig()->poll_timeout_millis());
}

std::vector<MQMessageExt> DefaultLitePullConsumerImpl::poll(long timeout) {
  // checkServiceState();
  if (auto_commit_) {
    maybeAutoCommit();
  }

  int64_t end_time = UtilAll::currentTimeMillis() + timeout;

  auto consume_request = consume_request_cache_.pop_front(timeout, time_unit::milliseconds);
  if (end_time - UtilAll::currentTimeMillis() > 0) {
    while (consume_request != nullptr && consume_request->process_queue()->dropped()) {
      consume_request = consume_request_cache_.pop_front();
      if (end_time - UtilAll::currentTimeMillis() <= 0) {
        break;
      }
    }
  }

  if (consume_request != nullptr && !consume_request->process_queue()->dropped()) {
    auto& messages = consume_request->message_exts();
    long offset = consume_request->process_queue()->removeMessage(messages);
    assigned_message_queue_->updateConsumeOffset(consume_request->message_queue(), offset);
    // if namespace not empty, reset Topic without namespace.
    resetTopic(messages);
    return MQMessageExt::FromPtrList(messages);
  }

  return std::vector<MQMessageExt>();
}

void DefaultLitePullConsumerImpl::resetTopic(std::vector<MessageExtPtr>& msg_list) {
  if (msg_list.empty()) {
    return;
  }

  // If namespace not null , reset Topic without namespace.
  const auto& name_space = getDefaultLitePullConsumerConfig()->name_space();
  if (!name_space.empty()) {
    for (auto& message_ext : msg_list) {
      message_ext->set_topic(NamespaceUtil::withoutNamespace(message_ext->topic(), name_space));
    }
  }
}

void DefaultLitePullConsumerImpl::subscribe(const std::string& topic, const std::string& subExpression) {
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  try {
    if (topic.empty()) {
      THROW_MQEXCEPTION(MQClientException, "Topic can not be null or empty.", -1);
    }

    // record subscription data
    set_subscription_type(SubscriptionType::SUBSCRIBE);
    auto* subscription_data = FilterAPI::buildSubscriptionData(topic, subExpression);
    rebalance_impl_->setSubscriptionData(topic, subscription_data);

    message_queue_listener_.reset(new MessageQueueListenerImpl(shared_from_this()));
    assigned_message_queue_->set_rebalance_impl(rebalance_impl_.get());

    if (service_state_ == ServiceState::RUNNING) {
      client_instance_->sendHeartbeatToAllBrokerWithLock();
      updateTopicSubscribeInfoWhenSubscriptionChanged();
    }
  } catch (std::exception& e) {
    THROW_MQEXCEPTION2(MQClientException, "subscribe exception", -1, std::make_exception_ptr(e));
  }
}

void DefaultLitePullConsumerImpl::subscribe(const std::string& topic, const MessageSelector& selector) {
  // TODO:
}

void DefaultLitePullConsumerImpl::unsubscribe(const std::string& topic) {
  // TODO:
}

std::vector<SubscriptionData> DefaultLitePullConsumerImpl::subscriptions() const {
  std::vector<SubscriptionData> result;
  auto& subTable = rebalance_impl_->getSubscriptionInner();
  for (const auto& it : subTable) {
    result.push_back(*(it.second));
  }
  return result;
}

void DefaultLitePullConsumerImpl::updateTopicSubscribeInfo(const std::string& topic,
                                                           std::vector<MQMessageQueue>& info) {
  rebalance_impl_->setTopicSubscribeInfo(topic, info);
}

void DefaultLitePullConsumerImpl::doRebalance() {
  if (rebalance_impl_ != nullptr) {
    rebalance_impl_->doRebalance(false);
  }
}

void DefaultLitePullConsumerImpl::updateAssignedMessageQueue(const std::string& topic,
                                                             std::vector<MQMessageQueue>& assigned_message_queues) {
  auto pull_request_list = assigned_message_queue_->updateAssignedMessageQueue(topic, assigned_message_queues);
  dispatchAssigndPullRequest(pull_request_list);
}

void DefaultLitePullConsumerImpl::updateAssignedMessageQueue(std::vector<MQMessageQueue>& assigned_message_queues) {
  auto pull_request_list = assigned_message_queue_->updateAssignedMessageQueue(assigned_message_queues);
  dispatchAssigndPullRequest(pull_request_list);
}

void DefaultLitePullConsumerImpl::dispatchAssigndPullRequest(std::vector<PullRequestPtr>& pull_request_list) {
  for (const auto& pull_request : pull_request_list) {
    pull_request->set_consumer_group(client_config_->group_name());
    if (service_state_ != ServiceState::RUNNING) {
      pull_request->process_queue()->set_paused(true);
    }
    executePullRequestImmediately(pull_request);
  }
}

int64_t DefaultLitePullConsumerImpl::nextPullOffset(const ProcessQueuePtr& process_queue) {
  int64_t offset = -1;
  int64_t seek_offset = process_queue->seek_offset();
  if (seek_offset != -1) {
    offset = seek_offset;
    process_queue->set_consume_offset(offset);
    process_queue->set_seek_offset(-1);
  } else {
    offset = process_queue->pull_offset();
    if (offset == -1) {
      offset = fetchConsumeOffset(process_queue->message_queue());
    }
  }
  return offset;
}

int64_t DefaultLitePullConsumerImpl::fetchConsumeOffset(const MQMessageQueue& message_queue) {
  // checkServiceState();
  return rebalance_impl_->computePullFromWhere(message_queue);
}

void DefaultLitePullConsumerImpl::executePullRequestLater(PullRequestPtr pull_request, long delay) {
  client_instance_->getPullMessageService()->executePullRequestLater(pull_request, delay);
}

void DefaultLitePullConsumerImpl::executePullRequestImmediately(PullRequestPtr pull_request) {
  client_instance_->getPullMessageService()->executePullRequestImmediately(pull_request);
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

  auto& message_queue = pull_request->message_queue();

  if (process_queue->paused()) {
    executePullRequestLater(pull_request, PULL_TIME_DELAY_MILLS_WHEN_PAUSE);
    LOG_DEBUG_NEW("Message Queue: {} has been paused!", message_queue.toString());
    return;
  }

  auto config = getDefaultLitePullConsumerConfig();

  if (consume_request_cache_.size() * config->pull_batch_size() > config->pull_threshold_for_all()) {
    executePullRequestLater(pull_request, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
    if ((consume_request_flow_control_times_++ % 1000) == 0) {
      LOG_WARN_NEW(
          "The consume request count exceeds threshold {}, so do flow control, consume request count={}, "
          "flowControlTimes={}",
          config->pull_threshold_for_all(), consume_request_cache_.size(), consume_request_flow_control_times_);
    }
    return;
  }

  auto cached_message_count = process_queue->getCacheMsgCount();
  if (cached_message_count > config->pull_threshold_for_queue()) {
    executePullRequestLater(pull_request, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
    if ((queue_flow_control_times_++ % 1000) == 0) {
      LOG_WARN_NEW(
          "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, "
          "count={}, size={} MiB, flowControlTimes={}",
          config->pull_threshold_for_queue(), process_queue->getCacheMinOffset(), process_queue->getCacheMaxOffset(),
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

  auto offset = nextPullOffset(process_queue);
  try {
    SubscriptionData* subscription_data = nullptr;
    if (subscription_type_ == SubscriptionType::SUBSCRIBE) {
      subscription_data = rebalance_impl_->getSubscriptionData(message_queue.topic());
    } else {
      subscription_data = FilterAPI::buildSubscriptionData(message_queue.topic(), SUB_ALL);
    }

    std::unique_ptr<AsyncPullCallback> callback(new AsyncPullCallback(
        shared_from_this(), pull_request, subscription_data, subscription_type_ != SubscriptionType::SUBSCRIBE));

    int sysFlag = PullSysFlag::buildSysFlag(false,  // commit offset
                                            true,   // suspend
                                            true,   // suspend
                                            false,  // class filter
                                            true);

    bool is_tag_type = ExpressionType::isTagType(subscription_data->expression_type());

    pull_api_wrapper_->pullKernelImpl(message_queue,                                        // mq
                                      subscription_data->sub_string(),                      // subExpression
                                      subscription_data->expression_type(),                 // expressionType
                                      is_tag_type ? 0L : subscription_data->sub_version(),  // subVersion
                                      offset,                                               // offset
                                      config->pull_batch_size(),                            // maxNums
                                      sysFlag,                                              // sysFlag
                                      0,                                                    // commitOffset
                                      config->broker_suspend_max_time_millis(),        // brokerSuspendMaxTimeMillis
                                      config->consumer_timeout_millis_when_suspend(),  // timeoutMillis
                                      CommunicationMode::SYNC,                         // communicationMode
                                      callback.get());                                 // pullCallback

    (void)callback.release();
  } catch (std::exception& e) {
    LOG_ERROR_NEW("An error occurred in pull message process. {}", e.what());
    executePullRequestLater(pull_request, config->pull_time_delay_millis_when_exception());
  }
}

void DefaultLitePullConsumerImpl::submitConsumeRequest(ConsumeRequest* consume_request) {
  consume_request_cache_.push_back(consume_request);
}

void DefaultLitePullConsumerImpl::updatePullOffset(const MQMessageQueue& message_queue, int64_t next_pull_offset) {
  if (assigned_message_queue_->getSeekOffset(message_queue) == -1) {
    assigned_message_queue_->updatePullOffset(message_queue, next_pull_offset);
  }
}

void DefaultLitePullConsumerImpl::maybeAutoCommit() {
  auto now = UtilAll::currentTimeMillis();
  if (now >= next_auto_commit_deadline_) {
    commitAll();
    next_auto_commit_deadline_ = now + getDefaultLitePullConsumerConfig()->auto_commit_interval_millis();
  }
}

void DefaultLitePullConsumerImpl::commitAll() {
  try {
    std::vector<MQMessageQueue> message_queues = assigned_message_queue_->messageQueues();
    for (const auto& message_queue : message_queues) {
      auto process_queue = assigned_message_queue_->getProcessQueue(message_queue);
      if (process_queue != nullptr && !process_queue->dropped()) {
        updateConsumeOffset(message_queue, process_queue->consume_offset());
      }
    }
    if (getDefaultLitePullConsumerConfig()->message_model() == MessageModel::BROADCASTING) {
      offset_store_->persistAll(message_queues);
    }
  } catch (std::exception& e) {
    LOG_ERROR_NEW("An error occurred when update consume offset Automatically.");
  }
}

void DefaultLitePullConsumerImpl::updateConsumeOffset(const MQMessageQueue& mq, int64_t offset) {
  // checkServiceState();
  offset_store_->updateOffset(mq, offset, false);
}

void DefaultLitePullConsumerImpl::persistConsumerOffset() {
  if (isServiceStateOk()) {
    std::vector<MQMessageQueue> allocated_mqs = assigned_message_queue_->messageQueues();
    offset_store_->persistAll(allocated_mqs);
  }
}

std::vector<MQMessageQueue> DefaultLitePullConsumerImpl::fetchMessageQueues(const std::string& topic) {
  std::vector<MQMessageQueue> result;
  if (isServiceStateOk()) {
    client_instance_->getMQAdminImpl()->fetchSubscribeMessageQueues(topic, result);
    parseMessageQueues(result);
  }
  return result;
}

void DefaultLitePullConsumerImpl::parseMessageQueues(std::vector<MQMessageQueue>& queueSet) {
  const auto& name_space = client_config_->name_space();
  if (name_space.empty()) {
    return;
  }
  for (auto& messageQueue : queueSet) {
    auto user_topic = NamespaceUtil::withoutNamespace(messageQueue.topic(), name_space);
    messageQueue.set_topic(user_topic);
  }
}

void DefaultLitePullConsumerImpl::assign(std::vector<MQMessageQueue>& message_queues) {
  if (message_queues.empty()) {
    THROW_MQEXCEPTION(MQClientException, "Message queues can not be empty.", -1);
  }
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  set_subscription_type(SubscriptionType::ASSIGN);
  updateAssignedMessageQueue(message_queues);
}

void DefaultLitePullConsumerImpl::seek(const MQMessageQueue& message_queue, int64_t offset) {
  auto process_queue = assigned_message_queue_->getProcessQueue(message_queue);
  if (process_queue == nullptr || process_queue->dropped()) {
    if (subscription_type_ == SubscriptionType::SUBSCRIBE) {
      THROW_MQEXCEPTION(
          MQClientException,
          "The message queue is not in assigned list, may be rebalancing, message queue: " + message_queue.toString(),
          -1);
    } else {
      THROW_MQEXCEPTION(MQClientException,
                        "The message queue is not in assigned list, message queue: " + message_queue.toString(), -1);
    }
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
  clearMessageQueueInCache(process_queue);
}

void DefaultLitePullConsumerImpl::seekToBegin(const MQMessageQueue& message_queue) {
  auto begin = minOffset(message_queue);
  seek(message_queue, begin);
}

void DefaultLitePullConsumerImpl::seekToEnd(const MQMessageQueue& message_queue) {
  auto end = maxOffset(message_queue);
  seek(message_queue, end);
}

void DefaultLitePullConsumerImpl::clearMessageQueueInCache(const ProcessQueuePtr& process_queue) {
  // Iterator<ConsumeRequest> iter = consumeRequestCache.iterator();
  // while (iter.hasNext()) {
  //  if (iter.next().getMessageQueue().equals(messageQueue))
  //    iter.remove();
  //}
}

int64_t DefaultLitePullConsumerImpl::offsetForTimestamp(const MQMessageQueue& message_queue, int64_t timestamp) {
  return searchOffset(message_queue, timestamp);
}

void DefaultLitePullConsumerImpl::pause(const std::vector<MQMessageQueue>& message_queues) {
  assigned_message_queue_->pause(message_queues);
}

void DefaultLitePullConsumerImpl::resume(const std::vector<MQMessageQueue>& message_queues) {
  assigned_message_queue_->resume(message_queues);
}

void DefaultLitePullConsumerImpl::commitSync() {
  commitAll();
}

int64_t DefaultLitePullConsumerImpl::committed(const MQMessageQueue& message_queue) {
  // checkServiceState();
  auto offset = offset_store_->readOffset(message_queue, ReadOffsetType::MEMORY_FIRST_THEN_STORE);
  if (offset == -2) {
    THROW_MQEXCEPTION(MQClientException, "Fetch consume offset from broker exception", -1);
  }
  return offset;
}

void DefaultLitePullConsumerImpl::registerTopicMessageQueueChangeListener(
    const std::string& topic,
    TopicMessageQueueChangeListener* topicMessageQueueChangeListener) {
  std::lock_guard<std::mutex> lock(mutex_);  // synchronized
  if (topic.empty() || nullptr == topicMessageQueueChangeListener) {
    THROW_MQEXCEPTION(MQClientException, "Topic or listener is null", -1);
  }
  if (topic_message_queue_change_listener_map_.find(topic) != topic_message_queue_change_listener_map_.end()) {
    LOG_WARN_NEW("Topic {} had been registered, new listener will overwrite the old one", topic);
  }

  topic_message_queue_change_listener_map_[topic] = topicMessageQueueChangeListener;
  if (service_state_ == ServiceState::RUNNING) {
    auto messageQueues = fetchMessageQueues(topic);
    message_queues_for_topic_[topic] = std::move(messageQueues);
  }
}

ConsumerRunningInfo* DefaultLitePullConsumerImpl::consumerRunningInfo() {
  auto* info = new ConsumerRunningInfo();

  info->setProperty(ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP, UtilAll::to_string(start_time_));

  info->setSubscriptionSet(subscriptions());

  auto mqs = assigned_message_queue_->messageQueues();
  for (const auto& mq : mqs) {
    auto pq = assigned_message_queue_->getProcessQueue(mq);
    if (pq != nullptr && !pq->dropped()) {
      ProcessQueueInfo pq_info;
      pq_info.setCommitOffset(offset_store_->readOffset(mq, MEMORY_FIRST_THEN_STORE));
      pq->fillProcessQueueInfo(pq_info);
      info->setMqTable(mq, pq_info);
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
  return getDefaultLitePullConsumerConfig()->message_model();
};

ConsumeType DefaultLitePullConsumerImpl::consumeType() const {
  return CONSUME_ACTIVELY;
}

ConsumeFromWhere DefaultLitePullConsumerImpl::consumeFromWhere() const {
  return getDefaultLitePullConsumerConfig()->consume_from_where();
}

void DefaultLitePullConsumerImpl::set_subscription_type(SubscriptionType subscription_type) {
  if (subscription_type_ == SubscriptionType::NONE) {
    subscription_type_ = subscription_type;
  } else if (subscription_type_ != subscription_type) {
    THROW_MQEXCEPTION(MQClientException, "Subscribe and assign are mutually exclusive.", -1);
  }
}

}  // namespace rocketmq
