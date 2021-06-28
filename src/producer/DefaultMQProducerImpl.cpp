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
#include "DefaultMQProducerImpl.h"

#ifndef WIN32
#include <signal.h>
#endif

#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include "ClientErrorCode.h"
#include "CommunicationMode.h"
#include "CorrelationIdUtil.hpp"
#include "Logging.h"
#include "MQClientAPIImpl.h"
#include "MQClientInstance.h"
#include "MQClientManager.h"
#include "MQException.h"
#include "MQFaultStrategy.h"
#include "MQProtos.h"
#include "Message.h"
#include "MessageBatch.h"
#include "MessageClientIDSetter.h"
#include "MessageDecoder.h"
#include "MessageQueue.hpp"
#include "MessageSysFlag.h"
#include "RequestFutureTable.h"
#include "ResultState.hpp"
#include "SendResult.hpp"
#include "TopicPublishInfo.hpp"
#include "TransactionMQProducerConfig.h"
#include "UtilAll.h"
#include "Validators.h"
#include "common/GSL.hpp"
#include "protocol/header/CheckTransactionStateRequestHeader.hpp"
#include "protocol/header/EndTransactionRequestHeader.hpp"
#include "protocol/header/SendMessageRequestHeader.hpp"
#include "utility/MakeUnique.hpp"

namespace rocketmq {

DefaultMQProducerImpl::DefaultMQProducerImpl(const std::shared_ptr<DefaultMQProducerConfigImpl>& config,
                                             RPCHookPtr rpc_hook)
    : MQClientImpl(std::static_pointer_cast<MQClientConfig>(config), std::move(rpc_hook)),
      mq_fault_strategy_(new MQFaultStrategy()) {}

DefaultMQProducerImpl::~DefaultMQProducerImpl() = default;

void DefaultMQProducerImpl::start() {
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
      LOG_INFO_NEW("DefaultMQProducerImpl: {} start", config().group_name());

      service_state_ = ServiceState::kStartFailed;

      config().changeInstanceNameToPID();
      mq_fault_strategy_->set_enable(config().send_latency_fault_enable());

      MQClientImpl::start();

      bool registerOK = client_instance_->RegisterProducer(config().group_name(), this);
      if (!registerOK) {
        service_state_ = ServiceState::kCreateJust;
        THROW_MQEXCEPTION(
            MQClientException,
            "The producer group[" + config().group_name() + "] has been created before, specify another name please.",
            -1);
      }

      if (nullptr == async_send_executor_) {
        async_send_executor_ =
            MakeUnique<thread_pool_executor>("AsyncSendThread", config().async_send_thread_nums(), false);
      }
      async_send_executor_->startup();

      client_instance_->Start();

      LOG_INFO_NEW("the producer [{}] start OK.", config().group_name());
      service_state_ = ServiceState::kRunning;
      break;
    }
    case ServiceState::kRunning:
    case ServiceState::kStartFailed:
    case ServiceState::kShutdownAlready:
      THROW_MQEXCEPTION(MQClientException, "The producer service state not OK, maybe started once", -1);
      break;
    default:
      break;
  }

  client_instance_->SendHeartbeatToAllBrokerWithLock();
}

void DefaultMQProducerImpl::shutdown() {
  switch (service_state_) {
    case ServiceState::kRunning: {
      LOG_INFO("DefaultMQProducerImpl shutdown");

      async_send_executor_->shutdown();

      client_instance_->UnregisterProducer(config().group_name());
      client_instance_->Shutdown();

      service_state_ = ServiceState::kShutdownAlready;
      break;
    }
    case ServiceState::kShutdownAlready:
    case ServiceState::kCreateJust:
    default:
      break;
  }
}

std::vector<MessageQueue> DefaultMQProducerImpl::FetchPublishMessageQueues(const std::string& topic) {
  auto topicPublishInfo = client_instance_->FindTopicPublishInfo(topic);
  if (topicPublishInfo != nullptr) {
    return topicPublishInfo->getMessageQueueList();
  }
  return std::vector<MessageQueue>{};
}

SendResult DefaultMQProducerImpl::Send(const MessagePtr& message, int64_t timeout) {
  try {
    return *SendDefaultImpl(message, CommunicationMode::kSync, nullptr, timeout);
  } catch (MQException& e) {
    LOG_ERROR_NEW("send failed, exception:{}", e.what());
    throw;
  }
}

SendResult DefaultMQProducerImpl::Send(const MessagePtr& message, const MessageQueue& message_queue, int64_t timeout) {
  try {
    return *SendToQueueImpl(message, message_queue, CommunicationMode::kSync, nullptr, timeout);
  } catch (MQException& e) {
    LOG_ERROR_NEW("send failed, exception:{}", e.what());
    throw;
  }
}

void DefaultMQProducerImpl::Send(const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
  Expects(send_callback != nullptr);
  async_send_executor_->submit(
#if __cplusplus >= 201402L
      [this, message, send_callback = std::move(send_callback), timeout]
#else
      [this, message, send_callback, timeout]
#endif
      () {
        try {
          (void)SendDefaultImpl(message, CommunicationMode::kAsync, send_callback, timeout);
        } catch (...) {
          send_callback({std::current_exception()});
        }
      });
}

void DefaultMQProducerImpl::Send(const MessagePtr& message,
                                 const MessageQueue& message_queue,
                                 SendCallback send_callback,
                                 int64_t timeout) noexcept {
  Expects(send_callback != nullptr);
  async_send_executor_->submit(
#if __cplusplus >= 201402L
      [this, message, message_queue, send_callback = std::move(send_callback), timeout]
#else
      [this, message, message_queue, send_callback, timeout]
#endif
      () {
        try {
          try {
            (void)SendToQueueImpl(message, message_queue, CommunicationMode::kAsync, send_callback, timeout);
          } catch (MQBrokerException& e) {
            std::string error_message = std::string("unknown exception, ") + e.what();
            THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
          }
        } catch (...) {
          send_callback({std::current_exception()});
        }
      });
}

void DefaultMQProducerImpl::SendOneway(const MessagePtr& message) {
  try {
    (void)SendDefaultImpl(message, CommunicationMode::kOneway, nullptr, config().send_msg_timeout());
  } catch (MQBrokerException& e) {
    std::string error_message = std::string("unknown exception, ") + e.what();
    THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
  }
}

void DefaultMQProducerImpl::SendOneway(const MessagePtr& message, const MessageQueue& message_queue) {
  try {
    (void)SendToQueueImpl(message, message_queue, CommunicationMode::kAsync, nullptr, config().send_msg_timeout());
  } catch (MQBrokerException& e) {
    std::string error_message = std::string("unknown exception, ") + e.what();
    THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
  }
}

SendResult DefaultMQProducerImpl::Send(const MessagePtr& message, MessageQueueSelector selector, int64_t timeout) {
  try {
    return *SendSelectQueueImpl(message, selector, CommunicationMode::kSync, nullptr, timeout);
  } catch (MQException& e) {
    LOG_ERROR_NEW("send failed, exception:{}", e.what());
    throw;
  }
}

void DefaultMQProducerImpl::Send(const MessagePtr& message,
                                 MessageQueueSelector selector,
                                 SendCallback send_callback,
                                 int64_t timeout) noexcept {
  Expects(send_callback != nullptr);
  async_send_executor_->submit(
#if __cplusplus >= 201402L
      [this, message, selector = std::move(selector), send_callback = std::move(send_callback), timeout]
#else
      [this, message, selector, send_callback, timeout]
#endif
      () mutable {
        try {
          try {
            (void)SendSelectQueueImpl(message, selector, CommunicationMode::kAsync, send_callback, timeout);
          } catch (MQBrokerException& e) {
            std::string error_message = std::string("unknown exception, ") + e.what();
            THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
          }
        } catch (...) {
          send_callback({std::current_exception()});
        }
      });
}

void DefaultMQProducerImpl::SendOneway(const MessagePtr& message, MessageQueueSelector selector) {
  try {
    (void)SendSelectQueueImpl(message, selector, CommunicationMode::kOneway, nullptr, config().send_msg_timeout());
  } catch (MQBrokerException& e) {
    std::string error_message = std::string("unknown exception, ") + e.what();
    THROW_MQEXCEPTION(MQClientException, error_message, e.GetError());
  }
}

namespace {

void TryToCompressMessage(Message& message, DefaultMQProducerConfig& config) {
  const auto& body = message.body();
  if (body.size() >= config.compress_msg_body_over_howmuch()) {
    std::string compressed_body;
    if (UtilAll::deflate(body, compressed_body, config.compress_level())) {
      message.set_body(std::move(compressed_body));
      message.putProperty(MQMessageConst::PROPERTY_ALREADY_COMPRESSED_FLAG, "true");
    }
  }
}

void Preprocess(Message& message, DefaultMQProducerConfig& config) {
  if (message.isBatch()) {
    try {
      auto& message_batch = dynamic_cast<MessageBatch&>(message);
      for (const auto& message : message_batch.messages()) {
        Validators::checkMessage(*message, config.max_message_size());
        MessageClientIDSetter::setUniqID(*message);
      }
      message_batch.set_body(message_batch.Encode());
    } catch (std::exception& e) {
      THROW_MQEXCEPTION(MQClientException, "Failed to initiate the MessageBatch", -1);
    }
  }

  Validators::checkMessage(message, config.max_message_size());

  if (!message.isBatch()) {
    MessageClientIDSetter::setUniqID(message);
    TryToCompressMessage(message, config);
  }
}

inline void Preprocess(Message& message, const std::string& topic, DefaultMQProducerConfig& config) {
  Preprocess(message, config);

  if (message.topic() != topic) {
    THROW_MQEXCEPTION(MQClientException, "message's topic not equal mq's topic", -1);
  }
}

inline TopicPublishInfoPtr TryToFindTopicPublishInfo(const std::string& topic, MQClientInstance& client_instance) {
  auto topic_publish_info = client_instance.FindTopicPublishInfo(topic);
  if (topic_publish_info == nullptr || !topic_publish_info->ok()) {
    THROW_MQEXCEPTION(MQClientException, "No route info of this topic: " + topic, -1);
  }
  return topic_publish_info;
}

}  // namespace

class DefaultMQProducerImpl::UnifiedSendDefaultImpl {
 public:
  UnifiedSendDefaultImpl(DefaultMQProducerImpl& producer,
                         MessagePtr message,
                         CommunicationMode communication_mode,
                         SendCallback send_callback,
                         int64_t timeout)
      : message_(std::move(message)),
        communication_mode_(communication_mode),
        retry_times_(communication_mode == CommunicationMode::kAsync ? producer.config().retry_times_for_async()
                                                                     : producer.config().retry_times()),
        retry_when_not_store_ok_(producer.config().retry_another_broker_when_not_store_ok()),
        begin_time_first_(UtilAll::currentTimeMillis()),
        begin_time_prev_(begin_time_first_),
        end_time_(begin_time_first_),
        deadline_(begin_time_first_ + timeout),
        send_callback_(std::move(send_callback)) {}

  ~UnifiedSendDefaultImpl() = default;

  // enable copy
  UnifiedSendDefaultImpl(const UnifiedSendDefaultImpl&) = default;
  UnifiedSendDefaultImpl& operator=(const UnifiedSendDefaultImpl&) = default;

  // enable move
  UnifiedSendDefaultImpl(UnifiedSendDefaultImpl&&) = default;
  UnifiedSendDefaultImpl& operator=(UnifiedSendDefaultImpl&&) = default;

  std::unique_ptr<SendResult> SendDefault(DefaultMQProducerImpl& producer) {
    if (communication_mode_ == CommunicationMode::kAsync) {
      producer_ptr_ = producer.shared_from_this();
    }

    topic_publish_info_ = TryToFindTopicPublishInfo(message_->topic(), *producer.client_instance_);

    if (DoSend(producer) || *send_result_ != nullptr) {
      return std::move(*send_result_);
    }

    std::string error_message = "Send [" + UtilAll::to_string(retry_count_) + "] times, still failed, cost [" +
                                UtilAll::to_string(end_time_ - begin_time_first_) + "]ms, Topic: " + message_->topic();
    THROW_MQEXCEPTION(MQClientException, error_message, -1);
  }

  bool DoSend(DefaultMQProducerImpl& producer) {
    for (retry_count_++; retry_count_ <= retry_times_; retry_count_++) {
      begin_time_prev_ = UtilAll::currentTimeMillis();
      if (begin_time_prev_ >= deadline_) {
        break;
      }

      try {
        const auto& message_queue =
            producer.mq_fault_strategy_->SelectOneMessageQueue(topic_publish_info_.get(), last_broker_name_);
        last_broker_name_ = message_queue.broker_name();
        auto send_result =
            producer.SendKernelImpl(message_, message_queue, communication_mode_,
                                    communication_mode_ == CommunicationMode::kAsync ? *this : (SendCallback) nullptr,
                                    deadline_ - begin_time_prev_);
        if (communication_mode_ == CommunicationMode::kSync) {
          *send_result_ = std::move(send_result);
          if (!ProcessSendResult(producer, **send_result_)) {
            continue;
          }
        }
        return true;
      } catch (const std::exception& e) {
        ProcessException(producer, e);
        continue;
      }
    }
    return false;
  }

  bool ProcessSendResult(DefaultMQProducerImpl& producer, SendResult& send_result) {
    end_time_ = UtilAll::currentTimeMillis();
    producer.mq_fault_strategy_->UpdateFaultItem(last_broker_name_, end_time_ - begin_time_prev_, false);
    return send_result.send_status() == SendStatus::kSendOk || !retry_when_not_store_ok_;
  }

  void ProcessException(DefaultMQProducerImpl& producer, const std::exception& exception) {
    end_time_ = UtilAll::currentTimeMillis();
    producer.mq_fault_strategy_->UpdateFaultItem(last_broker_name_, end_time_ - begin_time_prev_, true);
    LOG_WARN_NEW("send failed of times:{}, brokerName:{}. exception:{}", retry_count_, last_broker_name_,
                 exception.what());
  }

  void operator()(ResultState<std::unique_ptr<SendResult>> state) noexcept {
    auto producer = producer_ptr_.lock();
    if (producer == nullptr) {
      try {
        send_callback_({std::move(state.GetResult())});
      } catch (...) {
        send_callback_({std::current_exception()});
      }
      return;
    }

    try {
      *send_result_ = std::move(state.GetResult());
      if (ProcessSendResult(*producer, **send_result_)) {
        send_callback_({std::move(*send_result_)});
        return;
      }
    } catch (const std::exception& e) {
      ProcessException(*producer, e);
    }

    if (DoSend(*producer)) {
      return;
    }

    if (*send_result_ != nullptr) {
      send_callback_({std::move(*send_result_)});
      return;
    }

    std::string error_message = "Send [" + UtilAll::to_string(retry_count_) + "] times, still failed, cost [" +
                                UtilAll::to_string(end_time_ - begin_time_first_) + "]ms, Topic: " + message_->topic();
    MQClientException exception(error_message, -1, __FILE__, __LINE__);
    send_callback_({std::make_exception_ptr(exception)});
  }

 private:
  std::weak_ptr<DefaultMQProducerImpl> producer_ptr_;
  MessagePtr message_;
  CommunicationMode communication_mode_;
  TopicPublishInfoPtr topic_publish_info_;
  std::string last_broker_name_;
  int retry_count_{0};
  int retry_times_;
  bool retry_when_not_store_ok_;
  int64_t begin_time_first_;
  int64_t begin_time_prev_;
  int64_t end_time_;
  int64_t deadline_;
  SendCallback send_callback_;
  std::shared_ptr<std::unique_ptr<SendResult>> send_result_{std::make_shared<std::unique_ptr<SendResult>>()};
};

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendDefaultImpl(const MessagePtr& message,
                                                                   CommunicationMode communication_mode,
                                                                   SendCallback send_callback,
                                                                   int64_t timeout) {
  Preprocess(*message, config());

  UnifiedSendDefaultImpl send_default_impl{*this, message, communication_mode, std::move(send_callback), timeout};

  return send_default_impl.SendDefault(*this);
}

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendToQueueImpl(const MessagePtr& message,
                                                                   const MessageQueue& message_queue,
                                                                   CommunicationMode communication_mode,
                                                                   SendCallback send_callback,
                                                                   int64_t timeout) {
  Preprocess(*message, message_queue.topic(), config());

  return SendKernelImpl(message, message_queue, communication_mode, std::move(send_callback), timeout);
}

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendSelectQueueImpl(const MessagePtr& message,
                                                                       const MessageQueueSelector& selector,
                                                                       CommunicationMode communication_mode,
                                                                       SendCallback send_callback,
                                                                       int64_t timeout) {
  Preprocess(*message, config());

  int64_t begin_time = UtilAll::currentTimeMillis();

  TopicPublishInfoPtr topic_publish_info = client_instance_->FindTopicPublishInfo(message->topic());
  if (topic_publish_info != nullptr && topic_publish_info->ok()) {
    auto message_queue = selector(topic_publish_info->getMessageQueueList(), message);

    int64_t cost_time = UtilAll::currentTimeMillis() - begin_time;
    if (timeout < cost_time) {
      THROW_MQEXCEPTION(RemotingTooMuchRequestException, "sendSelectImpl call timeout", -1);
    }

    return SendKernelImpl(message, message_queue, communication_mode, std::move(send_callback), timeout - cost_time);
  }

  THROW_MQEXCEPTION(MQClientException, "No route info of this topic: " + message->topic(), -1);
}

std::unique_ptr<SendResult> DefaultMQProducerImpl::SendKernelImpl(const MessagePtr& message,
                                                                  const MessageQueue& message_queue,
                                                                  CommunicationMode communication_mode,
                                                                  SendCallback send_callback,
                                                                  int64_t timeout) {
  int64_t begin_time = UtilAll::currentTimeMillis();

  auto broker_addr = client_instance_->FindBrokerAddressInPublish(message_queue);

  if (!broker_addr.empty()) {
    try {
      int system_flag = 0;
      if (UtilAll::stob(message->getProperty(MQMessageConst::PROPERTY_ALREADY_COMPRESSED_FLAG))) {
        system_flag |= MessageSysFlag::COMPRESSED_FLAG;
      }
      if (UtilAll::stob(message->getProperty(MQMessageConst::PROPERTY_TRANSACTION_PREPARED))) {
        system_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
      }

      // TOOD: send message hook

      auto request_header = MakeUnique<SendMessageRequestHeader>();
      request_header->producer_group = config().group_name();
      request_header->topic = message->topic();
      request_header->default_topic = AUTO_CREATE_TOPIC_KEY_TOPIC;
      request_header->default_topic_queue_nums = 4;
      request_header->queue_id = message_queue.queue_id();
      request_header->system_flag = system_flag;
      request_header->born_timestamp = UtilAll::currentTimeMillis();
      request_header->flag = message->flag();
      request_header->properties = MessageDecoder::messageProperties2String(message->properties());
      request_header->reconsume_times = 0;
      request_header->unit_mode = false;
      request_header->batch = message->isBatch();

      if (UtilAll::isRetryTopic(message_queue.topic())) {
        const auto& reconsume_time = MessageAccessor::getReconsumeTime(*message);
        if (!reconsume_time.empty()) {
          request_header->reconsume_times = std::stoi(reconsume_time);
          MessageAccessor::clearProperty(*message, MQMessageConst::PROPERTY_RECONSUME_TIME);
        }

        const auto& max_reconsume_times = MessageAccessor::getMaxReconsumeTimes(*message);
        if (!max_reconsume_times.empty()) {
          request_header->max_reconsume_times = std::stoi(max_reconsume_times);
          MessageAccessor::clearProperty(*message, MQMessageConst::PROPERTY_MAX_RECONSUME_TIMES);
        }
      }

      int64_t cost_time = UtilAll::currentTimeMillis() - begin_time;
      if (timeout < cost_time) {
        THROW_MQEXCEPTION(RemotingTooMuchRequestException, "sendKernelImpl call timeout", -1);
      }

      LOG_DEBUG_NEW("send to mq: {}", message_queue.ToString());
      return client_instance_->GetMQClientAPIImpl()->SendMessage(broker_addr, message_queue.broker_name(), message,
                                                                 std::move(request_header), timeout, communication_mode,
                                                                 std::move(send_callback));
    } catch (MQException& e) {
      throw;
    }
  }

  THROW_MQEXCEPTION(MQClientException, "The broker[" + message_queue.broker_name() + "] not exist", -1);
}

//
// Transaction

void DefaultMQProducerImpl::InitialTransactionEnv() {
  if (nullptr == check_transaction_executor_) {
    check_transaction_executor_ = MakeUnique<thread_pool_executor>(1, false);
  }
  check_transaction_executor_->startup();
}

void DefaultMQProducerImpl::DestroyTransactionEnv() {
  check_transaction_executor_->shutdown();
}

TransactionSendResult DefaultMQProducerImpl::SendMessageInTransaction(const MessagePtr& message, void* arg) {
  try {
    return *SendInTransactionImpl(message, arg, config().send_msg_timeout());
  } catch (MQException& e) {
    LOG_ERROR_NEW("sendMessageInTransaction failed, exception:{}", e.what());
    throw;
  }
}

std::unique_ptr<TransactionSendResult> DefaultMQProducerImpl::SendInTransactionImpl(const MessagePtr& message,
                                                                                    void* arg,
                                                                                    int64_t timeout) {
  auto* transaction_listener = config().transaction_listener();
  if (nullptr == transaction_listener) {
    THROW_MQEXCEPTION(MQClientException, "transactionListener is null", -1);
  }

  MessageAccessor::putProperty(*message, MQMessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
  MessageAccessor::putProperty(*message, MQMessageConst::PROPERTY_PRODUCER_GROUP, config().group_name());

  std::unique_ptr<SendResult> send_result;
  try {
    send_result = SendDefaultImpl(message, CommunicationMode::kSync, nullptr, timeout);
  } catch (MQException& e) {
    THROW_MQEXCEPTION(MQClientException, "send message Exception", -1);
  }

  LocalTransactionState local_transaction_state = LocalTransactionState::kUnknown;
  std::exception_ptr local_exception;
  switch (send_result->send_status()) {
    case SendStatus::kSendOk:
      try {
        if (!send_result->transaction_id().empty()) {
          message->putProperty("__transactionId__", send_result->transaction_id());
        }
        const auto& transaction_id = message->getProperty(MQMessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (!transaction_id.empty()) {
          message->set_transaction_id(transaction_id);
        }
        local_transaction_state = transaction_listener->executeLocalTransaction(MQMessage(message), arg);
        if (local_transaction_state != LocalTransactionState::kCommitMessage) {
          LOG_INFO_NEW("executeLocalTransaction return not COMMIT_MESSAGE, msg:{}", message->toString());
        }
      } catch (MQException& e) {
        LOG_INFO_NEW("executeLocalTransaction exception, msg:{}", message->toString());
        local_exception = std::current_exception();
      }
      break;
    case SendStatus::kSendFlushDiskTimeout:
    case SendStatus::kSendFlushSlaveTimeout:
    case SendStatus::kSendSlaveNotAvailable:
      local_transaction_state = LocalTransactionState::kRollbackMessage;
      LOG_WARN_NEW("sendMessageInTransaction, send not ok, rollback, result:{}", send_result->ToString());
      break;
    default:
      break;
  }

  try {
    EndTransaction(*send_result, local_transaction_state, local_exception);
  } catch (MQException& e) {
    LOG_WARN_NEW("local transaction execute {}, but end broker transaction failed: {}", local_transaction_state,
                 e.what());
  }

  // FIXME: setTransactionId will cause OOM?
  std::unique_ptr<TransactionSendResult> transaction_send_result(new TransactionSendResult(*send_result));
  transaction_send_result->set_transaction_id(message->transaction_id());
  transaction_send_result->set_local_transaction_state(local_transaction_state);
  return transaction_send_result;
}

void DefaultMQProducerImpl::checkTransactionState(const std::string& addr,
                                                  MessageExtPtr message,
                                                  CheckTransactionStateRequestHeader* check_request_header) {
  long transaction_state_table_offset = check_request_header->transaction_state_table_offset;
  long commit_log_offset = check_request_header->commit_log_offset;
  const auto& message_id = check_request_header->message_id;
  const auto& transaction_id = check_request_header->transaction_id;
  const auto& offset_message_id = check_request_header->offset_message_id;

  check_transaction_executor_->submit([this, addr, message, transaction_state_table_offset, commit_log_offset,
                                       message_id, transaction_id, offset_message_id] {
    CheckTransactionStateImpl(addr, message, transaction_state_table_offset, commit_log_offset, message_id,
                              transaction_id, offset_message_id);
  });
}

void DefaultMQProducerImpl::CheckTransactionStateImpl(const std::string& address,
                                                      const MessageExtPtr& message,
                                                      long transaction_state_table_offset,
                                                      long commit_log_offset,
                                                      const std::string& message_id,
                                                      const std::string& transaction_id,
                                                      const std::string& offset_message_id) {
  auto* transaction_check_listener = config().transaction_listener();
  if (nullptr == transaction_check_listener) {
    LOG_WARN_NEW("CheckTransactionState, pick transactionCheckListener by group[{}] failed", config().group_name());
    return;
  }

  LocalTransactionState local_transaction_state = LocalTransactionState::kUnknown;
  std::exception_ptr exception = nullptr;
  try {
    local_transaction_state = transaction_check_listener->checkLocalTransaction(MQMessageExt(message));
  } catch (MQException& e) {
    LOG_ERROR_NEW("Broker call checkTransactionState, but checkLocalTransactionState exception, {}", e.what());
    exception = std::current_exception();
  }

  auto request_header = MakeUnique<EndTransactionRequestHeader>();
  request_header->commit_log_offset = commit_log_offset;
  request_header->producer_group = config().group_name();
  request_header->transaction_state_table_offset = transaction_state_table_offset;
  request_header->from_transaction_check = true;

  std::string unique_key = message->getProperty(MQMessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
  if (unique_key.empty()) {
    unique_key = message->msg_id();
  }

  request_header->message_id = unique_key;
  request_header->transaction_id = transaction_id;
  switch (local_transaction_state) {
    case LocalTransactionState::kCommitMessage:
      request_header->commit_or_rollback = MessageSysFlag::TRANSACTION_COMMIT_TYPE;
      break;
    case LocalTransactionState::kRollbackMessage:
      request_header->commit_or_rollback = MessageSysFlag::TRANSACTION_ROLLBACK_TYPE;
      LOG_WARN_NEW("when broker check, client rollback this transaction, {}", request_header->toString());
      break;
    case LocalTransactionState::kUnknown:
      request_header->commit_or_rollback = MessageSysFlag::TRANSACTION_NOT_TYPE;
      LOG_WARN_NEW("when broker check, client does not know this transaction state, {}", request_header->toString());
      break;
    default:
      break;
  }

  std::string remark;
  if (exception != nullptr) {
    remark = "checkLocalTransactionState Exception: " + UtilAll::to_string(exception);
  }

  try {
    client_instance_->GetMQClientAPIImpl()->EndTransactionOneway(address, std::move(request_header), remark);
  } catch (std::exception& e) {
    LOG_ERROR_NEW("endTransactionOneway exception: {}", e.what());
  }
}

void DefaultMQProducerImpl::EndTransaction(SendResult& send_result,
                                           LocalTransactionState local_transaction_state,
                                           const std::exception_ptr& local_exception) {
  const auto& message_id =
      !send_result.offset_message_id().empty() ? send_result.offset_message_id() : send_result.message_id();
  auto id = MessageDecoder::decodeMessageId(message_id);
  const auto& transaction_id = send_result.transaction_id();
  std::string broker_address = client_instance_->FindBrokerAddressInPublish(send_result.message_queue().broker_name());

  auto request_header = MakeUnique<EndTransactionRequestHeader>();
  request_header->transaction_id = transaction_id;
  request_header->commit_log_offset = id.getOffset();
  switch (local_transaction_state) {
    case LocalTransactionState::kCommitMessage:
      request_header->commit_or_rollback = MessageSysFlag::TRANSACTION_COMMIT_TYPE;
      break;
    case LocalTransactionState::kRollbackMessage:
      request_header->commit_or_rollback = MessageSysFlag::TRANSACTION_ROLLBACK_TYPE;
      break;
    case LocalTransactionState::kUnknown:
      request_header->commit_or_rollback = MessageSysFlag::TRANSACTION_NOT_TYPE;
      break;
    default:
      break;
  }
  request_header->producer_group = config().group_name();
  request_header->transaction_state_table_offset = send_result.queue_offset();
  request_header->message_id = send_result.message_id();

  std::string remark =
      local_exception ? ("executeLocalTransactionBranch exception: " + UtilAll::to_string(local_exception)) : null;

  client_instance_->GetMQClientAPIImpl()->EndTransactionOneway(broker_address, std::move(request_header), remark);
}

//
// RPC

MessagePtr DefaultMQProducerImpl::Request(const MessagePtr& message, int64_t timeout) {
  return SyncRequestImpl(message, timeout,
                         [this](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
                           Send(message, std::move(send_callback), timeout);
                         });
}

void DefaultMQProducerImpl::Request(const MessagePtr& message,
                                    RequestCallback request_callback,
                                    int64_t timeout) noexcept {
  Expects(request_callback != nullptr);
  AsyncRequestImpl(message, timeout, std::move(request_callback),
                   [this](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
                     Send(message, std::move(send_callback), timeout);
                   });
}

MessagePtr DefaultMQProducerImpl::Request(const MessagePtr& message,
                                          const MessageQueue& message_queue,
                                          int64_t timeout) {
  return SyncRequestImpl(
      message, timeout,
      [this, &message_queue](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
        Send(message, message_queue, std::move(send_callback), timeout);
      });
}

void DefaultMQProducerImpl::Request(const MessagePtr& message,
                                    const MessageQueue& message_queue,
                                    RequestCallback request_callback,
                                    int64_t timeout) noexcept {
  Expects(request_callback != nullptr);
  AsyncRequestImpl(
      message, timeout, std::move(request_callback),
      [this, &message_queue](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
        Send(message, message_queue, std::move(send_callback), timeout);
      });
}

MessagePtr DefaultMQProducerImpl::Request(const MessagePtr& message, MessageQueueSelector selector, int64_t timeout) {
  return SyncRequestImpl(
      message, timeout,
      [this, &selector](const MessagePtr& message, SendCallback send_callback, int64_t timeout) noexcept {
        Send(message, std::move(selector), std::move(send_callback), timeout);
      });
}

void DefaultMQProducerImpl::Request(const MessagePtr& message,
                                    MessageQueueSelector selector,
                                    RequestCallback request_callback,
                                    int64_t timeout) noexcept {
  Expects(request_callback != nullptr);
  AsyncRequestImpl(
      message, timeout, std::move(request_callback),
      [this, &selector](const MessagePtr& message, SendCallback send_callback, int64_t timeout) mutable noexcept {
        Send(message, std::move(selector), std::move(send_callback), timeout);
      });
}

namespace {

const std::string& PrepareSendRequest(Message& message, int64_t timeout, MQClientInstance& client_instance) {
  MessageAccessor::putProperty(message, MQMessageConst::PROPERTY_CORRELATION_ID,
                               CorrelationIdUtil::createCorrelationId());
  MessageAccessor::putProperty(message, MQMessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
                               client_instance.GetClientId());
  MessageAccessor::putProperty(message, MQMessageConst::PROPERTY_MESSAGE_TTL, UtilAll::to_string(timeout));

  bool has_route_data = client_instance.GetTopicRouteData(message.topic()) != nullptr;
  if (!has_route_data) {
    int64_t begin_time = UtilAll::currentTimeMillis();
    client_instance.FindTopicPublishInfo(message.topic());
    client_instance.SendHeartbeatToAllBrokerWithLock();
    int64_t cost_time = UtilAll::currentTimeMillis() - begin_time;
    if (cost_time > 500) {
      LOG_WARN_NEW("prepare send request for <{}> cost {} ms", message.topic(), cost_time);
    }
  }
  return message.getProperty(MQMessageConst::PROPERTY_CORRELATION_ID);
}

}  // namespace

MessagePtr DefaultMQProducerImpl::SyncRequestImpl(const MessagePtr& message,
                                                  int64_t timeout,
                                                  const AsyncSendFunction& send_delegate) {
  int64_t begin_time = UtilAll::currentTimeMillis();

  const auto& correlation_id = PrepareSendRequest(*message, timeout, *client_instance_);
  auto response_future = std::make_shared<RequestResponseFuture>(correlation_id, timeout, nullptr);
  RequestFutureTable::putRequestFuture(correlation_id, response_future);

  int64_t cost_timeout = UtilAll::currentTimeMillis() - begin_time;

  send_delegate(
      message,
      [response_future](ResultState<std::unique_ptr<SendResult>> state) noexcept {
        try {
          (void)state.GetResult();
          response_future->set_send_request_ok(true);
        } catch (...) {
          response_future->set_send_request_ok(false);
          response_future->putResponseMessage(nullptr);
          response_future->set_cause(std::current_exception());
        }
      },
      timeout - cost_timeout);

  auto response_message = response_future->waitResponseMessage(timeout - cost_timeout);
  if (response_message != nullptr) {
    return response_message;
  }

  (void)RequestFutureTable::removeRequestFuture(correlation_id);
  if (response_future->send_request_ok()) {
    std::string error_message = "send request message to <" + message->topic() +
                                "> OK, but wait reply message timeout, " + UtilAll::to_string(timeout) + " ms.";
    THROW_MQEXCEPTION(RequestTimeoutException, error_message, ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION);
  }

  std::string error_message = "send request message to <" + message->topic() + "> fail";
  THROW_MQEXCEPTION2(MQClientException, error_message, -1, response_future->cause());
}

void DefaultMQProducerImpl::AsyncRequestImpl(const MessagePtr& message,
                                             int64_t timeout,
                                             RequestCallback request_callback,
                                             const AsyncSendFunction& send_delegate) noexcept {
  int64_t begin_time = UtilAll::currentTimeMillis();

  const auto& correlation_id = PrepareSendRequest(*message, timeout, *client_instance_);
  auto response_future = std::make_shared<RequestResponseFuture>(correlation_id, timeout, std::move(request_callback));
  RequestFutureTable::putRequestFuture(correlation_id, response_future);

  int64_t cost_time = UtilAll::currentTimeMillis() - begin_time;

  // kAsync
  send_delegate(
      message,
      [response_future](ResultState<std::unique_ptr<SendResult>> state) noexcept {
        try {
          (void)state.GetResult();
          response_future->set_send_request_ok(true);
        } catch (...) {
          response_future->set_cause(std::current_exception());
          if (RequestFutureTable::removeRequestFuture(response_future->correlation_id()) != nullptr) {
            response_future->set_send_request_ok(false);
            response_future->putResponseMessage(nullptr);
            try {
              response_future->executeRequestCallback();
            } catch (std::exception& e) {
              LOG_WARN_NEW("execute request_callback in request fail, and callback throw {}", e.what());
            }
          }
        }
      },
      timeout - cost_time);
}

}  // namespace rocketmq
