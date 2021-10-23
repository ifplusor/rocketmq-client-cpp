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
#include "RebalancePushImpl.h"

#include <memory>
#include <mutex>

#include "AllocateMQAveragely.h"
#include "MQClientAPIImpl.h"
#include "MessageQueue.hpp"
#include "OffsetStore.h"
#include "UtilAll.h"
#include "common/FindBrokerResult.hpp"
#include "proxy/consuming/LogicalQueueSet.hpp"

namespace rocketmq {

RebalancePushImpl::RebalancePushImpl(DefaultMQPushConsumerImpl* consumerImpl)
    : RebalanceImpl(null, CLUSTERING, AllocateMQAveragely, nullptr), default_mq_push_consumer_impl_(consumerImpl) {}

bool RebalancePushImpl::Lock(const MessageQueue& mq) {
  auto findBrokerResult = client_instance()->FindBrokerAddressInSubscribe(mq.broker_name(), MASTER_ID, true);
  if (findBrokerResult) {
    try {
      LOG_DEBUG_NEW("try to lock mq:{}", mq.ToString());

      std::vector<MessageQueue> lockedMq = client_instance()->GetMQClientAPIImpl()->LockBatchMQ(
          findBrokerResult.broker_addr, consumer_group(), client_instance()->GetClientId(), {mq}, 1000);

      bool lockOK = false;
      if (!lockedMq.empty()) {
        for (const auto& mmqq : lockedMq) {
          ProcessQueuePtr processQueue = getProcessQueue(mq);
          if (processQueue != nullptr) {
            processQueue->set_locked(true);
            processQueue->set_last_lock_timestamp(UtilAll::currentTimeMillis());
          }

          if (mmqq == mq) {
            lockOK = true;
          }
        }
      }

      LOG_INFO_NEW("the message queue lock {}, {} {}", lockOK ? "OK" : "Failed", consumer_group(), mq.ToString());
      return lockOK;
    } catch (MQException& e) {
      LOG_ERROR_NEW("lockBatchMQ exception, mq:{}", mq.ToString());
    }
  } else {
    LOG_ERROR_NEW("lock: findBrokerAddressInSubscribe() returen null for broker:{}", mq.broker_name());
  }

  return false;
}

void RebalancePushImpl::LockAll() {
  auto brokerMqs = buildProcessQueueTableByBrokerName();
  LOG_INFO_NEW("LockAll {} broker mqs", brokerMqs->size());

  for (const auto& it : *brokerMqs) {
    const std::string& brokerName = it.first;
    const std::vector<MessageQueue>& mqs = it.second;

    if (mqs.empty()) {
      continue;
    }

    auto findBrokerResult = client_instance()->FindBrokerAddressInSubscribe(brokerName, MASTER_ID, true);
    if (findBrokerResult) {
      try {
        LOG_INFO_NEW("try to lock:{} mqs of broker:{}", mqs.size(), brokerName);

        std::vector<MessageQueue> lockOKMQVec = client_instance()->GetMQClientAPIImpl()->LockBatchMQ(
            findBrokerResult.broker_addr, consumer_group(), client_instance()->GetClientId(), mqs, 1000);

        std::set<MessageQueue> lockOKMQSet;
        for (const auto& mq : lockOKMQVec) {
          lockOKMQSet.insert(mq);

          ProcessQueuePtr processQueue = getProcessQueue(mq);
          if (processQueue != nullptr) {
            if (!processQueue->locked()) {
              LOG_INFO_NEW("the message queue locked OK, Group: {} {}", consumer_group(), mq.ToString());
            }

            processQueue->set_locked(true);
            processQueue->set_last_lock_timestamp(UtilAll::currentTimeMillis());
          }
        }

        for (const auto& mq : mqs) {
          if (lockOKMQSet.find(mq) == lockOKMQSet.end()) {
            ProcessQueuePtr processQueue = getProcessQueue(mq);
            if (processQueue != nullptr) {
              processQueue->set_locked(false);
              LOG_WARN_NEW("the message queue locked Failed, Group: {} {}", consumer_group(), mq.ToString());
            }
          }
        }
      } catch (MQException& e) {
        LOG_ERROR_NEW("lockBatchMQ fails");
      }
    } else {
      LOG_ERROR_NEW("lockAll: findBrokerAddressInSubscribe() return null for broker:{}", brokerName);
    }
  }
}

void RebalancePushImpl::Unlock(const MessageQueue& mq, bool oneway) {
  auto findBrokerResult = client_instance()->FindBrokerAddressInSubscribe(mq.broker_name(), MASTER_ID, true);
  if (findBrokerResult) {
    try {
      client_instance()->GetMQClientAPIImpl()->UnlockBatchMQ(findBrokerResult.broker_addr, consumer_group(),
                                                             client_instance()->GetClientId(), {mq}, 1000, oneway);

      ProcessQueuePtr processQueue = getProcessQueue(mq);
      if (processQueue != nullptr) {
        processQueue->set_locked(false);
      }

      LOG_WARN_NEW("unlock messageQueue. group:{}, clientId:{}, mq:{}", consumer_group(),
                   client_instance()->GetClientId(), mq.ToString());
    } catch (MQException& e) {
      LOG_ERROR_NEW("unlockBatchMQ exception, mq:{}", mq.ToString());
    }
  } else {
    LOG_WARN("unlock findBrokerAddressInSubscribe ret null for broker:{}", mq.broker_name());
  }
}

void RebalancePushImpl::UnlockAll(bool oneway) {
  auto brokerMqs = buildProcessQueueTableByBrokerName();
  LOG_INFO_NEW("unLockAll {} broker mqs", brokerMqs->size());

  for (const auto& it : *brokerMqs) {
    const std::string& brokerName = it.first;
    const std::vector<MessageQueue>& mqs = it.second;

    if (mqs.empty()) {
      continue;
    }

    auto findBrokerResult = client_instance()->FindBrokerAddressInSubscribe(brokerName, MASTER_ID, true);
    if (findBrokerResult) {
      try {
        client_instance()->GetMQClientAPIImpl()->UnlockBatchMQ(findBrokerResult.broker_addr, consumer_group(),
                                                               client_instance()->GetClientId(), mqs, 1000, oneway);
        for (const auto& mq : mqs) {
          ProcessQueuePtr processQueue = getProcessQueue(mq);
          if (processQueue != nullptr) {
            processQueue->set_locked(false);
            LOG_INFO_NEW("the message queue unlock OK, Group: {} {}", consumer_group(), mq.ToString());
          }
        }
      } catch (MQException& e) {
        LOG_ERROR_NEW("unlockBatchMQ exception");
      }
    } else {
      LOG_ERROR_NEW("unlockAll findBrokerAddressInSubscribe ret null for broker:{}", brokerName);
    }
  }
}

std::shared_ptr<BROKER2MQS> RebalancePushImpl::buildProcessQueueTableByBrokerName() {
  auto brokerMqs = std::make_shared<BROKER2MQS>();
  auto processQueueTable = getProcessQueueTable();
  for (const auto& it : processQueueTable) {
    const auto& mq = it.first;
    std::string brokerName = mq.broker_name();
    if (brokerMqs->find(brokerName) == brokerMqs->end()) {
      brokerMqs->emplace(brokerName, std::vector<MessageQueue>());
    }
    (*brokerMqs)[brokerName].push_back(mq);
  }
  return brokerMqs;
}

void RebalancePushImpl::shutdown() {
  std::lock_guard<std::mutex> lock(process_queue_table_mutex_);
  for (const auto& it : process_queue_table_) {
    it.second->set_dropped(true);
  }
  process_queue_table_.clear();
}

bool RebalancePushImpl::updateMessageQueueInRebalance(const std::string& topic,
                                                      std::vector<MessageQueue>& allocated_mqs,
                                                      bool orderly) {
  return updateProcessQueueTableInRebalance(topic, allocated_mqs, orderly);
}

bool RebalancePushImpl::updateProcessQueueTableInRebalance(const std::string& topic,
                                                           std::vector<MessageQueue>& allocated_mqs,
                                                           bool orderly) {
  LOG_DEBUG("updateRequestTableInRebalance Enter");

  bool changed = false;

  // remove
  MQ2PQ processQueueTable(getProcessQueueTable());  // get copy of process_queue_table_
  for (const auto& it : processQueueTable) {
    const auto& mq = it.first;
    auto pq = it.second;

    if (mq.topic() == topic) {
      if (allocated_mqs.empty() || (find(allocated_mqs.begin(), allocated_mqs.end(), mq) == allocated_mqs.end())) {
        pq->set_dropped(true);
        if (removeUnnecessaryMessageQueue(mq, pq)) {
          removeProcessQueueDirectly(mq);
          changed = true;
          LOG_INFO_NEW("doRebalance, {}, remove unnecessary mq, {}", consumer_group(), mq.ToString());
        }
      } else if (pq->IsPullExpired()) {
        switch (consumeType()) {
          case CONSUME_ACTIVELY:
            break;
          case CONSUME_PASSIVELY:
            pq->set_dropped(true);
            if (removeUnnecessaryMessageQueue(mq, pq)) {
              removeProcessQueueDirectly(mq);
              changed = true;
              LOG_ERROR_NEW(
                  "[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                  consumer_group(), mq.ToString());
            }
            break;
          default:
            break;
        }
      }
    }
  }

  // update
  std::vector<PullRequestPtr> pull_request_list;
  for (const auto& mq : allocated_mqs) {
    ProcessQueuePtr pq = getProcessQueue(mq);
    if (nullptr == pq) {
      if (orderly && !Lock(mq)) {
        LOG_WARN_NEW("doRebalance, {}, add a new mq failed, {}, because lock failed", consumer_group(), mq.ToString());
        continue;
      }

      removeDirtyOffset(mq);
      pq = std::make_shared<ProcessQueue>(mq);
      int64_t nextOffset = computePullFromWhere(mq);
      if (nextOffset >= 0) {
        auto pre = putProcessQueueIfAbsent(mq, pq);
        if (pre) {
          LOG_INFO_NEW("doRebalance, {}, mq already exists, {}", consumer_group(), mq.ToString());
        } else {
          LOG_INFO_NEW("doRebalance, {}, add a new mq, {}", consumer_group(), mq.ToString());
          auto pull_request = std::make_shared<PullRequest>(consumer_group(), pq);
          pull_request->set_next_offset(nextOffset);
          pull_request_list.push_back(std::move(pull_request));
          changed = true;
        }
      } else {
        LOG_WARN_NEW("doRebalance, {}, add new mq failed, {}", consumer_group(), mq.ToString());
      }
    }
  }

  default_mq_push_consumer_impl_->DispatchPullRequest(pull_request_list);

  LOG_DEBUG_NEW("updateRequestTableInRebalance exit");
  return changed;
}

bool RebalancePushImpl::removeUnnecessaryMessageQueue(const MessageQueue& mq, ProcessQueuePtr pq) {
  auto* offset_store = default_mq_push_consumer_impl_->offset_store();

  offset_store->persist(mq);
  offset_store->removeOffset(mq);

  if (default_mq_push_consumer_impl_->consume_orderly() &&
      CLUSTERING == default_mq_push_consumer_impl_->messageModel()) {
    try {
      if (UtilAll::try_lock_for(pq->consume_mutex(), 1000)) {
        std::lock_guard<std::timed_mutex> lock(pq->consume_mutex(), std::adopt_lock);
        // TODO: unlockDelay
        Unlock(mq);
        return true;
      }

      LOG_WARN("[WRONG] mq is consuming, so can not unlock it, %s. maybe hanged for a while, %ld",
               mq.ToString().c_str(), pq->try_unlock_times());
      pq->inc_try_unlock_times();
    } catch (const std::exception& e) {
      LOG_ERROR("removeUnnecessaryMessageQueue Exception: %s", e.what());
    }

    return false;
  }

  return true;
}

void RebalancePushImpl::removeDirtyOffset(const MessageQueue& mq) {
  default_mq_push_consumer_impl_->offset_store()->removeOffset(mq);
}

int64_t RebalancePushImpl::computePullFromWhere(const MessageQueue& mq) {
  return RebalanceImpl::computePullFromWhereImpl(mq, default_mq_push_consumer_impl_->config().consume_from_where(),
                                                 default_mq_push_consumer_impl_->config().consume_timestamp(),
                                                 *default_mq_push_consumer_impl_->offset_store(),
                                                 *default_mq_push_consumer_impl_);
}

void RebalancePushImpl::messageQueueChanged(const std::string& topic,
                                            std::vector<MessageQueue>& mqAll,
                                            std::vector<MessageQueue>& mqDivided) {
  // TODO: update subscription's version
}

void RebalancePushImpl::truncateMessageQueueNotMyTopic() {
  auto& subTable = getSubscriptionInner();
  std::vector<MessageQueue> mqs = getAllocatedMQ();
  for (const auto& mq : mqs) {
    if (subTable.find(mq.topic()) == subTable.end()) {
      auto pq = removeProcessQueueDirectly(mq);
      if (pq != nullptr) {
        pq->set_dropped(true);
        LOG_INFO_NEW("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumer_group(),
                     mq.ToString());
      }
    }
  }
}

void RebalancePushImpl::removeProcessQueue(const MessageQueue& mq) {
  std::lock_guard<std::mutex> lock(process_queue_table_mutex_);
  const auto& it = process_queue_table_.find(mq);
  if (it != process_queue_table_.end()) {
    auto prev = it->second;
    process_queue_table_.erase(it);

    bool dropped = prev->dropped();
    prev->set_dropped(true);
    removeUnnecessaryMessageQueue(mq, prev);
    LOG_INFO_NEW("Fix Offset, {}, remove unnecessary mq, {} Dropped: {}", consumer_group(), mq.ToString(),
                 UtilAll::to_string(dropped));
  }
}

ProcessQueuePtr RebalancePushImpl::removeProcessQueueDirectly(const MessageQueue& mq) {
  std::lock_guard<std::mutex> lock(process_queue_table_mutex_);
  const auto& it = process_queue_table_.find(mq);
  if (it != process_queue_table_.end()) {
    auto old = it->second;
    process_queue_table_.erase(it);
    return old;
  }
  return nullptr;
}

ProcessQueuePtr RebalancePushImpl::putProcessQueueIfAbsent(const MessageQueue& mq, ProcessQueuePtr pq) {
  std::lock_guard<std::mutex> lock(process_queue_table_mutex_);
  const auto& it = process_queue_table_.find(mq);
  if (it != process_queue_table_.end()) {
    return it->second;
  } else {
    process_queue_table_[mq] = pq;
    return nullptr;
  }
}

ProcessQueuePtr RebalancePushImpl::getProcessQueue(const MessageQueue& mq) {
  std::lock_guard<std::mutex> lock(process_queue_table_mutex_);
  const auto& it = process_queue_table_.find(mq);
  if (it != process_queue_table_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

MQ2PQ RebalancePushImpl::getProcessQueueTable() {
  std::lock_guard<std::mutex> lock(process_queue_table_mutex_);
  return process_queue_table_;
}

std::vector<MessageQueue> RebalancePushImpl::getAllocatedMQ() {
  std::vector<MessageQueue> mqs;
  std::lock_guard<std::mutex> lock(process_queue_table_mutex_);
  for (const auto& it : process_queue_table_) {
    mqs.push_back(it.first);
  }
  return mqs;
}

}  // namespace rocketmq
