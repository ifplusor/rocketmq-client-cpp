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
#include "ConsumeMsgService.h"

#include "Logging.h"
#include "OffsetStore.h"
#include "RebalancePushImpl.h"
#include "UtilAll.h"

namespace rocketmq {

const uint64_t MAX_TIME_CONSUME_CONTINUOUSLY = 60000;
const uint64_t kRebalanceLockInterval = 20000;  // ms

ConsumeMessageOrderlyService::ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl* consumer,
                                                           int threadCount,
                                                           MessageListener msgListener)
    : consumer_(consumer),
      message_listener_(msgListener),
      consume_executor_("OderlyConsumeService", threadCount, false),
      scheduled_executor_service_(false) {}

ConsumeMessageOrderlyService::~ConsumeMessageOrderlyService() = default;

void ConsumeMessageOrderlyService::start() {
  consume_executor_.startup();

  scheduled_executor_service_.startup();
  scheduled_executor_service_.schedule([this]() { lockMQPeriodically(); }, kRebalanceLockInterval,
                                       time_unit::milliseconds);
}

void ConsumeMessageOrderlyService::shutdown() {
  stopThreadPool();
  unlockAllMQ();
}

void ConsumeMessageOrderlyService::stopThreadPool() {
  consume_executor_.shutdown();
  scheduled_executor_service_.shutdown();
}

void ConsumeMessageOrderlyService::lockMQPeriodically() {
  consumer_->rebalance_impl()->LockAll();

  scheduled_executor_service_.schedule([this] { lockMQPeriodically(); }, kRebalanceLockInterval,
                                       time_unit::milliseconds);
}

void ConsumeMessageOrderlyService::unlockAllMQ() {
  consumer_->rebalance_impl()->UnlockAll(false);
}

bool ConsumeMessageOrderlyService::lockOneMQ(const MessageQueue& mq) {
  return consumer_->rebalance_impl()->Lock(mq);
}

void ConsumeMessageOrderlyService::submitConsumeRequest(std::vector<MessageExtPtr>& msgs,
                                                        ProcessQueuePtr processQueue,
                                                        const bool dispathToConsume) {
  if (dispathToConsume) {
    consume_executor_.submit([this, processQueue]() mutable { ConsumeRequest(std::move(processQueue)); });
  }
}

void ConsumeMessageOrderlyService::submitConsumeRequestLater(ProcessQueuePtr processQueue,
                                                             const long suspendTimeMillis) {
  long timeMillis = suspendTimeMillis;
  if (timeMillis == -1) {
    timeMillis = 1000;
  }

  timeMillis = std::max(10L, std::min(timeMillis, 30000L));

  scheduled_executor_service_.schedule(
      [this, processQueue]() mutable {
        std::vector<MessageExtPtr> dummy;
        submitConsumeRequest(dummy, std::move(processQueue), true);
      },
      timeMillis, time_unit::milliseconds);
}

void ConsumeMessageOrderlyService::tryLockLaterAndReconsume(ProcessQueuePtr processQueue, const long delayMills) {
  scheduled_executor_service_.schedule(
      [this, processQueue]() {
        const auto& mq = processQueue->message_queue();
        bool lockOK = lockOneMQ(mq);
        if (lockOK) {
          submitConsumeRequestLater(processQueue, 10);
        } else {
          submitConsumeRequestLater(processQueue, 3000);
        }
      },
      delayMills, time_unit::milliseconds);
}

void ConsumeMessageOrderlyService::ConsumeRequest(ProcessQueuePtr processQueue) {
  const auto& messageQueue = processQueue->message_queue();
  if (processQueue->dropped()) {
    LOG_WARN_NEW("run, the message queue not be able to consume, because it's dropped. {}", messageQueue.ToString());
    return;
  }

  // TODO: 优化锁，或在线程中做hash
  auto objLock = message_queue_lock_.fetchLockObject(messageQueue);
  std::lock_guard<std::mutex> lock(*objLock);

  if (BROADCASTING == consumer_->messageModel() || (processQueue->locked() && !processQueue->IsLockExpired())) {
    auto beginTime = UtilAll::currentTimeMillis();
    for (bool continueConsume = true; continueConsume;) {
      if (processQueue->dropped()) {
        LOG_WARN_NEW("the message queue not be able to consume, because it's dropped. {}", messageQueue.ToString());
        break;
      }

      if (CLUSTERING == consumer_->messageModel() && !processQueue->locked()) {
        LOG_WARN_NEW("the message queue not locked, so consume later, {}", messageQueue.ToString());
        tryLockLaterAndReconsume(processQueue, 10);
        break;
      }

      if (CLUSTERING == consumer_->messageModel() && processQueue->IsLockExpired()) {
        LOG_WARN_NEW("the message queue lock expired, so consume later, {}", messageQueue.ToString());
        tryLockLaterAndReconsume(processQueue, 10);
        break;
      }

      auto interval = UtilAll::currentTimeMillis() - beginTime;
      if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
        submitConsumeRequestLater(processQueue, 10);
        break;
      }

      const int consumeBatchSize = consumer_->config().consume_message_batch_max_size();

      std::vector<MessageExtPtr> msgs = processQueue->TakeMessages(consumeBatchSize);
      consumer_->ResetRetryAndNamespace(msgs);
      if (!msgs.empty()) {
        ConsumeStatus status = RECONSUME_LATER;
        try {
          std::lock_guard<std::timed_mutex> lock(processQueue->consume_mutex());
          if (processQueue->dropped()) {
            LOG_WARN_NEW("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                         messageQueue.ToString());
            break;
          }
          status = message_listener_(msgs);
        } catch (const std::exception& e) {
          LOG_WARN_NEW("encounter unexpected exception when consume messages.\n{}", e.what());
        }

        // processConsumeResult
        long commitOffset = -1L;
        switch (status) {
          case CONSUME_SUCCESS:
            commitOffset = processQueue->Commit();
            break;
          case RECONSUME_LATER:
            processQueue->MakeMessagesToCosumeAgain(msgs);
            submitConsumeRequestLater(processQueue, -1);
            continueConsume = false;
            break;
          default:
            break;
        }

        if (commitOffset >= 0 && !processQueue->dropped()) {
          consumer_->offset_store()->updateOffset(messageQueue, commitOffset, false);
        }
      } else {
        continueConsume = false;
      }
    }
  } else {
    if (processQueue->dropped()) {
      LOG_WARN_NEW("the message queue not be able to consume, because it's dropped. {}", messageQueue.ToString());
      return;
    }

    tryLockLaterAndReconsume(processQueue, 100);
  }
}

}  // namespace rocketmq
