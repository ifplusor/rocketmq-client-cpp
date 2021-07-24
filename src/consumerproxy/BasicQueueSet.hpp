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
#ifndef ROCKETMQ_CONSUMERPROXY_BASICQUEUESET_HPP_
#define ROCKETMQ_CONSUMERPROXY_BASICQUEUESET_HPP_

/*

  QueueSet synopsis:

namespace rocketmq {

template <typename Queue>
class BasicQueueSet {
 public:
  using QueueType = Queue;
  using IdentityType = typename QueueType::IdentityType;
  using MessageType = typename QueueType::MessageType;
  using IndexType = typename QueueType::IndexType;

  std::tuple<std::vector<IdentityType>, std::vector<IdentityType>> AssignQueues(std::vector<IdentityType> queues);

  std::shared_ptr<QueueType> GetQueue(const IdentityType& identity);

  std::vector<MessageType> Poll(size_t batch_size);

  template <class Rep, class Period>
  std::vector<MessageType> Poll(size_t batch_size, const std::chrono::duration<Rep, Period>& timeout);

  template <class Rep, class Period>
  void Poll(size_t batch_size,
            std::function<void(std::vector<MessageType>)> callback,
            const std::chrono::duration<Rep, Period>& timeout);

  template <class Rep, class Period>
  void TryWakeupBlockingQueueRequest(const std::chrono::duration<Rep, Period>& timeout);
};

}  // namespace rocketmq

*/

#include <cstddef>  // size_t
#include <cstdint>  // int64_t
#include <cstdlib>  // std::abort

#include <algorithm>           // std::binary_search, std::sort
#include <chrono>              // std::chrono::duration, std::chrono::steady_clock, std::chrono::time_point
#include <condition_variable>  // std::condition_variable
#include <exception>           // std::exception
#include <functional>          // std::function, std::greater
#include <future>              // std::future, std::promise
#include <map>                 // std::map
#include <memory>              // std::shared_ptr
#include <mutex>               // std::lock_guard, std::mutex, std::unique_lock
#include <queue>               // std::priority_queue, std::queue
#include <tuple>               // std::tuple
#include <utility>             // std::move
#include <vector>              // std::vector

#include "consumerproxy/ConsumeStrategy.hpp"
#include "consumerproxy/FlowControlStrategy.hpp"

namespace rocketmq {

namespace detail {

template <typename Queue>
struct QueueRequest {
  std::chrono::time_point<std::chrono::steady_clock> weakup;
  std::function<void(std::shared_ptr<Queue>)> callback;

  QueueRequest(std::chrono::time_point<std::chrono::steady_clock> weakup,
               std::function<void(std::shared_ptr<Queue>)> callback)
      : weakup(weakup), callback(std::move(callback)) {}

  void operator()(std::shared_ptr<Queue> queue) const noexcept try {
    callback(std::move(queue));
  } catch (const std::exception& e) {
    // encounter exception
    std::abort();
  }

  bool operator>(const QueueRequest& other) const { return weakup > other.weakup; }
};

}  // namespace detail

/**
 * @brief support poll multiplexing on ProcessQueue(s)
 *
 * @tparam Queue
 */
template <typename Queue, template <typename, typename> class ConsumeStrategy = SharedConsumeStrategy>
class BasicQueueSet : private ConsumeStrategy<BasicQueueSet<Queue, ConsumeStrategy>, Queue>,
                      private PartialCompleteFlowControlStrategy<BasicQueueSet<Queue, ConsumeStrategy>, Queue> {
  using ConsumeStrategyType = ConsumeStrategy<BasicQueueSet<Queue, ConsumeStrategy>, Queue>;
  friend ConsumeStrategyType;

  using FlowControlStrategyType = PartialCompleteFlowControlStrategy<BasicQueueSet<Queue, ConsumeStrategy>, Queue>;
  friend FlowControlStrategyType;

 public:
  using QueueType = Queue;
  using IdentityType = typename QueueType::IdentityType;
  using MessageType = typename QueueType::MessageType;
  using IndexType = typename QueueType::IndexType;

 private:
  using QueueRequest = detail::QueueRequest<QueueType>;

 public:
  BasicQueueSet() = default;

  void Assign(std::vector<IdentityType> assigned,
              const std::function<void(const std::shared_ptr<QueueType>&)>& after_insert,
              const std::function<void(std::shared_ptr<QueueType>)>& after_remove) {
    std::lock_guard<std::mutex> assigned_queues_lock(assigned_queues_mutex_);
    Update(Diff(std::move(assigned)), after_insert, after_remove);
  }

  /**
   * @brief synchronous non-blocking poll
   *
   * @param batch_size
   * @return std::vector<MessageType>
   */
  std::tuple<std::vector<MessageType>, std::shared_ptr<QueueType>> Poll(size_t batch_size) {
    auto ready_queue = TryPopReadyQueue();
    return PollImpl(std::move(ready_queue), batch_size);
  }

  /**
   * @brief synchronous blocking poll
   *
   * @tparam Rep
   * @tparam Period
   * @param batch_size
   * @param timeout
   * @return std::vector<MessageType>
   */
  template <class Rep, class Period>
  std::tuple<std::vector<MessageType>, std::shared_ptr<QueueType>> Poll(
      size_t batch_size,
      const std::chrono::duration<Rep, Period>& timeout) {
    auto ready_queue = TryPopReadyQueue();
    if (!ready_queue) {
      ready_queue = WaitReadyQueue(timeout);
    }
    return PollImpl(std::move(ready_queue), batch_size);
  }

  /**
   * @brief asynchronous poll
   *
   * @tparam Rep
   * @tparam Period
   * @param batch_size
   * @param callback
   * @param timeout
   */
  template <class Rep, class Period>
  void Poll(size_t batch_size,
            std::function<void(std::tuple<std::vector<MessageType>, std::shared_ptr<QueueType>>)> callback,
            const std::chrono::duration<Rep, Period>& timeout) {
    // FIXME: lifecycle of this
    WaitReadyQueue(
        [this, batch_size, callback](std::shared_ptr<QueueType> ready_queue) {
          callback(PollImpl(std::move(ready_queue), batch_size));
        },
        timeout);
  }

  template <typename InputIterator>
  void Commit(const std::shared_ptr<QueueType>& queue, InputIterator first, InputIterator last) {
    CommitImpl(queue, [&first, &last](const std::shared_ptr<QueueType>& queue) { return queue->Commit(first, last); });
  }

  void Commit(const std::shared_ptr<QueueType>& queue, const MessageType& message) {
    CommitImpl(queue, [&message](const std::shared_ptr<QueueType>& queue) { return queue->Commit(message); });
  }

  template <typename InputIterator>
  void Rollback(const std::shared_ptr<QueueType>& queue, InputIterator first, InputIterator last) {
    std::unique_ptr<std::mutex> queue_lock(queue->mutex());
    ConsumeStrategyType::AfterConsume(queue, queue->Rollback(first, last));
  }

  void Rollback(const std::shared_ptr<QueueType>& queue, const MessageType& message) {
    std::unique_ptr<std::mutex> queue_lock(queue->mutex());
    ConsumeStrategyType::AfterConsume(queue, queue->Rollback(message));
  }

  template <class Rep, class Period>
  void TryWakeupPoll(const std::chrono::duration<Rep, Period>& timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;

    std::unique_lock<std::mutex> queue_requests_lock(queue_requests_mutex_);
    while (true) {
      while (!queue_requests_.empty()) {
        const auto& queue_request = queue_requests_.top();

        auto ready_queue = TryPopReadyQueue();
        if (ready_queue) {
          queue_request(std::move(ready_queue));
          queue_requests_.pop();
          continue;
        }

        if (queue_request.weakup <= deadline) {
          if (queue_requests_cv_.wait_until(queue_requests_lock, queue_request.weakup) == std::cv_status::timeout) {
            // request timeout
            queue_request(nullptr);
            queue_requests_.pop();
          }
        } else {
          if (queue_requests_cv_.wait_until(queue_requests_lock, deadline) == std::cv_status::timeout) {
            // call timeout
            return;
          }
        }
      }

      if (queue_requests_cv_.wait_until(queue_requests_lock, deadline) == std::cv_status::timeout) {
        return;
      }
    }
  }

  template <class Rep, class Period>
  void TryPullMessages(
      const std::function<void(std::shared_ptr<QueueType> /* pull_queue */,
                               std::function<void(std::vector<MessageType>)> /* callback */)>& delegate,
      const std::chrono::duration<Rep, Period>& timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;

    std::unique_lock<std::mutex> pull_queues_lock(pull_queues_mutex_);
    while (true) {
      for (int i = 0; i < 31; ++i) {
        auto pull_queue = TryPopQueue(pull_queues_);
        if (!pull_queue) {
          break;
        }

        pull_queues_lock.unlock();
        delegate(pull_queue,
                 [this, pull_queue](std::vector<MessageType> messages) { PutImpl(pull_queue, std::move(messages)); });
        pull_queues_lock.lock();
      }

      if (pull_queues_.empty()) {
        // wait pull queue
        if (pull_queues_cv_.wait_until(pull_queues_lock, deadline) == std::cv_status::timeout) {
          break;
        }
      } else {
        if (std::chrono::steady_clock::now() >= deadline) {
          break;
        }
      }
    }
  }

  std::shared_ptr<QueueType> At(const IdentityType& identity) {
    std::lock_guard<std::mutex> lock(assigned_queues_mutex_);
    return assigned_queues_.at(identity);
  }

 private:
  std::tuple<std::vector<IdentityType>, std::vector<IdentityType>> Diff(std::vector<IdentityType> assigned) {
    std::sort(assigned.begin(), assigned.end());

    // remove expried queue
    std::vector<IdentityType> removed_queues;
    for (auto it = assigned_queues_.begin(); it != assigned_queues_.end();) {
      const auto& identity = it->first;
      if (!std::binary_search(assigned.begin(), assigned.end(), identity)) {
        removed_queues.push_back(identity);
      }
    }

    // add new queue
    std::vector<IdentityType> added_queues;
    for (auto it = assigned.begin(); it != assigned.end();) {
      auto& identity = *it;
      if (assigned_queues_.find(identity) == assigned_queues_.end()) {
        added_queues.push_back(std::move(identity));
        it = assigned_queues_.erase(it);
      } else {
        ++it;
      }
    }

    return std::make_tuple(std::move(removed_queues), std::move(added_queues));
  }

  void Update(std::tuple<std::vector<IdentityType>, std::vector<IdentityType>> removed_and_added,
              const std::function<void(const std::shared_ptr<QueueType>&)>& after_insert,
              const std::function<void(std::shared_ptr<QueueType>)>& after_remove) {
    // remove expried queue
    const auto& removed_queues = std::get<0>(removed_and_added);
    for (const auto& identity : removed_queues) {
      // TODO: optimize flow control
      Erase(identity, after_remove);
    }

    // add new queue
    const auto& added_queues = std::get<1>(removed_and_added);
    for (const auto& identity : added_queues) {
      Insert(identity, after_insert);
    }
  }

  void Insert(IdentityType identity, const std::function<void(const std::shared_ptr<QueueType>&)>& after_insert) {
    auto it = assigned_queues_.find(identity);
    if (it != assigned_queues_.end()) {
      return;
    }
    auto queue = std::make_shared<QueueType>(identity);
    assigned_queues_.emplace(std::move(identity), queue);
    if (after_insert) {
      after_insert(queue);
    }
    PushPullQueue(std::move(queue));
  }

  void Erase(const IdentityType& identity, const std::function<void(std::shared_ptr<QueueType>)>& after_erase) {
    auto it = assigned_queues_.find(identity);
    if (it == assigned_queues_.end()) {
      return;
    }

    auto queue = std::move(it.second);
    std::unique_lock<std::mutex> queue_lock(queue->mutex());
    assigned_queues_.erase(it);
    queue->Drop();

    // flow control
    FlowControlStrategyType::AfterDrop(queue);

    if (after_erase) {
      after_erase(std::move(queue));
    }
  }

  std::shared_ptr<QueueType> TryPopQueue(std::queue<std::shared_ptr<QueueType>> queues) {
    if (queues.empty()) {
      return {};
    }

    // pop queue
    auto queue = std::move(queues.front());
    // pop again if queue is dropped
    while (queue->dropped()) {
      queues.pop();
      if (queues.empty()) {
        return {};
      }
      queue = std::move(queues.front());
    }
    queues.pop();

    return queue;
  }

  void PushPullQueue(std::shared_ptr<QueueType> pull_queue) {
    std::lock_guard<std::mutex> pull_queues_lock(pull_queues_mutex_);
    pull_queues_.push(std::move(pull_queue));
  }

  template <typename InputIterator>
  void PushPullQueue(InputIterator first, InputIterator last) {
    std::lock_guard<std::mutex> pull_queues_lock(pull_queues_mutex_);
    for (; first != last; ++first) {
      pull_queues_.push(*first);
    }
  }

  void PushReadyQueue(std::shared_ptr<QueueType> ready_queue) {
    std::lock_guard<std::mutex> ready_queues_lock(ready_queues_mutex_);
    ready_queues_.push(std::move(ready_queue));
  }

  std::shared_ptr<QueueType> TryPopReadyQueue() {
    std::lock_guard<std::mutex> ready_queues_lock(ready_queues_mutex_);
    return TryPopQueue(ready_queues_);
  }

  template <class Rep, class Period>
  void WaitReadyQueue(std::function<void(std::shared_ptr<QueueType>)> callback,
                      const std::chrono::duration<Rep, Period>& timeout) {
    QueueRequest take_request{std::chrono::steady_clock::now() + timeout, std::move(callback)};

    std::lock_guard<std::mutex> queue_requests_lock(queue_requests_mutex_);
    queue_requests_.push(std::move(take_request));
    queue_requests_cv_.notify_all();
  }

  template <class Rep, class Period>
  std::shared_ptr<QueueType> WaitReadyQueue(const std::chrono::duration<Rep, Period>& timeout) {
    std::promise<std::shared_ptr<QueueType>> promise;
    auto future = promise.get_future();

    WaitReadyQueue([&promise](std::shared_ptr<QueueType> ready_queue) { promise.set_value(std::move(ready_queue)); },
                   timeout);

    // hold up
    return future.get();
  }

  void PutImpl(std::shared_ptr<QueueType> pull_queue, std::vector<MessageType> messages) {
    std::unique_lock<std::mutex> queue_lock(pull_queue->mutex());
    if (pull_queue->dropped()) {
      return;
    }

    if (messages.empty()) {
      PushPullQueue(std::move(pull_queue));
      return;
    }

    FlowTracker tracker(*pull_queue);
    if (pull_queue->Put(messages.begin(), messages.end())) {
      // no message before, consumable now
      PushReadyQueue(pull_queue);
    }

    // flow control
    FlowControlStrategyType::AfterPut(pull_queue, tracker);
  }

  std::tuple<std::vector<MessageType>, std::shared_ptr<QueueType>> PollImpl(std::shared_ptr<QueueType> ready_queue,
                                                                            size_t batch_size) {
    if (!ready_queue) {
      return {};
    }

    std::unique_lock<std::mutex> queue_lock(ready_queue->mutex());
    if (ready_queue->dropped()) {
      return {};
    }

    bool empty = true;
    auto messages = ready_queue->Take(batch_size,
                                      /* upper_bound */ nullptr,  //
                                      &empty,
                                      /* next_index */ nullptr);

    ConsumeStrategyType::AfterTake(ready_queue, empty);

    return std::make_tuple(std::move(messages), std::move(ready_queue));
  }

  template <typename CommitFunction>
  void CommitImpl(const std::shared_ptr<QueueType>& queue, const CommitFunction& commit_delegate) {
    std::unique_lock<std::mutex> queue_lock(queue->mutex());

    FlowTracker tracker(*queue);
    bool no_inflight = commit_delegate(queue);

    // consumable
    ConsumeStrategyType::AfterConsume(queue, no_inflight);

    // flow control
    FlowControlStrategyType::AfterCommit(queue, tracker);
  }

 private:
  std::map<IdentityType, std::shared_ptr<QueueType>> assigned_queues_;
  std::mutex assigned_queues_mutex_;

  std::queue<std::shared_ptr<QueueType>> pull_queues_;
  std::mutex pull_queues_mutex_;
  std::condition_variable pull_queues_cv_;

  std::queue<std::shared_ptr<QueueType>> ready_queues_;
  std::mutex ready_queues_mutex_;

  std::priority_queue<QueueRequest, std::vector<QueueRequest>, std::greater<QueueRequest>> queue_requests_;
  std::mutex queue_requests_mutex_;
  std::condition_variable queue_requests_cv_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMERPROXY_BASICQUEUESET_HPP_
