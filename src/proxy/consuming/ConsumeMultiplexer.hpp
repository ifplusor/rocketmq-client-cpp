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
#ifndef ROCKETMQ_PROXY_CONSUMING_CONSUMEMULTIPLEXER_HPP_
#define ROCKETMQ_PROXY_CONSUMING_CONSUMEMULTIPLEXER_HPP_

#include <condition_variable>  // std::condition_variable
#include <functional>          // std::function, std::greater
#include <future>              // std::future, std::promise
#include <memory>              // std::shared_ptr
#include <mutex>               // std::mutex
#include <queue>               // std::priority_queue, std::queue
#include <utility>             // std::move
#include <vector>              // std::vector

#include "proxy/consuming/PopQueue.hpp"

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

template <typename Queue, typename TakeMessagesFunction>
struct QueueWithTakeMessages {
  std::shared_ptr<Queue> queue;
  TakeMessagesFunction take_messages;

  bool dropped() const { return queue->dropped; }

  explicit operator bool() const { return static_cast<bool>(queue); }
};

}  // namespace detail

/**
 * @brief support poll multiplexing on ProcessQueue(s)
 */
template <typename Queue, typename Message>
class ConsumeMultiplexer {
 public:
  using TakeMessagesFunction =
      std::function<std::tuple<std::vector<Message>>(std::shared_ptr<Queue>, size_t batch_size)>;

 private:
  using ConsumeTask = detail::QueueWithTakeMessages<Queue, TakeMessagesFunction>;
  using QueueRequest = detail::QueueRequest<Queue>;

 public:
  void Consume(std::shared_ptr<Queue> consume_queue) {
    std::lock_guard<std::mutex> ready_queues_lock(task_queue_mutex_);
    task_queue_.push(std::move(task_queue_));
  }

  /**
   * @brief synchronous non-blocking poll
   *
   * @param batch_size
   * @return std::vector<Message>
   */
  std::tuple<std::vector<Message>, std::shared_ptr<Queue>> Poll(size_t batch_size) {
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
   * @return std::vector<Message>
   */
  template <class Rep, class Period>
  std::tuple<std::vector<Message>, std::shared_ptr<Queue>> Poll(size_t batch_size,
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
            std::function<void(std::tuple<std::vector<Message>, std::shared_ptr<Queue>>)> callback,
            const std::chrono::duration<Rep, Period>& timeout) {
    // FIXME: lifecycle of this
    WaitReadyQueue([this, batch_size, callback](
                       std::shared_ptr<Queue> ready_queue) { callback(PollImpl(std::move(ready_queue), batch_size)); },
                   timeout);
  }

  template <typename InputIterator>
  void Commit(const std::shared_ptr<Queue>& queue, InputIterator first, InputIterator last) {
    CommitImpl(queue, [&first, &last](const std::shared_ptr<Queue>& queue) { return queue->Commit(first, last); });
  }

  void Commit(const std::shared_ptr<Queue>& queue, const Message& message) {
    CommitImpl(queue, [&message](const std::shared_ptr<Queue>& queue) { return queue->Commit(message); });
  }

  template <typename InputIterator>
  void Rollback(const std::shared_ptr<Queue>& queue, InputIterator first, InputIterator last) {
    std::unique_ptr<std::mutex> queue_lock(queue->mutex());
    ConsumeStrategyType::AfterConsume(queue, queue->Rollback(first, last));
  }

  void Rollback(const std::shared_ptr<Queue>& queue, const Message& message) {
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

 private:
  void PushReadyQueue(std::shared_ptr<Queue> ready_queue) {}

  std::shared_ptr<Queue> TryPopReadyQueue() {
    std::lock_guard<std::mutex> ready_queues_lock(task_queue_mutex_);
    return TryPopQueue(task_queue_);
  }

  template <class Rep, class Period>
  void WaitReadyQueue(std::function<void(std::shared_ptr<Queue>)> callback,
                      const std::chrono::duration<Rep, Period>& timeout) {
    QueueRequest take_request{std::chrono::steady_clock::now() + timeout, std::move(callback)};

    std::lock_guard<std::mutex> queue_requests_lock(queue_requests_mutex_);
    queue_requests_.push(std::move(take_request));
    queue_requests_cv_.notify_all();
  }

  template <class Rep, class Period>
  std::shared_ptr<Queue> WaitReadyQueue(const std::chrono::duration<Rep, Period>& timeout) {
    std::promise<std::shared_ptr<Queue>> promise;
    auto future = promise.get_future();

    WaitReadyQueue([&promise](std::shared_ptr<Queue> ready_queue) { promise.set_value(std::move(ready_queue)); },
                   timeout);

    // hold up
    return future.get();
  }

  std::tuple<std::vector<Message>, std::shared_ptr<Queue>> PollImpl(std::shared_ptr<Queue> ready_queue,
                                                                    size_t batch_size) {
    if (!ready_queue) {
      return {};
    }

    std::unique_lock<std::mutex> queue_lock(ready_queue->mutex());
    if (ready_queue->dropped()) {
      return {};
    }

    bool empty = true;
    auto messages = ready_queue->Take(batch_size, &empty);

    ConsumeStrategyType::AfterTake(ready_queue, empty);

    return std::make_tuple(std::move(messages), std::move(ready_queue));
  }

  template <typename CommitFunction>
  void CommitImpl(const std::shared_ptr<Queue>& queue, const CommitFunction& commit_delegate) {
    std::unique_lock<std::mutex> queue_lock(queue->mutex());

    FlowTracker tracker(*queue);
    bool no_inflight = commit_delegate(queue);

    // consumable
    ConsumeStrategyType::AfterConsume(queue, no_inflight);

    // flow control
    flow_control_strategy_.AfterCommit(queue, tracker);
  }

 private:
  std::queue<ConsumeTask> task_queue_;
  std::mutex task_queue_mutex_;

  std::priority_queue<QueueRequest, std::vector<QueueRequest>, std::greater<QueueRequest>> queue_requests_;
  std::mutex queue_requests_mutex_;
  std::condition_variable queue_requests_cv_;
};

template <typename Queue, typename Message>
class MultiplexingConsumeAdaptor {
 public:
  MultiplexingConsumeAdaptor(ConsumeMultiplexer<Queue, Message>* consume_multiplexer)
      : consume_multiplexer_(consume_multiplexer) {}

  ~MultiplexingConsumeAdaptor() = default;

  MultiplexingConsumeAdaptor(const MultiplexingConsumeAdaptor&) = default;
  MultiplexingConsumeAdaptor& operator=(const MultiplexingConsumeAdaptor&) = default;

  MultiplexingConsumeAdaptor(MultiplexingConsumeAdaptor&&) noexcept = default;
  MultiplexingConsumeAdaptor& operator=(MultiplexingConsumeAdaptor&&) noexcept = default;

  template <typename... Args>
  void operator()(Args... args) {
    consume_multiplexer_->Consume(std::forward<Args>(args)...);
  }

 private:
  ConsumeMultiplexer<Queue, Message> consume_multiplexer_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_CONSUMEMULTIPLEXER_HPP_
