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
#ifndef ROCKETMQ_PROXY_CONSUMING_PULLTASKQUEUE_HPP_
#define ROCKETMQ_PROXY_CONSUMING_PULLTASKQUEUE_HPP_

#include <condition_variable>  // std::condition_variable
#include <functional>          // std::function
#include <memory>              // std::shared_ptr
#include <mutex>               // std::mutex
#include <queue>               // std::queue
#include <utility>             // std::move

#include "proxy/consuming/PopQueue.hpp"

namespace rocketmq {

namespace detail {

template <typename Queue, typename OnMessagesFunction>
struct QueueWithOnMessages {
  std::shared_ptr<Queue> queue;
  OnMessagesFunction on_messages;

  bool dropped() const { return queue->dropped; }

  explicit operator bool() const { return static_cast<bool>(queue); }
};

}  // namespace detail

template <typename Queue, typename Message>
class PullTaskQueue {
 public:
  using OnMessagesFunction = std::function<void(std::shared_ptr<Queue>, std::vector<Message>)>;

 private:
  using PullTask = detail::QueueWithOnMessages<Queue, OnMessagesFunction>;

 public:
  void Execute(std::shared_ptr<Queue> pull_queue, OnMessagesFunction on_messages) {
    std::lock_guard<std::mutex> pull_queues_lock(task_queue_mutex_);
    task_queue_.emplace(std::move(pull_queue), std::move(on_messages));
  }

  template <typename InputIterator>
  void Execute(InputIterator first, InputIterator last, OnMessagesFunction on_messages) {
    std::lock_guard<std::mutex> pull_queues_lock(task_queue_mutex_);
    for (; first != last; ++first) {
      task_queue_.emplace(*first, on_messages);
    }
  }

  template <class Rep, class Period>
  void TryPull(const std::function<void(std::shared_ptr<Queue> /* pull_queue */,
                                        std::function<void(std::vector<Message>)> /* on_messages */)>& delegate,
               const std::chrono::duration<Rep, Period>& timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;

    std::unique_lock<std::mutex> task_queue_lock(task_queue_mutex_);
    while (running_) {
      for (int i = 0; i < 31; ++i) {
        auto task = TryPopQueue(task_queue_);
        if (!task) {
          break;
        }

        task_queue_lock.unlock();
        delegate(std::move(task.queue), std::move(task.on_messages));
        task_queue_lock.lock();
      }

      if (task_queue_.empty()) {
        // wait pull queue
        if (task_queue_cv_.wait_until(task_queue_lock, deadline) == std::cv_status::timeout) {
          break;
        }
      } else {
        if (std::chrono::steady_clock::now() >= deadline) {
          break;
        }
      }
    }
  }

 private:
  bool running_{true};

  std::queue<PullTask> task_queue_;
  std::mutex task_queue_mutex_;
  std::condition_variable task_queue_cv_;
};

template <typename Queue, typename Message>
class QueuedPullAdaptor {
 public:
  QueuedPullAdaptor(PullTaskQueue<Queue, Message>* task_queue) : task_queue_(task_queue) {}

  ~QueuedPullAdaptor() = default;

  QueuedPullAdaptor(const QueuedPullAdaptor&) = default;
  QueuedPullAdaptor& operator=(const QueuedPullAdaptor&) = default;

  QueuedPullAdaptor(QueuedPullAdaptor&&) noexcept = default;
  QueuedPullAdaptor& operator=(QueuedPullAdaptor&&) noexcept = default;

  template <typename... Args>
  void operator()(Args... args) {
    task_queue_->Execute(std::forward<Args>(args)...);
  }

 private:
  PullTaskQueue<Queue, Message>* task_queue_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_PULLTASKQUEUE_HPP_
