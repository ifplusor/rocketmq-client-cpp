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
#ifndef ROCKETMQ_PROXY_CONSUMING_BASICQUEUESET_HPP_
#define ROCKETMQ_PROXY_CONSUMING_BASICQUEUESET_HPP_

/*

  BasicQueueSet synopsis:

namespace rocketmq {

template <typename Queue>
class BasicQueueSet {
 public:
  using QueueType = Queue;
  using IdentityType = typename QueueType::IdentityType;
  using MessageType = typename QueueType::MessageType;
  using OffsetType = typename QueueType::OffsetType;

  void Assign(std::vector<IdentityType> assigned,
              const std::function<bool(const std::shared_ptr<QueueType>&)>& before_insert,
              const std::function<bool(const std::shared_ptr<QueueType>&)>& before_remove);

  std::tuple<std::vector<MessageType>, std::shared_ptr<QueueType>> Poll(size_t batch_size) {

  template <class Rep, class Period>
  std::tuple<std::vector<MessageType>, std::shared_ptr<QueueType>> Poll(
      size_t batch_size,
      const std::chrono::duration<Rep, Period>& timeout);

  template <class Rep, class Period>
  void Poll(size_t batch_size,
            std::function<void(std::tuple<std::vector<MessageType>, std::shared_ptr<QueueType>>)> callback,
            const std::chrono::duration<Rep, Period>& timeout);

  template <typename InputIterator>
  void Commit(const std::shared_ptr<QueueType>& queue, InputIterator first, InputIterator last);

  void Commit(const std::shared_ptr<QueueType>& queue, const MessageType& message);

  template <typename InputIterator>
  void Rollback(const std::shared_ptr<QueueType>& queue, InputIterator first, InputIterator last);

  void Rollback(const std::shared_ptr<QueueType>& queue, const MessageType& message);

  template <class Rep, class Period>
  void TryWakeupPoll(const std::chrono::duration<Rep, Period>& timeout);

  template <class Rep, class Period>
  void TryPullMessages(
      const std::function<void(std::shared_ptr<QueueType>, std::function<void(std::vector<MessageType>)>)>& delegate,
      const std::chrono::duration<Rep, Period>& timeout);
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
#include <map>                 // std::map
#include <memory>              // std::shared_ptr
#include <mutex>               // std::lock_guard, std::mutex, std::unique_lock
#include <tuple>               // std::tuple
#include <utility>             // std::move
#include <vector>              // std::vector

#include "proxy/consuming/CacheManager.hpp"
#include "proxy/consuming/ConsumeManager.hpp"
#include "proxy/consuming/ConsumeStrategy.hpp"
#include "proxy/consuming/FlowControlStrategy.hpp"
#include "proxy/consuming/PullManager.hpp"

namespace rocketmq {

namespace detail {

template <typename Queue, typename PullStrategy, typename ConsumeStrategy, typename FlowControlStrategy>
class BasicQueueSetImpl {
 public:
  using QueueType = Queue;
  using IdentityType = typename QueueType::IdentityType;
  using MessageType = typename QueueType::MessageType;
  using OffsetType = typename QueueType::OffsetType;

 public:
  BasicQueueSetImpl() = default;

  void OnQueueAdded(std::shared_ptr<QueueType> queue) {  //
    OnQueuePullable(std::move(queue));
  }

  void OnQueueRemoved(std::shared_ptr<QueueType> queue) {  //
    cache_manager_->Clear(std::move(queue));
  }

  void OnQueuePullable(std::shared_ptr<QueueType> queue) {  //
    pull_manager_->Pull(std::move(queue));
  }

  template <typename InputIterator>
  void OnQueuePullable(InputIterator first, InputIterator last) {
    pull_manager_->Pull(std::move(first), std::move(last));
  }

  void OnMessagesReceived(std::shared_ptr<QueueType> queue, std::vector<MessageType> messages) {
    cache_manager_->Put(std::move(queue), std::move(messages));
  }

  void OnQueueConsumable(std::shared_ptr<QueueType> queue) {  //
    consume_manager_->Consume(std::move(queue));
  }

 private:
  PullManager<BasicQueueSetImpl, PullStrategy>* pull_manager_;
  ConsumeManager<BasicQueueSetImpl, ConsumeStrategy>* consume_manager_;
  CacheManager<BasicQueueSetImpl, FlowControlStrategy>* cache_manager_;
};

}  // namespace detail

// template <typename Derived, typename Queue, template <typename, typename> class ConsumeStrategy =
// SharedConsumeStrategy> class BasicQueueSet : public detail::BasicQueueSetImpl<Derived, Queue, ConsumeStrategy> {};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_BASICQUEUESET_HPP_
