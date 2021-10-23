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
#ifndef ROCKETMQ_PROXY_CONSUMING_PULLMANAGER_HPP_
#define ROCKETMQ_PROXY_CONSUMING_PULLMANAGER_HPP_

#include <functional>   // std::function
#include <memory>       // std::shared_ptr
#include <type_traits>  // std::enable_if
#include <utility>      // std::declval, std::move

#include "utility/Void.hpp"

namespace rocketmq {
namespace detail {

template <typename PullDelegate, typename InputIterator, typename OnMessagesFunction, typename = void>
struct CanPullWithIterator {
  static constexpr bool value = false;
};

template <typename PullDelegate, typename InputIterator, typename OnMessagesFunction>
struct CanPullWithIterator<PullDelegate,
                           InputIterator,
                           OnMessagesFunction,
                           void_t<decltype(std::declval<PullDelegate>()(std::declval<InputIterator>(),
                                                                        std::declval<InputIterator>(),
                                                                        std::declval<OnMessagesFunction>()))>> {
  static constexpr bool value = true;
};

template <typename QueueSet, typename PullDelegate>
class PullManager {
 public:
  using Queue = typename QueueSet::QueueType;
  using Message = typename QueueSet::MessageType;

 private:
  using OnMessagesFunction = std::function<void(std::shared_ptr<Queue>, std::vector<Message>)>;

 public:
  PullManager(PullDelegate pull_delegate) : pull_delegate_(std::move(pull_delegate)) {}

  void Pull(std::shared_ptr<Queue> pull_queue) {  //
    pull_delegate_(std::move(pull_queue),
                   /* on_messages */ [this](std::shared_ptr<Queue> queue, std::vector<Message> messages) {
                     OnMessages(std::move(queue), std::move(messages));
                   });
  }

  template <typename InputIterator>
  auto Pull(InputIterator first, InputIterator last) ->
      typename std::enable_if<CanPullWithIterator<PullDelegate, InputIterator, OnMessagesFunction>::value, void>::type {
    pull_delegate_(std::move<InputIterator>(first), std::move<InputIterator>(last),
                   /* on_messages */ [this](std::shared_ptr<Queue> queue, std::vector<Message> messages) {
                     OnMessages(std::move(queue), std::move(messages));
                   });
  }

  template <typename InputIterator>
  auto Pull(InputIterator first, InputIterator last) ->
      typename std::enable_if<!CanPullWithIterator<PullDelegate, InputIterator, OnMessagesFunction>::value,
                              void>::type {
    for (; first != last; ++first) {
      Pull(*first);
    }
  }

 private:
  void OnMessages(std::shared_ptr<Queue> pull_queue, std::vector<Message> messages) {
    queue_set_->OnMessagesReceived(std::move(pull_queue), std::move(messages));
  }

 private:
  PullDelegate pull_delegate_;

  QueueSet* queue_set_;
};

}  // namespace detail
}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_PULLMANAGER_HPP_
