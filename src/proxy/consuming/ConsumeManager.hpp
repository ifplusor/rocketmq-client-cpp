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
#ifndef ROCKETMQ_PROXY_CONSUMING_CONSUMEMANAGER_HPP_
#define ROCKETMQ_PROXY_CONSUMING_CONSUMEMANAGER_HPP_

#include <functional>   // std::function
#include <memory>       // std::shared_ptr
#include <type_traits>  // std::enable_if
#include <utility>      // std::declval, std::move

#include "utility/Void.hpp"

namespace rocketmq {
namespace detail {

template <typename QueueSet, typename ConsumeDelegate>
class ConsumeManager {
 public:
  using Queue = typename QueueSet::QueueType;
  using Message = typename QueueSet::MessageType;

 public:
  ConsumeManager(ConsumeDelegate consume_delegate) : consume_delegate_(std::move(consume_delegate)) {}

  void Consume(std::shared_ptr<Queue> consume_queue) {  //
    consume_delegate_(std::move(consume_queue),
                      /* take_messages */ [this](std::shared_ptr<Queue> queue) {  //
                        OnMessages(std::move(queue));
                      });
  }

 private:
  ConsumeDelegate consume_delegate_;

  QueueSet* queue_set_;
};

}  // namespace detail
}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_CONSUMEMANAGER_HPP_
