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
#ifndef ROCKETMQ_PROXY_CONSUMING_POPQUEUE_HPP_
#define ROCKETMQ_PROXY_CONSUMING_POPQUEUE_HPP_

#include <queue>  // std::queue

namespace rocketmq {

template <typename Queue>
Queue TryPopQueue(std::queue<Queue> queues) {
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

}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_POPQUEUE_HPP_
