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
#ifndef ROCKETMQ_CONSUMERPROXY_CONSUMESTRATEGY_HPP_
#define ROCKETMQ_CONSUMERPROXY_CONSUMESTRATEGY_HPP_

#include <memory>  // std::shared_ptr

namespace rocketmq {

template <typename QueueSet, typename Queue>
class SharedConsumeStrategy {
 public:
  void AfterTake(const std::shared_ptr<Queue>& queue, bool no_cached) {
    if (!no_cached) {
      // reput if queue is still not empty
      static_cast<QueueSet*>(this)->PushReadyQueue(queue);
    }
  }

  void AfterConsume(const std::shared_ptr<Queue>& queue, bool no_inflight) {}
};

template <typename QueueSet, typename Queue>
class ExclusiveConsumeStrategy {
 public:
  void AfterTake(const std::shared_ptr<Queue>& queue, bool no_cached) {}

  void AfterConsume(const std::shared_ptr<Queue>& queue, bool no_inflight) {
    if (no_inflight) {
      // reput if no inflight messages
      static_cast<QueueSet*>(this)->PushReadyQueue(queue);
    }
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMERPROXY_CONSUMESTRATEGY_HPP_
