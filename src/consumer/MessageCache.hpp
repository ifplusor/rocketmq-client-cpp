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
#ifndef ROCKETMQ_CONSUMER_MESSAGECACHE_HPP_
#define ROCKETMQ_CONSUMER_MESSAGECACHE_HPP_

#include <vector>

#include "MessageExt.h"
#include "ProcessQueue.h"

namespace rocketmq {

class MessageCache {
 public:
  virtual void PutMessages(const ProcessQueuePtr& process_queue, const std::vector<MessageExtPtr>& messages) {
    process_queue->PutMessages(messages);
  }

  virtual void ClearMessages(const ProcessQueuePtr& process_queue) { process_queue->ClearAllMessages(); }

  std::vector<MessageExtPtr> TakeMessages(const ProcessQueuePtr& process_queue, int batch_size, int64_t offset_limit) {
    return TakeMessagesImpl(process_queue, batch_size, offset_limit, nullptr);
  }

 protected:
  std::vector<MessageExtPtr> TakeMessagesImpl(const ProcessQueuePtr& process_queue,
                                              int batch_size,
                                              int64_t offset_limit,
                                              bool* remained) {
    if (process_queue->dropped()) {
      if (remained != nullptr) {
        *remained = false;
      }
      return {};
    }

    int64_t next_offset = 0;
    auto messages = process_queue->TakeMessages(batch_size, false, offset_limit, next_offset, remained);
    process_queue->set_consume_offset(next_offset);
    return messages;
  }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMER_MESSAGECACHE_HPP_
