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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>

#include "proxy/consuming/LogicalQueueSet.hpp"

using testing::InitGoogleMock;
using testing::InitGoogleTest;
using testing::Return;

using rocketmq::LogicalQueueSet;

TEST(LogicalQueueSetTest, LogicalQueueSet) {
  using MessageType = LogicalQueueSet::MessageType;
  using QueueType = LogicalQueueSet::QueueType;

  LogicalQueueSet queue_set;

  queue_set.TryPullMessages(
      [&queue_set](std::shared_ptr<QueueType> pull_queue,
                   std::function<void(std::vector<MessageType>)> callback) -> void {
        std::vector<MessageType> messages;
        queue_set.Commit(pull_queue, messages.begin(), messages.end());
      },
      std::chrono::seconds(3));
  queue_set.TryWakeupPoll(std::chrono::seconds(3));

  auto messages = queue_set.Poll(1);
}

int main(int argc, char* argv[]) {
  InitGoogleMock(&argc, argv);
  testing::GTEST_FLAG(throw_on_failure) = true;
  testing::GTEST_FLAG(filter) = "LogicalQueueSetTest.*";
  return RUN_ALL_TESTS();
}
