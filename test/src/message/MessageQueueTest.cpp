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

#include "MessageQueue.hpp"

using testing::InitGoogleMock;
using testing::InitGoogleTest;
using testing::Return;

using rocketmq::MessageQueue;

TEST(MessageQueueTest, Init) {
  MessageQueue messageQueue;
  EXPECT_EQ(messageQueue.broker_name(), "");
  EXPECT_EQ(messageQueue.topic(), "");
  EXPECT_EQ(messageQueue.queue_id(), -1);

  MessageQueue twoMessageQueue("testTopic", "testBroker", 1);
  EXPECT_EQ(twoMessageQueue.broker_name(), "testBroker");
  EXPECT_EQ(twoMessageQueue.topic(), "testTopic");
  EXPECT_EQ(twoMessageQueue.queue_id(), 1);

  MessageQueue threeMessageQueue("threeTestTopic", "threeTestBroker", 2);
  MessageQueue fourMessageQueue(threeMessageQueue);
  EXPECT_EQ(fourMessageQueue.broker_name(), "threeTestBroker");
  EXPECT_EQ(fourMessageQueue.topic(), "threeTestTopic");
  EXPECT_EQ(fourMessageQueue.queue_id(), 2);

  fourMessageQueue = twoMessageQueue;
  EXPECT_EQ(fourMessageQueue.broker_name(), "testBroker");
  EXPECT_EQ(fourMessageQueue.topic(), "testTopic");
  EXPECT_EQ(fourMessageQueue.queue_id(), 1);

  fourMessageQueue.set_broker_name("fourTestBroker");
  fourMessageQueue.set_topic("fourTestTopic");
  fourMessageQueue.set_queue_id(4);
  EXPECT_EQ(fourMessageQueue.broker_name(), "fourTestBroker");
  EXPECT_EQ(fourMessageQueue.topic(), "fourTestTopic");
  EXPECT_EQ(fourMessageQueue.queue_id(), 4);
}

TEST(MessageQueueTest, Operators) {
  MessageQueue messageQueue;
  EXPECT_EQ(messageQueue, messageQueue);
  EXPECT_EQ(messageQueue.Compare(messageQueue), 0);

  MessageQueue twoMessageQueue;
  EXPECT_EQ(messageQueue, twoMessageQueue);
  EXPECT_EQ(messageQueue.Compare(twoMessageQueue), 0);

  twoMessageQueue.set_topic("testTopic");
  EXPECT_FALSE(messageQueue == twoMessageQueue);
  EXPECT_NE(messageQueue.Compare(twoMessageQueue), 0);

  twoMessageQueue = messageQueue;
  EXPECT_TRUE(messageQueue == twoMessageQueue);

  twoMessageQueue.set_queue_id(1);
  EXPECT_FALSE(messageQueue == twoMessageQueue);
  EXPECT_NE(messageQueue.Compare(twoMessageQueue), 0);

  twoMessageQueue = messageQueue;
  EXPECT_TRUE(messageQueue == twoMessageQueue);

  twoMessageQueue.set_broker_name("testBroker");
  EXPECT_FALSE(messageQueue == twoMessageQueue);
  EXPECT_FALSE(messageQueue.Compare(twoMessageQueue) == 0);
}

int main(int argc, char* argv[]) {
  InitGoogleMock(&argc, argv);
  testing::GTEST_FLAG(throw_on_failure) = true;
  testing::GTEST_FLAG(filter) = "MessageQueueTest.*";
  int itestts = RUN_ALL_TESTS();
  return itestts;
}
