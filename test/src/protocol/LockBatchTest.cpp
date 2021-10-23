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

#include <vector>

#include "ByteArray.h"
#include "MessageQueue.hpp"
#include "protocol/body/ConsumeQueueSet.hpp"
#include "protocol/body/LockBatchResult.hpp"

using testing::InitGoogleMock;
using testing::InitGoogleTest;
using testing::Return;

using rocketmq::ByteArray;
using rocketmq::ConsumeQueueSet;
using rocketmq::LockBatchResult;
using rocketmq::MessageQueue;

TEST(LockBatchTest, ConsumeQueueSet) {
  ConsumeQueueSet consume_queue_set;

  consume_queue_set.client_id = "testClientId";
  EXPECT_EQ(consume_queue_set.client_id, "testClientId");

  consume_queue_set.consumer_group = "testGroup";
  EXPECT_EQ(consume_queue_set.consumer_group, "testGroup");

  std::vector<MessageQueue> messageQueueList;
  messageQueueList.emplace_back("testTopic", "testBroker", 1);
  messageQueueList.emplace_back("testTopic", "testBroker", 2);

  consume_queue_set.message_queue_set = messageQueueList;
  EXPECT_EQ(consume_queue_set.message_queue_set, messageQueueList);

  std::string outData = consume_queue_set.Encode();

  Json::Value root;
  Json::Reader reader;
  reader.parse(outData, root);
  EXPECT_EQ(root["clientId"], "testClientId");
  EXPECT_EQ(root["consumerGroup"], "testGroup");
  EXPECT_EQ(root["mqSet"][1]["topic"], "testTopic");
  EXPECT_EQ(root["mqSet"][1]["brokerName"], "testBroker");
  EXPECT_EQ(root["mqSet"][1]["queueId"], 2);
}

TEST(LockBatchBodyTest, LockBatchResult) {
  Json::Value root;
  Json::Value mqs;

  Json::Value mq;
  mq["topic"] = "testTopic";
  mq["brokerName"] = "testBroker";
  mq["queueId"] = 1;
  mqs[0] = mq;
  root["lockOKMQSet"] = mqs;

  Json::FastWriter fastwrite;
  std::string data = fastwrite.write(root);

  const ByteArray bodyData((char*)data.data(), data.size());
  std::unique_ptr<LockBatchResult> lock_batch_result(LockBatchResult::Decode(bodyData));

  MessageQueue messageQueue("testTopic", "testBroker", 1);
  EXPECT_EQ(messageQueue, lock_batch_result->lock_ok_message_queue_set[0]);
}

int main(int argc, char* argv[]) {
  InitGoogleMock(&argc, argv);
  testing::GTEST_FLAG(throw_on_failure) = true;
  testing::GTEST_FLAG(filter) = "LockBatchBodyTest.*";
  return RUN_ALL_TESTS();
}
