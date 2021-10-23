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
#include <json/json.h>
#include <json/value.h>
#include <json/writer.h>

#include <map>
#include <memory>
#include <string>

#include "ByteArray.h"
#include "MQException.h"
#include "MessageSysFlag.h"
#include "UtilAll.h"
#include "protocol/body/ConsumerList.hpp"
#include "protocol/header/ConsumerSendMsgBackRequestHeader.hpp"
#include "protocol/header/GetConsumerRunningInfoRequestHeader.hpp"
#include "protocol/header/NotifyConsumerIdsChangedRequestHeader.hpp"
#include "protocol/header/ResetOffsetRequestHeader.hpp"

using testing::InitGoogleMock;
using testing::InitGoogleTest;
using testing::Return;

using Json::FastWriter;
using Json::Value;

using rocketmq::ByteArray;
using rocketmq::CommandCustomHeader;
using rocketmq::ConsumerList;
using rocketmq::ConsumerSendMsgBackRequestHeader;
using rocketmq::GetConsumerRunningInfoRequestHeader;
using rocketmq::NotifyConsumerIdsChangedRequestHeader;
using rocketmq::ResetOffsetRequestHeader;

TEST(CommandHeaderTest, ConsumerSendMsgBackRequestHeader) {}

TEST(CommandHeaderTest, ConsumerList) {
  Value value;
  value[0] = "consumer1";
  value[1] = "consumer2";

  Value root;
  root["consumerIdList"] = value;

  FastWriter writer;
  std::string data = writer.write(root);

  const ByteArray bodyData((char*)data.data(), data.size());
  auto body = ConsumerList::Decode(bodyData);
  EXPECT_EQ(body->consumer_id_list.size(), 2);
}

TEST(CommandHeaderTest, ResetOffsetRequestHeader) {
  ResetOffsetRequestHeader header;

  header.topic = "testTopic";
  EXPECT_EQ(header.topic, "testTopic");

  header.group = "testGroup";
  EXPECT_EQ(header.group, "testGroup");

  header.timestamp = 123;
  EXPECT_EQ(header.timestamp, 123);

  header.force = true;
  EXPECT_TRUE(header.force);

  std::map<std::string, std::string> resetOffsetFields;
  resetOffsetFields["topic"] = "testTopic";
  resetOffsetFields["group"] = "testGroup";
  resetOffsetFields["timestamp"] = "123";
  resetOffsetFields["isForce"] = "true";
  std::unique_ptr<ResetOffsetRequestHeader> resetOffsetHeader(ResetOffsetRequestHeader::Decode(resetOffsetFields));
  EXPECT_EQ(resetOffsetHeader->topic, "testTopic");
  EXPECT_EQ(resetOffsetHeader->group, "testGroup");
  EXPECT_EQ(resetOffsetHeader->timestamp, 123);
  EXPECT_TRUE(resetOffsetHeader->force);
}

TEST(CommandHeaderTest, GetConsumerRunningInfoRequestHeader) {
  GetConsumerRunningInfoRequestHeader header;
  header.client_id = "testClientId";
  header.consumer_group = "testConsumer";
  header.jstack_enable = true;

  std::map<std::string, std::string> requestMap;
  header.SetDeclaredFieldOfCommandHeader(requestMap);
  EXPECT_EQ(requestMap["clientId"], "testClientId");
  EXPECT_EQ(requestMap["consumerGroup"], "testConsumer");
  EXPECT_EQ(requestMap["jstackEnable"], "true");

  Value outData;
  header.Encode(outData);
  EXPECT_EQ(outData["clientId"], "testClientId");
  EXPECT_EQ(outData["consumerGroup"], "testConsumer");
  EXPECT_EQ(outData["jstackEnable"], "true");

  std::unique_ptr<GetConsumerRunningInfoRequestHeader> decodeHeader(
      GetConsumerRunningInfoRequestHeader::Decode(requestMap));
  EXPECT_EQ(decodeHeader->client_id, "testClientId");
  EXPECT_EQ(decodeHeader->consumer_group, "testConsumer");
  EXPECT_TRUE(decodeHeader->jstack_enable);
}

TEST(CommandHeaderTest, NotifyConsumerIdsChangedRequestHeader) {
  std::map<std::string, std::string> extFields;
  extFields["consumerGroup"] = "testGroup";
  std::unique_ptr<NotifyConsumerIdsChangedRequestHeader> header(
      NotifyConsumerIdsChangedRequestHeader::Decode(extFields));
  EXPECT_EQ(header->consumer_group, "testGroup");
}

int main(int argc, char* argv[]) {
  InitGoogleMock(&argc, argv);
  testing::GTEST_FLAG(throw_on_failure) = true;
  testing::GTEST_FLAG(filter) = "CommandHeaderTest.*";
  return RUN_ALL_TESTS();
}
