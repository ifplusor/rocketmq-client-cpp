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
#include "protocol/body/ConsumerRunningInfo.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <json/reader.h>
#include <json/value.h>

#include <iostream>
#include <map>
#include <string>

using std::map;
using std::string;

using testing::InitGoogleMock;
using testing::InitGoogleTest;
using testing::Return;

using Json::Reader;
using Json::Value;

using rocketmq::ConsumerRunningInfo;
using rocketmq::MessageQueue;
using rocketmq::ProcessQueueInfo;
using rocketmq::SubscriptionData;

TEST(ConsumerRunningInfoTest, Init) {
  ConsumerRunningInfo consumerRunningInfo;
  consumerRunningInfo.jstack = "jstack";
  EXPECT_EQ(consumerRunningInfo.jstack, "jstack");

  EXPECT_TRUE(consumerRunningInfo.properties.empty());

  consumerRunningInfo.properties["testKey"] = "testValue";
  map<string, string> properties = consumerRunningInfo.properties;
  EXPECT_EQ(properties["testKey"], "testValue");

  consumerRunningInfo.properties = map<string, string>();
  EXPECT_TRUE(consumerRunningInfo.properties.empty());

  EXPECT_TRUE(consumerRunningInfo.subscription_set.empty());

  std::vector<SubscriptionData> subscriptionSet;
  subscriptionSet.emplace_back();

  consumerRunningInfo.subscription_set = subscriptionSet;
  EXPECT_EQ(consumerRunningInfo.subscription_set.size(), 1);

  EXPECT_TRUE(consumerRunningInfo.message_queue_table.empty());

  MessageQueue messageQueue("testTopic", "testBroker", 3);
  ProcessQueueInfo processQueueInfo;
  processQueueInfo.commit_offset = 1024;
  consumerRunningInfo.message_queue_table[messageQueue] = processQueueInfo;
  std::map<MessageQueue, ProcessQueueInfo> message_queue_table = consumerRunningInfo.message_queue_table;
  EXPECT_EQ(message_queue_table[messageQueue].commit_offset, processQueueInfo.commit_offset);

  // encode start
  consumerRunningInfo.properties.emplace(ConsumerRunningInfo::PROP_NAMESERVER_ADDR, "127.0.0.1:9876");
  consumerRunningInfo.properties.emplace(ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE, "core_size");
  consumerRunningInfo.properties.emplace(ConsumerRunningInfo::PROP_CONSUME_ORDERLY, "consume_orderly");
  consumerRunningInfo.properties.emplace(ConsumerRunningInfo::PROP_CONSUME_TYPE, "consume_type");
  consumerRunningInfo.properties.emplace(ConsumerRunningInfo::PROP_CLIENT_VERSION, "client_version");
  consumerRunningInfo.properties.emplace(ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP, "127");

  // TODO
  /* string outstr = consumerRunningInfo.encode();
   std::cout<< outstr;
   Value root;
   Reader reader;
   reader.parse(outstr.c_str(), root);

   EXPECT_EQ(root["jstack"].asString() , "jstack");

   Json::Value outData = root["properties"];
   EXPECT_EQ(outData[ConsumerRunningInfo::PROP_NAMESERVER_ADDR].asString(),"127.0.0.1:9876");
   EXPECT_EQ(
           outData[ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE].asString(),
           "core_size");
   EXPECT_EQ(outData[ConsumerRunningInfo::PROP_CONSUME_ORDERLY].asString(),
             "consume_orderly");
   EXPECT_EQ(outData[ConsumerRunningInfo::PROP_CONSUME_TYPE].asString(),
             "consume_type");
   EXPECT_EQ(outData[ConsumerRunningInfo::PROP_CLIENT_VERSION].asString(),
             "client_version");
   EXPECT_EQ(
           outData[ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP].asString(),
           "127");

   Json::Value subscriptionSetJson = root["subscriptionSet"];
   EXPECT_EQ(subscriptionSetJson[0], subscriptionSet[0].toJson());

   Json::Value mqTableJson = root["mqTable"];
   EXPECT_EQ(mqTableJson[messageQueue.toJson().toStyledString()].asString(),
             processQueueInfo.toJson().toStyledString());
*/
}

int main(int argc, char* argv[]) {
  InitGoogleMock(&argc, argv);
  testing::GTEST_FLAG(throw_on_failure) = true;
  testing::GTEST_FLAG(filter) = "ConsumerRunningInfoTest.*";
  return RUN_ALL_TESTS();
}
