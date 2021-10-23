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
#include <DefaultMQProducer.h>

#include "common.h"
#include "concurrent/latch.hpp"

using namespace rocketmq;

TpsReportService g_tps;
latch* g_finish = nullptr;

std::atomic<int> g_success(0);
std::atomic<int> g_failed(0);

class MyAutoDeleteSendCallback : public AutoDeleteSendCallback {
 public:
  MyAutoDeleteSendCallback(MQMessage message) : message_(std::move(message)) {}

  void onSuccess(SendResult& send_result) override {
    g_success++;
    g_finish->count_down();
    g_tps.Increment();
    std::cout << "message id: " << send_result.message_id() << std::endl;
  }

  void onException(MQException& e) noexcept override {
    g_failed++;
    g_finish->count_down();
    std::cout << "send Exception: " << e.what() << std::endl;
  }

 private:
  MQMessage message_;
};

void AsyncProducerWorker(RocketmqSendAndConsumerArgs* info, DefaultMQProducer* producer) {
  while (g_msg_count.fetch_sub(1) > 0) {
    MQMessage message(info->topic,  // topic
                      "*",          // tag
                      info->body);  // body

    SendCallback* callback = new MyAutoDeleteSendCallback(message);

    try {
      producer->send(message, callback);  // auto delete
    } catch (std::exception& e) {
      std::cout << "[BUG]:" << e.what() << std::endl;
      throw;
    }
  }
}

int main(int argc, char* argv[]) {
  RocketmqSendAndConsumerArgs info;
  if (!ParseArgs(argc, argv, &info)) {
    exit(-1);
  }
  PrintRocketmqSendAndConsumerArgs(info);

  DefaultMQProducer producer(info.groupname);
  producer.set_namesrv_addr(info.namesrv);
  producer.set_group_name(info.groupname);
  producer.set_async_send_thread_nums(1);
  producer.set_send_msg_timeout(3000);
  producer.set_retry_times(info.retrytimes);
  producer.set_retry_times_for_async(info.retrytimes);
  producer.set_send_latency_fault_enable(!info.selectUnactiveBroker);
  producer.set_tcp_transport_try_lock_timeout(1000);
  producer.set_tcp_transport_connect_timeout(400);
  producer.start();

  std::vector<std::shared_ptr<std::thread>> work_pool;
  int msgcount = g_msg_count.load();
  g_finish = new latch(msgcount);
  g_tps.start();

  auto start = std::chrono::system_clock::now();

  int threadCount = info.thread_count;
  for (int j = 0; j < threadCount; j++) {
    auto th = std::make_shared<std::thread>(AsyncProducerWorker, &info, &producer);
    work_pool.push_back(th);
  }

  for (size_t th = 0; th != work_pool.size(); ++th) {
    work_pool[th]->join();
  }

  g_finish->wait();

  auto end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::cout << "per msg time: " << duration.count() / (double)msgcount << "ms" << std::endl
            << "========================finished=============================" << std::endl
            << "success: " << g_success << ", failed: " << g_failed << std::endl;

  try {
    producer.shutdown();
  } catch (std::exception& e) {
    std::cout << "encounter exception: " << e.what() << std::endl;
  }

  delete g_finish;

  return 0;
}
