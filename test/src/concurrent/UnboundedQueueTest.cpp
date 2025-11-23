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
#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "concurrent/unbounded_queue.hpp"

using rocketmq::unbounded_queue;
using rocketmq::queue_op_status;

TEST(UnboundedQueueTest, BasicPushPop) {
  unbounded_queue<int> queue;

  // Test empty queue
  int value;
  EXPECT_EQ(queue.try_pop(value), queue_op_status::empty);

  // Test single push and pop
  queue.push(42);
  EXPECT_EQ(queue.try_pop(value), queue_op_status::success);
  EXPECT_EQ(value, 42);

  // Queue should be empty again
  EXPECT_EQ(queue.try_pop(value), queue_op_status::empty);
}

TEST(UnboundedQueueTest, MultiplePushPop) {
  unbounded_queue<int> queue;

  // Push multiple values
  for (int i = 0; i < 10; i++) {
    queue.push(i);
  }

  // Pop and verify values
  for (int i = 0; i < 10; i++) {
    int value;
    EXPECT_EQ(queue.try_pop(value), queue_op_status::success);
    EXPECT_EQ(value, i);
  }

  // Queue should be empty
  int value;
  EXPECT_EQ(queue.try_pop(value), queue_op_status::empty);
}

TEST(UnboundedQueueTest, ConcurrentPushPop) {
  unbounded_queue<int> queue;
  const int num_threads = 8;
  const int items_per_thread = 1000;
  std::atomic<int> push_count{0};
  std::atomic<int> pop_count{0};
  std::vector<std::thread> threads;

  // Create producer threads
  for (int t = 0; t < num_threads / 2; t++) {
    threads.emplace_back([&queue, &push_count, items_per_thread, t]() {
      for (int i = 0; i < items_per_thread; i++) {
        queue.push(t * items_per_thread + i);
        push_count++;
      }
    });
  }

  // Create consumer threads
  for (int t = 0; t < num_threads / 2; t++) {
    threads.emplace_back([&queue, &pop_count, num_threads, items_per_thread]() {
      int value;
      while (pop_count < (num_threads / 2) * items_per_thread) {
        if (queue.try_pop(value) == queue_op_status::success) {
          pop_count++;
        } else {
          std::this_thread::yield();
        }
      }
    });
  }

  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }

  // Verify counts
  EXPECT_EQ(push_count.load(), (num_threads / 2) * items_per_thread);
  EXPECT_EQ(pop_count.load(), (num_threads / 2) * items_per_thread);

  // Queue should be empty
  int value;
  EXPECT_EQ(queue.try_pop(value), queue_op_status::empty);
}

TEST(UnboundedQueueTest, ConcurrentSingleElementStress) {
  // This test specifically stresses the single-element case
  // where the bug would manifest
  unbounded_queue<int> queue;
  const int num_iterations = 10000;
  std::atomic<int> produced{0};
  std::atomic<int> consumed{0};
  std::set<int> consumed_values;
  std::mutex consumed_mutex;

  // Producer thread - pushes one item at a time
  std::thread producer([&]() {
    for (int i = 0; i < num_iterations; i++) {
      queue.push(i);
      produced++;
    }
  });

  // Multiple consumer threads competing for items
  std::vector<std::thread> consumers;
  for (int t = 0; t < 4; t++) {
    consumers.emplace_back([&]() {
      int value;
      while (consumed < num_iterations) {
        if (queue.try_pop(value) == queue_op_status::success) {
          consumed++;
          std::lock_guard<std::mutex> lock(consumed_mutex);
          consumed_values.insert(value);
        }
      }
    });
  }

  producer.join();
  for (auto& c : consumers) {
    c.join();
  }

  // Verify no data loss - all values should be consumed exactly once
  EXPECT_EQ(consumed.load(), num_iterations);
  EXPECT_EQ(consumed_values.size(), static_cast<size_t>(num_iterations));
  
  // Verify all values from 0 to num_iterations-1 are present
  for (int i = 0; i < num_iterations; i++) {
    EXPECT_TRUE(consumed_values.count(i) > 0) << "Value " << i << " was lost";
  }
}

TEST(UnboundedQueueTest, MoveSemantics) {
  struct NonCopyable {
    int value;
    NonCopyable(int v) : value(v) {}
    NonCopyable(const NonCopyable&) = delete;
    NonCopyable& operator=(const NonCopyable&) = delete;
    NonCopyable(NonCopyable&& other) : value(other.value) { other.value = -1; }
    NonCopyable& operator=(NonCopyable&& other) {
      value = other.value;
      other.value = -1;
      return *this;
    }
  };

  unbounded_queue<NonCopyable> queue;

  queue.push(NonCopyable(42));
  NonCopyable result(0);
  EXPECT_EQ(queue.try_pop(result), queue_op_status::success);
  EXPECT_EQ(result.value, 42);
}
