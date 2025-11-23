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
#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>

#include "concurrent/unbounded_queue.hpp"

using rocketmq::unbounded_queue;
using rocketmq::queue_op_status;

TEST(UnboundedQueueTest, BasicPushPop) {
  unbounded_queue<int> queue;
  
  EXPECT_TRUE(queue.is_empty());
  
  queue.push(1);
  queue.push(2);
  queue.push(3);
  
  EXPECT_FALSE(queue.is_empty());
  
  int value;
  EXPECT_EQ(queue.try_pop(value), queue_op_status::success);
  EXPECT_EQ(value, 1);
  
  EXPECT_EQ(queue.try_pop(value), queue_op_status::success);
  EXPECT_EQ(value, 2);
  
  EXPECT_EQ(queue.try_pop(value), queue_op_status::success);
  EXPECT_EQ(value, 3);
  
  EXPECT_EQ(queue.try_pop(value), queue_op_status::empty);
  EXPECT_TRUE(queue.is_empty());
}

TEST(UnboundedQueueTest, MoveSemantics) {
  unbounded_queue<std::string> queue;
  
  std::string str1 = "test1";
  std::string str2 = "test2";
  
  queue.push(std::move(str1));
  queue.push(std::move(str2));
  
  std::string result;
  EXPECT_EQ(queue.try_pop(result), queue_op_status::success);
  EXPECT_EQ(result, "test1");
  
  EXPECT_EQ(queue.try_pop(result), queue_op_status::success);
  EXPECT_EQ(result, "test2");
}

TEST(UnboundedQueueTest, ConcurrentPushSinglePop) {
  unbounded_queue<int> queue;
  const int num_threads = 4;
  const int items_per_thread = 1000;
  
  std::vector<std::thread> threads;
  
  // Multiple producers
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&queue, t, items_per_thread]() {
      for (int i = 0; i < items_per_thread; ++i) {
        queue.push(t * items_per_thread + i);
      }
    });
  }
  
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Single consumer
  std::vector<int> results;
  int value;
  while (queue.try_pop(value) == queue_op_status::success) {
    results.push_back(value);
  }
  
  EXPECT_EQ(results.size(), num_threads * items_per_thread);
  
  // All values should be present
  std::sort(results.begin(), results.end());
  for (int i = 0; i < num_threads * items_per_thread; ++i) {
    EXPECT_EQ(results[i], i);
  }
}

TEST(UnboundedQueueTest, ConcurrentMultiPushMultiPop) {
  unbounded_queue<int> queue;
  const int num_producers = 4;
  const int num_consumers = 4;
  const int items_per_producer = 1000;
  
  std::atomic<int> push_count{0};
  std::atomic<int> pop_count{0};
  std::vector<std::thread> threads;
  
  // Multiple producers
  for (int t = 0; t < num_producers; ++t) {
    threads.emplace_back([&queue, &push_count, t, items_per_producer]() {
      for (int i = 0; i < items_per_producer; ++i) {
        queue.push(t * items_per_producer + i);
        push_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  
  // Multiple consumers
  std::vector<std::vector<int>> consumer_results(num_consumers);
  for (int t = 0; t < num_consumers; ++t) {
    threads.emplace_back([&queue, &pop_count, &consumer_results, t]() {
      int value;
      while (true) {
        if (queue.try_pop(value) == queue_op_status::success) {
          consumer_results[t].push_back(value);
          pop_count.fetch_add(1, std::memory_order_relaxed);
        } else {
          // Give producers a chance
          std::this_thread::yield();
          // Check if all pushes are done
          if (pop_count.load(std::memory_order_relaxed) >= num_producers * items_per_producer) {
            break;
          }
        }
      }
    });
  }
  
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Verify all items were popped
  std::vector<int> all_results;
  for (const auto& results : consumer_results) {
    all_results.insert(all_results.end(), results.begin(), results.end());
  }
  
  EXPECT_EQ(all_results.size(), num_producers * items_per_producer);
  
  // All values should be present exactly once
  std::sort(all_results.begin(), all_results.end());
  for (int i = 0; i < num_producers * items_per_producer; ++i) {
    EXPECT_EQ(all_results[i], i);
  }
}

TEST(UnboundedQueueTest, StressTestRapidPushPop) {
  unbounded_queue<int> queue;
  const int duration_ms = 1000;
  std::atomic<bool> stop{false};
  std::atomic<int> push_total{0};
  std::atomic<int> pop_total{0};
  
  std::vector<std::thread> threads;
  
  // Producer thread
  threads.emplace_back([&]() {
    int count = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      queue.push(count++);
      push_total.fetch_add(1, std::memory_order_relaxed);
    }
  });
  
  // Consumer thread
  threads.emplace_back([&]() {
    int value;
    while (!stop.load(std::memory_order_relaxed)) {
      if (queue.try_pop(value) == queue_op_status::success) {
        pop_total.fetch_add(1, std::memory_order_relaxed);
      }
    }
  });
  
  // Run for specified duration
  std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
  stop.store(true, std::memory_order_relaxed);
  
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Drain remaining items
  int value;
  while (queue.try_pop(value) == queue_op_status::success) {
    pop_total.fetch_add(1, std::memory_order_relaxed);
  }
  
  // All pushed items should be popped
  EXPECT_EQ(push_total.load(), pop_total.load());
}

TEST(UnboundedQueueTest, IsEmptyConsistency) {
  unbounded_queue<int> queue;
  
  // Test empty queue
  EXPECT_TRUE(queue.is_empty());
  
  // Push and check not empty
  queue.push(1);
  EXPECT_FALSE(queue.is_empty());
  
  // Pop and check empty
  int value;
  queue.try_pop(value);
  EXPECT_TRUE(queue.is_empty());
  
  // Multiple items
  queue.push(1);
  queue.push(2);
  EXPECT_FALSE(queue.is_empty());
  
  queue.try_pop(value);
  EXPECT_FALSE(queue.is_empty());
  
  queue.try_pop(value);
  EXPECT_TRUE(queue.is_empty());
}

TEST(UnboundedQueueTest, DestructorClearWhenDestruct) {
  {
    unbounded_queue<int> queue(true);
    for (int i = 0; i < 100; ++i) {
      queue.push(i);
    }
    // Destructor should clear the queue
  }
  
  {
    unbounded_queue<int> queue(false);
    for (int i = 0; i < 100; ++i) {
      queue.push(i);
    }
    // Destructor should not clear, but should not crash
  }
}

TEST(UnboundedQueueTest, SingleElementConcurrency) {
  // Test the edge case where queue has exactly one element
  // and concurrent push/pop operations occur
  unbounded_queue<int> queue;
  const int iterations = 10000;
  std::atomic<int> push_count{0};
  std::atomic<int> pop_count{0};
  
  std::vector<std::thread> threads;
  
  // Pusher
  threads.emplace_back([&]() {
    for (int i = 0; i < iterations; ++i) {
      queue.push(i);
      push_count.fetch_add(1, std::memory_order_relaxed);
    }
  });
  
  // Popper
  threads.emplace_back([&]() {
    int value;
    int popped = 0;
    while (popped < iterations) {
      if (queue.try_pop(value) == queue_op_status::success) {
        pop_count.fetch_add(1, std::memory_order_relaxed);
        popped++;
      }
    }
  });
  
  for (auto& thread : threads) {
    thread.join();
  }
  
  EXPECT_EQ(push_count.load(), iterations);
  EXPECT_EQ(pop_count.load(), iterations);
  EXPECT_TRUE(queue.is_empty());
}
