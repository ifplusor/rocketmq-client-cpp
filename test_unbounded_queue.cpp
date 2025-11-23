/*
 * Standalone test for unbounded_queue fix
 */
#include <iostream>
#include <atomic>
#include <set>
#include <thread>
#include <vector>
#include <mutex>

// Include the fixed unbounded_queue header
#include "../src/concurrent/conqueue_base.hpp"
#include "../src/concurrent/unbounded_queue.hpp"

using rocketmq::unbounded_queue;
using rocketmq::queue_op_status;

bool test_basic() {
  unbounded_queue<int> queue;
  int value;
  
  // Test empty queue
  if (queue.try_pop(value) != queue_op_status::empty) {
    std::cerr << "FAIL: Empty queue should return empty status\n";
    return false;
  }

  // Test single push and pop
  queue.push(42);
  if (queue.try_pop(value) != queue_op_status::success || value != 42) {
    std::cerr << "FAIL: Should pop value 42\n";
    return false;
  }

  // Queue should be empty again
  if (queue.try_pop(value) != queue_op_status::empty) {
    std::cerr << "FAIL: Queue should be empty after pop\n";
    return false;
  }

  std::cout << "PASS: Basic push/pop test\n";
  return true;
}

bool test_multiple() {
  unbounded_queue<int> queue;

  // Push multiple values
  for (int i = 0; i < 10; i++) {
    queue.push(i);
  }

  // Pop and verify values
  for (int i = 0; i < 10; i++) {
    int value;
    if (queue.try_pop(value) != queue_op_status::success || value != i) {
      std::cerr << "FAIL: Expected value " << i << " but got " << value << "\n";
      return false;
    }
  }

  // Queue should be empty
  int value;
  if (queue.try_pop(value) != queue_op_status::empty) {
    std::cerr << "FAIL: Queue should be empty\n";
    return false;
  }

  std::cout << "PASS: Multiple push/pop test\n";
  return true;
}

bool test_concurrent_single_element_stress() {
  // This test specifically stresses the single-element case
  // where the data loss bug would manifest
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
  if (consumed.load() != num_iterations) {
    std::cerr << "FAIL: Expected " << num_iterations << " consumed, got " << consumed.load() << "\n";
    return false;
  }

  if (consumed_values.size() != static_cast<size_t>(num_iterations)) {
    std::cerr << "FAIL: Expected " << num_iterations << " unique values, got " << consumed_values.size() << "\n";
    return false;
  }
  
  // Verify all values from 0 to num_iterations-1 are present
  for (int i = 0; i < num_iterations; i++) {
    if (consumed_values.count(i) == 0) {
      std::cerr << "FAIL: Value " << i << " was lost\n";
      return false;
    }
  }

  std::cout << "PASS: Concurrent single-element stress test (the critical test for data loss)\n";
  return true;
}

int main() {
  std::cout << "Testing unbounded_queue fix for data loss bug...\n\n";

  bool all_pass = true;
  
  all_pass &= test_basic();
  all_pass &= test_multiple();
  all_pass &= test_concurrent_single_element_stress();

  if (all_pass) {
    std::cout << "\n✓ All tests passed!\n";
    return 0;
  } else {
    std::cout << "\n✗ Some tests failed!\n";
    return 1;
  }
}
