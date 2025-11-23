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
#ifndef ROCKETMQ_CONCURRENT_UNBOUNDEDQUEUE_HPP_
#define ROCKETMQ_CONCURRENT_UNBOUNDEDQUEUE_HPP_

#include <atomic>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "concurrent/conqueue_base.hpp"

namespace rocketmq {

template <typename T>
class unbounded_queue;

namespace detail {

template <typename T>
struct unbounded_queue_node {
  using value_type = T;
  using type = unbounded_queue_node<value_type>;

  template <typename E,
            typename std::enable_if<std::is_same<typename std::decay<E>::type, value_type>::value, int>::type = 0>
  explicit unbounded_queue_node(E&& v) : value(std::forward<E>(v)) {}

  value_type value;
  std::atomic<type*> next;
};

}  // namespace detail

template <typename T>
class unbounded_queue {
 public:
  // types:
  using value_type = T;
  using node_type = detail::unbounded_queue_node<value_type>;

  ~unbounded_queue() {
    // clear this queue
    while (_clear_when_destruct) {
      value_type value;
      if (try_pop(value) != queue_op_status::success) {
        break;
      }
    }

    ::operator delete(sentinel);
  }

  unbounded_queue(bool clear_when_destruct = true)
      : sentinel(static_cast<node_type*>(::operator new(sizeof(node_type)))),
        _clear_when_destruct(clear_when_destruct) {
    sentinel->next.store(sentinel);
    head_ = tail_ = sentinel;
  }

  bool is_empty() { return sentinel == tail_.load(); }

  void push(const value_type& value) {
    auto* node = new node_type(value);
    push_impl(node);
  }
  void push(value_type&& value) {
    auto* node = new node_type(std::move(value));
    push_impl(node);
  }

  queue_op_status try_pop(value_type& element) {
    node_type* node = pop_impl();
    if (node == sentinel) {
      return queue_op_status::empty;
    }
    std::unique_ptr<node_type> node_guard(node);
    element = std::move(node->value);
    return queue_op_status::success;
  }

 private:
  void push_impl(node_type* node) noexcept {
    node->next.store(sentinel);
    auto tail = tail_.exchange(node);
    if (tail == sentinel) {
      head_.store(node);
    } else {
      // guarantee: tail is not released
      tail->next.store(node);
    }
  }

  node_type* pop_impl() noexcept {
    auto head = head_.load();
    for (size_t i = 1;; i++) {
      if (head == sentinel) {
        // no task, or it is/are not ready
        return sentinel;
      }
      if (head != nullptr) {
        if (head_.compare_exchange_weak(head, nullptr)) {
          auto next = head->next.load();
          if (next == sentinel) {
            auto t = head;
            // only one element
            if (tail_.compare_exchange_strong(t, sentinel)) {
              t = nullptr;
              head_.compare_exchange_strong(t, sentinel);
              return head;
            }
            size_t j = 0;
            do {
              // push-pop conflict, spin
              if (0 == ++j % 10) {
                std::this_thread::yield();
              }
              next = head->next.load();
            } while (next == sentinel);
          }
          head_.store(next);
          return head;
        }
      } else {
        head = head_.load();
      }
      if (0 == i % 15 && head != sentinel) {
        std::this_thread::yield();
        head = head_.load();
      }
    }
  }

  std::atomic<node_type*> head_, tail_;
  node_type* const sentinel;
  bool _clear_when_destruct;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONCURRENT_UNBOUNDEDQUEUE_HPP_
