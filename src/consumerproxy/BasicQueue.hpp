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
#ifndef ROCKETMQ_CONSUMERPROXY_BASICQUEUE_HPP_
#define ROCKETMQ_CONSUMERPROXY_BASICQUEUE_HPP_

#include <cstddef>  // size_t

#include <functional>  // std::less
#include <map>         // std::map
#include <mutex>       // std::mutex
#include <utility>     // std::move
#include <vector>      // std::vector

#include "consumerproxy/FlowControl.hpp"

namespace rocketmq {

/**
 * @note non thread-safe
 */
template <typename Identity,
          typename Message,
          typename Index,
          typename IndexAccessor,
          typename SizeAccessor,
          typename Compare = std::less<Index>>
class BasicQueue : public FlowControlNode {
 public:
  using IdentityType = Identity;
  using MessageType = Message;
  using IndexType = Index;

  BasicQueue(IdentityType identity, Index nan_index)
      : max_index_(std::move(nan_index)), identity_(std::move(identity)) {}

  std::mutex& mutex() { return mutex_; }

  const IdentityType& identity() const { return identity_; }

  bool dropped() const { return dropped_; }

  void Drop() { dropped_ = true; }

  template <typename InputIterator>
  bool Put(InputIterator first, InputIterator last) {
    bool empty = message_cache_.empty();
    for (; first != last; ++first) {
      PutImpl(*first);
    }
    return empty;
  }

  bool Put(MessageType message) {
    bool empty = message_cache_.empty();
    PutImpl(std::move(message));
    return empty;
  }

  std::vector<MessageType> Take(size_t batch_size, const IndexType* upper_bound, bool* empty, IndexType* next_index) {
    std::vector<MessageType> messages;
    messages.reserve(batch_size);

    // take messages
    for (auto it = message_cache_.begin(); it != message_cache_.end() && batch_size-- > 0;) {
      if (upper_bound != nullptr && *upper_bound <= it->first) {
        break;
      }
      consuming_message_cache_[std::move(it->first)] = it->second;
      messages.push_back(std::move(it->second));
      it = message_cache_.erase(it);
    }

    // set output
    if (empty != nullptr || next_index != nullptr) {
      if (!message_cache_.empty()) {
        if (empty != nullptr) {
          *empty = false;
        }
        if (next_index != nullptr) {
          auto it = message_cache_.begin();
          *next_index = it->first;
        }
      } else {
        if (empty != nullptr) {
          *empty = true;
        }
        if (next_index != nullptr) {
          *next_index = max_index_;
          ++(*next_index);
        }
      }
    }

    messages.shrink_to_fit();
    return messages;
  }

  template <typename InputIterator>
  bool Commit(InputIterator first, InputIterator last) {
    for (; first != last; ++first) {
      CommitImpl(*first);
    }
    return consuming_message_cache_.empty();
  }

  bool Commit(const MessageType& message) {
    CommitImpl(message);
    return consuming_message_cache_.empty();
  }

  template <typename InputIterator>
  bool Rollback(InputIterator first, InputIterator last) {
    for (; first != last; ++first) {
      Rollback(*first);
    }
    return consuming_message_cache_.empty();
  }

  bool Rollback(const MessageType& message) {
    RollbackImpl(message);
    return consuming_message_cache_.empty();
  }

 private:
  void PutImpl(MessageType message) {
    IndexType index = index_accessor_(message);
    if (compare_(max_index_, index)) {
      max_index_ = index;
    }
    Acquire(size_accessor_(message));
    message_cache_[std::move(index)] = std::move(message);
  }

  void CommitImpl(const MessageType& message) {
    IndexType index = index_accessor_(message);
    auto it = consuming_message_cache_.find(index);
    if (it != consuming_message_cache_.end()) {
      consuming_message_cache_.erase(it);
      Release(size_accessor_(it->second));
    }
  }

  void RollbackImpl(const MessageType& message) {
    IndexType index = index_accessor_(message);
    auto it = consuming_message_cache_.find(index);
    if (it != consuming_message_cache_.end()) {
      // FIXME: check message_cache
      message_cache_[std::move(index)] = std::move(it.second);
      consuming_message_cache_.erase(it);
    }
  }

 private:
  IndexAccessor index_accessor_;
  Compare compare_;
  SizeAccessor size_accessor_;

  std::mutex mutex_;

  // message cache
  std::map<IndexType, MessageType, Compare> message_cache_;
  std::map<IndexType, MessageType, Compare> consuming_message_cache_;  // for orderly
  IndexType max_index_;

  IdentityType identity_;

  bool dropped_{false};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMERPROXY_BASICQUEUE_HPP_
