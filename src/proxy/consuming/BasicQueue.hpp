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
#ifndef ROCKETMQ_PROXY_CONSUMING_BASICQUEUE_HPP_
#define ROCKETMQ_PROXY_CONSUMING_BASICQUEUE_HPP_

/*

  BasicQueue synopsis:

namespace rocketmq {

template <typename Identity,
          typename Message,
          typename Offset,
          typename OffsetAccessor,
          typename SizeAccessor,
          typename Compare = std::less<Offset>>
class BasicQueue {
 public:
  using IdentityType = Identity;
  using MessageType = Message;
  using OffsetType = Offset;

  BasicQueue(IdentityType identity, OffsetType nan_offset);

  std::mutex& mutex();
  bool dropped() const;

  void Drop();

  template <typename InputIterator>
  bool Put(InputIterator first, InputIterator last);

  bool Put(MessageType message);

  std::vector<MessageType> Take(size_t batch_size, bool* empty);

  template <typename InputIterator>
  bool Commit(InputIterator first, InputIterator last);

  bool Commit(const MessageType& message);

  template <typename InputIterator>
  bool Rollback(InputIterator first, InputIterator last);

  bool Rollback(const MessageType& message);
};

}  // namespace rocketmq

*/

#include <cstddef>  // size_t

#include <functional>  // std::less
#include <map>         // std::map
#include <mutex>       // std::mutex
#include <utility>     // std::move
#include <vector>      // std::vector

#include "proxy/consuming/FlowControl.hpp"

namespace rocketmq {

/**
 * @note non thread-safe
 */
template <typename Identity,
          typename Message,
          typename Offset,
          typename OffsetAccessor,
          typename SizeAccessor,
          typename Compare = std::less<Offset>>
class BasicQueue : public FlowControlNode {
 public:
  using IdentityType = Identity;
  using MessageType = Message;
  using OffsetType = Offset;

  BasicQueue(IdentityType identity, OffsetType nan_offset)
      : max_offset_(std::move(nan_offset)), identity_(std::move(identity)) {}

  std::mutex& mutex() { return mutex_; }

  const IdentityType& identity() const { return identity_; }

  bool dropped() const { return dropped_; }

  OffsetType commit_offset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return GetCommitOffset();
  }

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

  std::vector<MessageType> Take(size_t batch_size, bool* empty) { return Take(batch_size, nullptr, empty, nullptr); }

  std::vector<MessageType> Take(size_t batch_size,
                                const OffsetType* upper_bound,
                                bool* empty,
                                OffsetType* commit_offset) {
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
    if (empty != nullptr) {
      *empty = IsEmpty();
    }
    if (commit_offset != nullptr) {
      *commit_offset = GetCommitOffset();
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
    OffsetType offset = offset_accessor_(message);
    if (compare_(max_offset_, offset)) {
      max_offset_ = offset;
    }
    Acquire(size_accessor_(message));
    message_cache_[std::move(offset)] = std::move(message);
  }

  void CommitImpl(const MessageType& message) {
    OffsetType index = offset_accessor_(message);
    auto it = consuming_message_cache_.find(index);
    if (it != consuming_message_cache_.end()) {
      consuming_message_cache_.erase(it);
      Release(size_accessor_(it->second));
    }
  }

  void RollbackImpl(const MessageType& message) {
    OffsetType index = offset_accessor_(message);
    auto it = consuming_message_cache_.find(index);
    if (it != consuming_message_cache_.end()) {
      // FIXME: check message_cache
      message_cache_[std::move(index)] = std::move(it.second);
      consuming_message_cache_.erase(it);
    }
  }

  OffsetType GetCommitOffset() const {
    if (!consuming_message_cache_.empty()) {
      return consuming_message_cache_.begin()->first;
    }
    if (!message_cache_.empty()) {
      return message_cache_.begin()->first;
    }
    return ++OffsetType{max_offset_};
  }

  bool IsEmpty() const { return message_cache_.empty() && consuming_message_cache_.empty(); }

 private:
  OffsetAccessor offset_accessor_;
  SizeAccessor size_accessor_;
  Compare compare_;

  mutable std::mutex mutex_;

  // message cache
  std::map<OffsetType, MessageType, Compare> message_cache_;
  std::map<OffsetType, MessageType, Compare> consuming_message_cache_;  // for orderly
  OffsetType max_offset_;

  IdentityType identity_;

  bool dropped_{false};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_PROXY_CONSUMING_BASICQUEUE_HPP_
