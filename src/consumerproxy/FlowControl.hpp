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
#ifndef ROCKETMQ_CONSUMERPROXY_FLOWCONTROL_HPP_
#define ROCKETMQ_CONSUMERPROXY_FLOWCONTROL_HPP_

#include "utility/Size.hpp"

namespace rocketmq {

enum class FlowControlState { kNormal, kSuppressed };

class FlowTracker;

class FlowControlNode {
 public:
  void Update(ssize_t count, ssize_t cache_size) {
    count_ += count;
    cache_size_ += cache_size;
  }
  void Update(const FlowTracker& tracker);

  void Acquire(ssize_t count, ssize_t cache_size) { Update(count, cache_size); }
  void Acquire(ssize_t cache_size) { Acquire(1, cache_size); }

  void Release(ssize_t count, ssize_t cache_size) { Update(-count, -cache_size); }
  void Release(ssize_t cache_size) { Release(1, cache_size); }

  bool Check(ssize_t count_threshold, ssize_t cache_size_threshold) const {
    return (count_threshold == -1 || count_ < count_threshold) &&
           (cache_size_threshold == -1 || cache_size_ < cache_size_threshold);
  }

  bool MaintainLT(ssize_t count_threshold, ssize_t cache_size_threshold) {
    if (Check(count_threshold, cache_size_threshold)) {
      if (state_ != FlowControlState::kNormal) {
        state_ = FlowControlState::kNormal;
      }
      return true;
    }
    if (state_ == FlowControlState::kNormal) {
      state_ = FlowControlState::kSuppressed;
    }
    return false;
  }

  bool MaintainET(ssize_t count_threshold, ssize_t cache_size_threshold) {
    if (Check(count_threshold, cache_size_threshold)) {
      if (state_ != FlowControlState::kNormal) {
        state_ = FlowControlState::kNormal;
        return true;
      }
    } else {
      if (state_ == FlowControlState::kNormal) {
        state_ = FlowControlState::kSuppressed;
        return true;
      }
    }
    return false;
  }

  ssize_t count() const { return count_; }
  ssize_t cache_size() const { return cache_size_; }

  FlowControlState state() const { return state_; }

 private:
  ssize_t count_{0};
  ssize_t cache_size_{0};

  FlowControlState state_{FlowControlState::kNormal};
};

class FlowTracker {
 public:
  FlowTracker(FlowControlNode& node) : node_(node), count_(node.count()), cache_size_(node.cache_size()) {}

  ssize_t count() const { return node_.count() - count_; }
  ssize_t cache_size() const { return node_.cache_size() - cache_size_; }

 private:
  FlowControlNode& node_;
  ssize_t count_;
  ssize_t cache_size_;
};

inline void FlowControlNode::Update(const FlowTracker& tracker) {
  Update(tracker.count(), tracker.cache_size());
}

}  // namespace rocketmq

#endif  // ROCKETMQ_CONSUMERPROXY_FLOWCONTROL_HPP_
