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
#ifndef ROCKETMQ_COMMON_RESULTSTATE_HPP_
#define ROCKETMQ_COMMON_RESULTSTATE_HPP_

#include <exception>
#include <stdexcept>

namespace rocketmq {

template <typename R>
class ResultState {
 public:
  using ResultType = typename std::decay<R>::type;

 public:
  ResultState() = default;
  ResultState(ResultType result) : has_result_(true), result_(std::move(result)) {}
  ResultState(const std::exception_ptr& exception) : has_result_(true), exception_(exception) {}

  ~ResultState() = default;

  // disable copy
  ResultState(const ResultState&) = delete;
  ResultState& operator=(const ResultState&) = delete;

  // enable move
  ResultState(ResultState&&) noexcept = default;
  ResultState& operator=(ResultState&&) noexcept = default;

 public:
  ResultType& GetResult() {
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    } else {
      return result_;
    }
  }

 public:
  template <typename T = ResultType>
  void set_result(T&& result) {
    check_state();
    result_ = std::forward<T>(result);
  }

  void set_exception(const std::exception_ptr& exception) {
    check_state();
    exception_ = exception;
  }

 private:
  void check_state() {
    if (has_result_) {
      throw std::runtime_error("state has set value already");
    }
  }

 private:
  bool has_result_{false};
  ResultType result_;
  std::exception_ptr exception_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_RESULTSTATE_HPP_
