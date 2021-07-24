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
#ifndef ROCKETMQ_UTILITY_SIZE_HPP_
#define ROCKETMQ_UTILITY_SIZE_HPP_

#include <cstddef>  // ptrdiff_t, size_t

#include <type_traits>  // std::common_type, std::make_signed

namespace rocketmq {

using ssize_t = std::common_type<std::ptrdiff_t, std::make_signed<size_t>::type>::type;

}  // namespace rocketmq

#endif  // ROCKETMQ_UTILITY_SIZE_HPP_
