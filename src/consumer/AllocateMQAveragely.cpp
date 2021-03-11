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
#include "AllocateMQAveragely.h"

#include <algorithm>

#include "Logging.h"
#include "MQException.h"

namespace rocketmq {

std::vector<MQMessageQueue> AllocateMQAveragely(const std::string& current_cid,
                                                std::vector<MQMessageQueue>& all_mqs,
                                                std::vector<std::string>& all_cids) {
  std::vector<MQMessageQueue> result;

  if (current_cid.empty()) {
    THROW_MQEXCEPTION(MQClientException, "current_cid is empty", -1);
  }

  if (all_cids.empty()) {
    THROW_MQEXCEPTION(MQClientException, "all_cids is empty", -1);
  }

  if (all_mqs.empty()) {
    THROW_MQEXCEPTION(MQClientException, "all_mqs is empty", -1);
  }

  // sort
  sort(all_mqs.begin(), all_mqs.end());
  sort(all_cids.begin(), all_cids.end());

  int index = -1;
  int cid_count = all_cids.size();
  for (int i = 0; i < cid_count; i++) {
    if (all_cids[i] == current_cid) {
      index = i;
      break;
    }
  }

  if (index == -1) {
    LOG_ERROR("could not find clientId from Broker");
    return result;
  }

  int mq_count = all_mqs.size();
  int mod = mq_count % cid_count;
  int average_size =
      mq_count <= cid_count ? 1 : (mod > 0 && index < mod ? mq_count / cid_count + 1 : mq_count / cid_count);
  int start_index = (mod > 0 && index < mod) ? index * average_size : index * average_size + mod;
  int range = (std::min)(average_size, mq_count - start_index);
  LOG_INFO("range is:%d, index is:%d, mq_count is:%d, average_size is:%d, start_index is:%d", range, index, mq_count,
           average_size, start_index);
  // out
  if (range >= 0) {  // example: range is:-1, index is:1, mqAllSize is:1, averageSize is:1, startIndex is:2
    for (int i = 0; i < range; i++) {
      if ((start_index + i) >= 0) {
        result.push_back(all_mqs.at((start_index + i) % mq_count));
      }
    }
  }

  return result;
}

}  // namespace rocketmq
