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
#ifndef ROCKETMQ_C_CTRANSACTIONSTATUS_H_
#define ROCKETMQ_C_CTRANSACTIONSTATUS_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef enum E_CTransactionStatus {
  E_COMMIT_TRANSACTION = 0,
  E_ROLLBACK_TRANSACTION = 1,
  E_UNKNOWN_TRANSACTION = 2,
} CTransactionStatus;

#ifdef __cplusplus
}
#endif

#endif  // ROCKETMQ_C_CTRANSACTIONSTATUS_H_
