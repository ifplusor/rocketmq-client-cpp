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
#ifndef __TCPTRANSPORT_H__
#define __TCPTRANSPORT_H__

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "EventLoop.h"
#include "dataBlock.h"

namespace rocketmq {

typedef enum TcpConnectStatus {
  TCP_CONNECT_STATUS_INIT = 0,
  TCP_CONNECT_STATUS_WAIT = 1,
  TCP_CONNECT_STATUS_SUCCESS = 2,
  TCP_CONNECT_STATUS_FAILED = 3
} TcpConnectStatus;

using TcpTransportReadCallback = void (*)(void* context, const MemoryBlock&, const std::string&);

class TcpRemotingClient;

class TcpTransport : public std::enable_shared_from_this<TcpTransport> {
 public:
  static std::shared_ptr<TcpTransport> CreateTransport(TcpRemotingClient* pTcpRemotingClient,
                                                       TcpTransportReadCallback handle = nullptr) {
    // transport must be managed by smart pointer
    std::shared_ptr<TcpTransport> transport(new TcpTransport(pTcpRemotingClient, handle));
    return transport;
  }

  virtual ~TcpTransport();

  void disconnect(const std::string& addr);
  TcpConnectStatus connect(const std::string& strServerURL, int timeoutMillis = 3000);
  TcpConnectStatus waitTcpConnectEvent(int timeoutMillis = 3000);
  TcpConnectStatus getTcpConnectStatus();

  bool sendMessage(const char* pData, size_t len);
  const std::string getPeerAddrAndPort();
  const uint64_t getStartTime() const;

 private:
  // don't instance object directly.
  TcpTransport(TcpRemotingClient* client, TcpTransportReadCallback callback = nullptr);

  // buffer
  static void ReadCallback(BufferEvent* event, TcpTransport* transport);
  static void EventCallback(BufferEvent* event, short what, TcpTransport* transport);

  void messageReceived(const MemoryBlock& mem, const std::string& addr);
  void freeBufferEvent();  // not thread-safe

  void setTcpConnectEvent(TcpConnectStatus connectStatus);
  void setTcpConnectStatus(TcpConnectStatus connectStatus);

  // convert host to binary
  u_long resolveInetAddr(std::string& hostname);

 private:
  uint64_t m_startTime;

  std::shared_ptr<BufferEvent> m_event;  // NOTE: use m_event in callback is unsafe.
  std::mutex m_eventMutex;

  std::atomic<TcpConnectStatus> m_tcpConnectStatus;
  std::mutex m_statusMutex;
  std::condition_variable m_statusEvent;

  // read data callback
  TcpTransportReadCallback m_readCallback;
  TcpRemotingClient* m_tcpRemotingClient;
};

}  // namespace rocketmq

#endif
