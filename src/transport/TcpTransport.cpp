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
#include "TcpTransport.h"

#include <chrono>

#ifndef WIN32
#include <arpa/inet.h>  // for sockaddr_in and inet_ntoa...
#include <netinet/tcp.h>
#include <sys/socket.h>  // for socket(), bind(), and connect()...
#endif

#include "Logging.h"
#include "TcpRemotingClient.h"
#include "UtilAll.h"

namespace rocketmq {

TcpTransport::TcpTransport(TcpRemotingClient* client, TcpTransportReadCallback callback)
    : m_event(nullptr),
      m_tcpConnectStatus(TCP_CONNECT_STATUS_INIT),
      m_statusMutex(),
      m_statusEvent(),
      m_readCallback(callback),
      m_tcpRemotingClient(client) {
  m_startTime = UtilAll::currentTimeMillis();
}

TcpTransport::~TcpTransport() {
  freeBufferEvent();
  m_readCallback = nullptr;
}

void TcpTransport::freeBufferEvent() {
  // freeBufferEvent is idempotent.

  // first, unlink BufferEvent
  if (m_event != nullptr) {
    m_event->setCallback(nullptr, nullptr, nullptr, nullptr);
  }

  // then, release BufferEvent
  m_event.reset();
}

void TcpTransport::setTcpConnectStatus(TcpConnectStatus connectStatus) {
  m_tcpConnectStatus = connectStatus;
}

TcpConnectStatus TcpTransport::getTcpConnectStatus() {
  return m_tcpConnectStatus;
}

TcpConnectStatus TcpTransport::waitTcpConnectEvent(int timeoutMillis) {
  if (m_tcpConnectStatus == TCP_CONNECT_STATUS_WAIT) {
    std::unique_lock<std::mutex> lock(m_statusMutex);
    if (!m_statusEvent.wait_for(lock, std::chrono::milliseconds(timeoutMillis),
                                [&] { return m_tcpConnectStatus != TCP_CONNECT_STATUS_WAIT; })) {
      LOG_INFO("connect timeout");
    }
  }
  return m_tcpConnectStatus;
}

// internal method
void TcpTransport::setTcpConnectEvent(TcpConnectStatus connectStatus) {
  TcpConnectStatus oldStatus = m_tcpConnectStatus.exchange(connectStatus, std::memory_order_relaxed);
  if (oldStatus == TCP_CONNECT_STATUS_WAIT) {
    // awake waiting thread
    m_statusEvent.notify_all();
  }
}

u_long TcpTransport::resolveInetAddr(std::string& hostname) {
  // TODO: support ipv6
  u_long addr = inet_addr(hostname.c_str());
  if (INADDR_NONE == addr) {
    auto ip = lookupNameServers(hostname);
    addr = inet_addr(ip.c_str());
  }
  return addr;
}

void TcpTransport::disconnect(const std::string& addr) {
  // disconnect is idempotent.
  std::lock_guard<std::mutex> lock(m_eventMutex);
  if (getTcpConnectStatus() != TCP_CONNECT_STATUS_INIT) {
    LOG_INFO("disconnect:%s start. event:%p", addr.c_str(), (void*)m_event.get());
    freeBufferEvent();
    setTcpConnectEvent(TCP_CONNECT_STATUS_INIT);
    LOG_INFO("disconnect:%s completely", addr.c_str());
  }
}

TcpConnectStatus TcpTransport::connect(const std::string& strServerURL, int timeoutMillis) {
  std::string hostname;
  short port;

  LOG_DEBUG("connect to [%s].", strServerURL.c_str());
  if (!UtilAll::SplitURL(strServerURL, hostname, port)) {
    LOG_INFO("connect to [%s] failed, Invalid url.", strServerURL.c_str());
    return TCP_CONNECT_STATUS_FAILED;
  }

  {
    std::lock_guard<std::mutex> lock(m_eventMutex);

    // TODO: support ipv6
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = resolveInetAddr(hostname);
    sin.sin_port = htons(port);

    // create and configure BufferEvent
    m_event.reset(EventLoop::GetDefaultEventLoop()->createBufferEvent(-1, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE));
    m_event->setCallback(ReadCallback, nullptr, EventCallback, shared_from_this());
    m_event->setWatermark(EV_READ, 4, 0);
    m_event->enable(EV_READ | EV_WRITE);

    setTcpConnectStatus(TCP_CONNECT_STATUS_WAIT);
    if (m_event->connect((struct sockaddr*)&sin, sizeof(sin)) < 0) {
      LOG_INFO("connect to fd:%d failed", m_event->getfd());
      freeBufferEvent();
      setTcpConnectStatus(TCP_CONNECT_STATUS_FAILED);
      return TCP_CONNECT_STATUS_FAILED;
    }
  }

  if (timeoutMillis <= 0) {
    LOG_INFO("try to connect to fd:%d, addr:%s", m_event->getfd(), hostname.c_str());
    return TCP_CONNECT_STATUS_WAIT;
  }

  TcpConnectStatus connectStatus = waitTcpConnectEvent(timeoutMillis);
  if (connectStatus != TCP_CONNECT_STATUS_SUCCESS) {
    LOG_WARN("can not connect to server:%s", strServerURL.c_str());

    std::lock_guard<std::mutex> lock(m_eventMutex);
    freeBufferEvent();
    setTcpConnectStatus(TCP_CONNECT_STATUS_FAILED);
    return TCP_CONNECT_STATUS_FAILED;
  }

  return TCP_CONNECT_STATUS_SUCCESS;
}

void TcpTransport::EventCallback(BufferEvent* event, short what, TcpTransport* transport) {
  socket_t fd = event->getfd();
  LOG_INFO("eventcb: received event:%x on fd:%d", what, fd);
  if (what & BEV_EVENT_CONNECTED) {
    LOG_INFO("eventcb: connect to fd:%d successfully", fd);

    // disable Nagle
    int val = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void*)&val, sizeof(val));
    transport->setTcpConnectEvent(TCP_CONNECT_STATUS_SUCCESS);
  } else if (what & (BEV_EVENT_ERROR | BEV_EVENT_EOF | BEV_EVENT_READING | BEV_EVENT_WRITING)) {
    LOG_INFO("eventcb: received error event cb:%x on fd:%d", what, fd);
    // if error, stop callback.
    event->setCallback(nullptr, nullptr, nullptr, nullptr);
    transport->setTcpConnectEvent(TCP_CONNECT_STATUS_FAILED);
  } else {
    LOG_ERROR("eventcb: received error event:%d on fd:%d", what, fd);
  }
}

void TcpTransport::ReadCallback(BufferEvent* event, TcpTransport* transport) {
  /* This callback is invoked when there is data to read on bev. */

  // protocol:  <length> <header length> <header data> <body data>
  //               1            2               3           4
  // rocketmq protocol contains 4 parts as following:
  //     1, big endian 4 bytes int, its length is sum of 2,3 and 4
  //     2, big endian 4 bytes int, its length is 3
  //     3, use json to serialization data
  //     4, application could self-defined binary data

  struct evbuffer* input = event->getInput();
  while (1) {
    struct evbuffer_iovec v[4];
    int n = evbuffer_peek(input, 4, NULL, v, sizeof(v) / sizeof(v[0]));

    char hdr[4];
    char* p = hdr;
    size_t needed = 4;

    for (int idx = 0; idx < n; idx++) {
      if (needed > 0) {
        size_t tmp = needed < v[idx].iov_len ? needed : v[idx].iov_len;
        memcpy(p, v[idx].iov_base, tmp);
        p += tmp;
        needed -= tmp;
      } else {
        break;
      }
    }

    if (needed > 0) {
      LOG_DEBUG("too little data received with sum = %d", 4 - needed);
      return;
    }

    uint32 totalLenOfOneMsg = *(uint32*)hdr;  // first 4 bytes, which indicates 1st part of protocol
    uint32 msgLen = ntohl(totalLenOfOneMsg);
    size_t recvLen = evbuffer_get_length(input);
    if (recvLen >= msgLen + 4) {
      LOG_DEBUG("had received all data. msgLen:%d, from:%d, recvLen:%d", msgLen, event->getfd(), recvLen);
    } else {
      LOG_DEBUG("didn't received whole. msgLen:%d, from:%d, recvLen:%d", msgLen, event->getfd(), recvLen);
      return;  // consider large data which was not received completely by now
    }

    if (msgLen > 0) {
      MemoryBlock msg(msgLen, true);

      event->read(hdr, 4);  // skip length field
      event->read(msg.getData(), msgLen);

      transport->messageReceived(msg, event->getPeerAddrPort());
    }
  }
}

void TcpTransport::messageReceived(const MemoryBlock& mem, const std::string& addr) {
  if (m_readCallback != nullptr) {
    m_readCallback(m_tcpRemotingClient, mem, addr);
  }
}

bool TcpTransport::sendMessage(const char* pData, size_t len) {
  std::lock_guard<std::mutex> lock(m_eventMutex);
  if (getTcpConnectStatus() != TCP_CONNECT_STATUS_SUCCESS) {
    return false;
  }

  /* NOTE:
      do not need to consider large data which could not send by once, as
      bufferevent could handle this case;
   */
  return m_event != nullptr && m_event->write(pData, len) == 0;
}

const std::string TcpTransport::getPeerAddrAndPort() {
  std::lock_guard<std::mutex> lock(m_eventMutex);
  return m_event ? m_event->getPeerAddrPort() : "";
}

const uint64_t TcpTransport::getStartTime() const {
  return m_startTime;
}

}  // namespace rocketmq
