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
#include "MQClientInstance.h"

#include <typeindex>

#include "ClientRemotingProcessor.h"
#include "ConsumerRunningInfo.h"
#include "Logging.h"
#include "MQAdminImpl.h"
#include "MQClientAPIImpl.h"
#include "MQClientManager.h"
#include "PermName.h"
#include "PullMessageService.h"
#include "PullRequest.h"
#include "RebalanceImpl.h"
#include "RebalanceService.h"
#include "TcpRemotingClient.h"
#include "TopicPublishInfo.h"
#include "UtilAll.h"

#define MAX_BUFF_SIZE 8192
#define PROCESS_NAME_BUF_SIZE 256
#define SAFE_BUFF_SIZE (MAX_BUFF_SIZE - PROCESS_NAME_BUF_SIZE)  // 8192 - 256 = 7936

namespace rocketmq {

MQClientInstance::MQClientInstance(MQClient* clientConfig, const std::string& clientId)
    : MQClientInstance(clientConfig, clientId, nullptr) {}

MQClientInstance::MQClientInstance(MQClient* clientConfig,
                                   const std::string& clientId,
                                   std::shared_ptr<RPCHook> rpcHook)
    : m_clientId(clientId),
      m_rebalanceService(new RebalanceService(this)),
      m_pullMessageService(new PullMessageService(this)),
      m_scheduledExecutorService("MQClient", false) {
  // default Topic register
  TopicPublishInfoPtr defaultTopicInfo(new TopicPublishInfo());
  m_topicPublishInfoTable[AUTO_CREATE_TOPIC_KEY_TOPIC] = defaultTopicInfo;

  m_pClientRemotingProcessor.reset(new ClientRemotingProcessor(this));
  m_clientAPIImpl.reset(new MQClientAPIImpl(m_pClientRemotingProcessor.get(), rpcHook, clientConfig));

  std::string namesrvAddr = clientConfig->getNamesrvAddr();
  if (!namesrvAddr.empty()) {
    m_clientAPIImpl->updateNameServerAddr(namesrvAddr);
    LOG_INFO_NEW("user specified name server address: {}", namesrvAddr);
  }

  m_mqAdminImpl.reset(new MQAdminImpl(this));

  m_serviceState = CREATE_JUST;
  LOG_DEBUG("MQClientInstance construct");
}

MQClientInstance::~MQClientInstance() {
  LOG_INFO("MQClientInstance:%s destruct", m_clientId.c_str());

  m_producerTable.clear();
  m_consumerTable.clear();
  m_topicPublishInfoTable.clear();
  m_topicRouteTable.clear();
  m_brokerAddrTable.clear();

  m_clientAPIImpl = nullptr;
}

TopicPublishInfoPtr MQClientInstance::topicRouteData2TopicPublishInfo(const std::string& topic,
                                                                      TopicRouteDataPtr route) {
  TopicPublishInfoPtr info(new TopicPublishInfo());
  info->setTopicRouteData(route);

  std::string orderTopicConf = route->getOrderTopicConf();
  if (!orderTopicConf.empty()) {  // order msg
    // "broker-a:8";"broker-b:8"
    std::vector<std::string> brokers;
    UtilAll::Split(brokers, orderTopicConf, ';');
    for (const auto& broker : brokers) {
      std::vector<std::string> item;
      UtilAll::Split(item, broker, ':');
      int nums = atoi(item[1].c_str());
      for (int i = 0; i < nums; i++) {
        info->getMessageQueueList().emplace_back(topic, item[0], i);
      }
    }
    info->setOrderTopic(true);
  } else {  // no order msg
    const auto& qds = route->getQueueDatas();
    for (const auto& qd : qds) {
      if (PermName::isWriteable(qd.perm)) {
        const BrokerData* brokerData = nullptr;
        for (const auto& bd : route->getBrokerDatas()) {
          if (bd.brokerName == qd.brokerName) {
            brokerData = &bd;
            break;
          }
        }

        if (nullptr == brokerData) {
          continue;
        }

        if (brokerData->brokerAddrs.find(MASTER_ID) == brokerData->brokerAddrs.end()) {
          continue;
        }

        for (int i = 0; i < qd.writeQueueNums; i++) {
          info->getMessageQueueList().emplace_back(topic, qd.brokerName, i);
        }
      }
    }
    info->setOrderTopic(false);
  }

  return info;
}

std::vector<MQMessageQueue> MQClientInstance::topicRouteData2TopicSubscribeInfo(const std::string& topic,
                                                                                TopicRouteDataPtr route) {
  std::vector<MQMessageQueue> mqList;
  const auto& queueDatas = route->getQueueDatas();
  for (const auto& qd : queueDatas) {
    if (PermName::isReadable(qd.perm)) {
      for (int i = 0; i < qd.readQueueNums; i++) {
        MQMessageQueue mq(topic, qd.brokerName, i);
        mqList.push_back(mq);
      }
    }
  }
  return mqList;
}

void MQClientInstance::start() {
  switch (m_serviceState) {
    case CREATE_JUST:
      LOG_INFO("MQClientInstance:%s start", m_clientId.c_str());
      m_serviceState = START_FAILED;

      m_clientAPIImpl->start();

      // start various schedule tasks
      startScheduledTask();

      // start pull service
      m_pullMessageService->start();

      // start rebalance service
      m_rebalanceService->start();

      LOG_INFO_NEW("the client factory [{}] start OK", m_clientId);
      m_serviceState = RUNNING;
      break;
    case RUNNING:
    case SHUTDOWN_ALREADY:
    case START_FAILED:
      LOG_INFO("The Factory object:%s start failed with fault state:%d", m_clientId.c_str(), m_serviceState);
      break;
    default:
      break;
  }
}

void MQClientInstance::shutdown() {
  if (getConsumerTableSize() != 0)
    return;

  if (getProducerTableSize() != 0)
    return;

  switch (m_serviceState) {
    case CREATE_JUST:
      break;
    case RUNNING: {
      m_serviceState = SHUTDOWN_ALREADY;
      m_pullMessageService->shutdown();
      m_scheduledExecutorService.shutdown();
      m_clientAPIImpl->shutdown();
      m_rebalanceService->shutdown();

      MQClientManager::getInstance()->removeMQClientInstance(m_clientId);
      LOG_INFO_NEW("the client factory [{}] shutdown OK", m_clientId);
    } break;
    case SHUTDOWN_ALREADY:
      break;
    default:
      break;
  }
}

bool MQClientInstance::isRunning() {
  return m_serviceState == RUNNING;
}

void MQClientInstance::startScheduledTask() {
  LOG_INFO("start scheduled task:%s", m_clientId.c_str());
  m_scheduledExecutorService.startup();

  // updateTopicRouteInfoFromNameServer
  m_scheduledExecutorService.schedule(std::bind(&MQClientInstance::updateTopicRouteInfoPeriodically, this), 10,
                                      time_unit::milliseconds);

  // sendHeartbeatToAllBroker
  m_scheduledExecutorService.schedule(std::bind(&MQClientInstance::sendHeartbeatToAllBrokerPeriodically, this), 1000,
                                      time_unit::milliseconds);

  // persistAllConsumerOffset
  m_scheduledExecutorService.schedule(std::bind(&MQClientInstance::persistAllConsumerOffsetPeriodically, this),
                                      1000 * 10, time_unit::milliseconds);
}

void MQClientInstance::updateTopicRouteInfoPeriodically() {
  updateTopicRouteInfoFromNameServer();

  // next round
  m_scheduledExecutorService.schedule(std::bind(&MQClientInstance::updateTopicRouteInfoPeriodically, this), 1000 * 30,
                                      time_unit::milliseconds);
}

void MQClientInstance::sendHeartbeatToAllBrokerPeriodically() {
  sendHeartbeatToAllBroker();

  // next round
  m_scheduledExecutorService.schedule(std::bind(&MQClientInstance::sendHeartbeatToAllBrokerPeriodically, this),
                                      1000 * 30, time_unit::milliseconds);
}

void MQClientInstance::persistAllConsumerOffsetPeriodically() {
  persistAllConsumerOffset();

  // next round
  m_scheduledExecutorService.schedule(std::bind(&MQClientInstance::persistAllConsumerOffsetPeriodically, this),
                                      1000 * 5, time_unit::milliseconds);
}

std::string MQClientInstance::getClientId() {
  return m_clientId;
}

void MQClientInstance::updateTopicRouteInfoFromNameServer() {
  std::set<std::string> topicList;

  // Consumer
  getTopicListFromConsumerSubscription(topicList);

  // Producer
  getTopicListFromTopicPublishInfo(topicList);

  // update
  if (!topicList.empty()) {
    for (const auto& topic : topicList) {
      updateTopicRouteInfoFromNameServer(topic);
    }
  }
}

void MQClientInstance::sendHeartbeatToAllBrokerWithLock() {
  if (m_lockHeartbeat.try_lock()) {
    std::lock_guard<std::timed_mutex> lock(m_lockHeartbeat, std::adopt_lock);
    sendHeartbeatToAllBroker();
  } else {
    LOG_WARN("lock heartBeat, but failed.");
  }
}

void MQClientInstance::persistAllConsumerOffset() {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  for (const auto& it : m_consumerTable) {
    LOG_DEBUG("Client factory start persistAllConsumerOffset");
    it.second->persistConsumerOffset();
  }
}

void MQClientInstance::sendHeartbeatToAllBroker() {
  std::unique_ptr<HeartbeatData> heartbeatData(prepareHeartbeatData());
  bool producerEmpty = heartbeatData->isProducerDataSetEmpty();
  bool consumerEmpty = heartbeatData->isConsumerDataSetEmpty();
  if (producerEmpty && consumerEmpty) {
    LOG_WARN("sending heartbeat, but no consumer and no producer");
    return;
  }

  BrokerAddrMAP brokerAddrTable(getBrokerAddrMap());
  if (!brokerAddrTable.empty()) {
    for (const auto& it : brokerAddrTable) {
      // const auto& brokerName = it.first;
      const auto& oneTable = it.second;
      for (const auto& it2 : oneTable) {
        const auto id = it2.first;
        const auto& addr = it2.second;
        if (consumerEmpty && id != MASTER_ID) {
          continue;
        }

        try {
          m_clientAPIImpl->sendHearbeat(addr, heartbeatData.get());
        } catch (MQException& e) {
          LOG_ERROR(e.what());
        }
      }
    }
    brokerAddrTable.clear();
  } else {
    LOG_WARN("sendheartbeat brokerAddrTable is empty");
  }
}

bool MQClientInstance::updateTopicRouteInfoFromNameServer(const std::string& topic, bool isDefault) {
  static const long LOCK_TIMEOUT_MILLIS = 3000L;
  if (UtilAll::try_lock_for(m_lockNamesrv, LOCK_TIMEOUT_MILLIS)) {
    std::lock_guard<std::timed_mutex> lock(m_lockNamesrv, std::adopt_lock);
    LOG_INFO("updateTopicRouteInfoFromNameServer start:%s", topic.c_str());

    try {
      TopicRouteDataPtr topicRouteData;
      if (isDefault) {
        topicRouteData.reset(m_clientAPIImpl->getTopicRouteInfoFromNameServer(AUTO_CREATE_TOPIC_KEY_TOPIC, 1000 * 3));
        if (topicRouteData != nullptr) {
          auto& queueDatas = topicRouteData->getQueueDatas();
          for (auto& qd : queueDatas) {
            int queueNums = std::min(4, qd.readQueueNums);
            qd.readQueueNums = queueNums;
            qd.writeQueueNums = queueNums;
          }
        }
        LOG_DEBUG("getTopicRouteInfoFromNameServer is null for topic: %s", topic.c_str());
      } else {
        topicRouteData.reset(m_clientAPIImpl->getTopicRouteInfoFromNameServer(topic, 1000 * 3));
      }
      if (topicRouteData != nullptr) {
        LOG_INFO("updateTopicRouteInfoFromNameServer has data");
        auto old = getTopicRouteData(topic);
        bool changed = topicRouteDataIsChange(old.get(), topicRouteData.get());

        if (changed) {
          LOG_INFO("updateTopicRouteInfoFromNameServer changed:%s", topic.c_str());

          // update broker addr
          const auto& brokerDatas = topicRouteData->getBrokerDatas();
          for (const auto& bd : brokerDatas) {
            LOG_INFO("updateTopicRouteInfoFromNameServer changed with broker name:%s", bd.brokerName.c_str());
            addBrokerToAddrMap(bd.brokerName, bd.brokerAddrs);
          }

          // update publish info
          {
            TopicPublishInfoPtr publishInfo(topicRouteData2TopicPublishInfo(topic, topicRouteData));
            publishInfo->setHaveTopicRouterInfo(true);
            updateProducerTopicPublishInfo(topic, publishInfo);
          }

          // update subscribe info
          if (getConsumerTableSize() > 0) {
            std::vector<MQMessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
            updateConsumerTopicSubscribeInfo(topic, subscribeInfo);
          }

          addTopicRouteData(topic, topicRouteData);
        }

        LOG_DEBUG("updateTopicRouteInfoFromNameServer end:%s", topic.c_str());
        return true;
      } else {
        LOG_WARN_NEW("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}",
                     topic);
      }
    } catch (const std::exception& e) {
      if (!UtilAll::isRetryTopic(topic) && topic != AUTO_CREATE_TOPIC_KEY_TOPIC) {
        LOG_WARN_NEW("updateTopicRouteInfoFromNameServer Exception, {}", e.what());
      }
    }
  } else {
    LOG_WARN_NEW("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
  }

  return false;
}

HeartbeatData* MQClientInstance::prepareHeartbeatData() {
  HeartbeatData* pHeartbeatData = new HeartbeatData();

  // clientID
  pHeartbeatData->setClientID(m_clientId);

  // Consumer
  insertConsumerInfoToHeartBeatData(pHeartbeatData);

  // Producer
  insertProducerInfoToHeartBeatData(pHeartbeatData);

  return pHeartbeatData;
}

void MQClientInstance::insertConsumerInfoToHeartBeatData(HeartbeatData* pHeartbeatData) {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  for (const auto& it : m_consumerTable) {
    const auto* consumer = it.second;
    ConsumerData consumerData;
    consumerData.groupName = consumer->groupName();
    consumerData.consumeType = consumer->consumeType();
    consumerData.messageModel = consumer->messageModel();
    consumerData.consumeFromWhere = consumer->consumeFromWhere();
    std::vector<SubscriptionData> result = consumer->subscriptions();
    consumerData.subscriptionDataSet.swap(result);
    // TODO: unitMode

    pHeartbeatData->insertDataToConsumerDataSet(consumerData);
  }
}

void MQClientInstance::insertProducerInfoToHeartBeatData(HeartbeatData* pHeartbeatData) {
  std::lock_guard<std::mutex> lock(m_producerTableMutex);
  for (const auto& it : m_producerTable) {
    ProducerData producerData;
    producerData.groupName = it.first;
    pHeartbeatData->insertDataToProducerDataSet(producerData);
  }
}

bool MQClientInstance::topicRouteDataIsChange(TopicRouteData* olddata, TopicRouteData* nowdata) {
  if (olddata == nullptr || nowdata == nullptr) {
    return true;
  }
  return !(*olddata == *nowdata);
}

TopicRouteDataPtr MQClientInstance::getTopicRouteData(const std::string& topic) {
  std::lock_guard<std::mutex> lock(m_topicRouteTableMutex);
  auto iter = m_topicRouteTable.find(topic);
  if (iter != m_topicRouteTable.end()) {
    return iter->second;
  }
  return TopicRouteDataPtr();
}

void MQClientInstance::addTopicRouteData(const std::string& topic, TopicRouteDataPtr topicRouteData) {
  std::lock_guard<std::mutex> lock(m_topicRouteTableMutex);
  m_topicRouteTable.emplace(topic, topicRouteData);
}

bool MQClientInstance::registerConsumer(const std::string& group, MQConsumer* consumer) {
  if (group.empty()) {
    return false;
  }

  if (!addConsumerToTable(group, consumer)) {
    LOG_WARN_NEW("the consumer group[{}] exist already.", group);
    return false;
  }

  LOG_DEBUG_NEW("registerConsumer success:{}", group);
  return true;
}

void MQClientInstance::unregisterConsumer(const std::string& group) {
  eraseConsumerFromTable(group);
  unregisterClient(null, group);
}

void MQClientInstance::unregisterClient(const std::string& producerGroup, const std::string& consumerGroup) {
  // FIXME: lock heartBeat
  BrokerAddrMAP brokerAddrTable(getBrokerAddrMap());
  for (const auto& it : brokerAddrTable) {
    const auto& brokerName = it.first;
    const auto& oneTable = it.second;
    for (const auto& it2 : oneTable) {
      const auto& index = it2.first;
      const auto& addr = it2.second;
      try {
        m_clientAPIImpl->unregisterClient(addr, m_clientId, producerGroup, consumerGroup);
        LOG_INFO_NEW("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup,
                     consumerGroup, brokerName, index, addr);
      } catch (const std::exception& e) {
        LOG_ERROR_NEW("unregister client exception from broker: {}. EXCEPTION: {}", addr, e.what());
      }
    }
  }
}

bool MQClientInstance::registerProducer(const std::string& group, MQProducer* producer) {
  if (group.empty()) {
    return false;
  }

  if (!addProducerToTable(group, producer)) {
    LOG_WARN_NEW("the consumer group[{}] exist already.", group);
    return false;
  }

  LOG_DEBUG_NEW("registerProducer success:{}", group);
  return true;
}

void MQClientInstance::unregisterProducer(const std::string& group) {
  eraseProducerFromTable(group);
  unregisterClient(group, null);
}

void MQClientInstance::rebalanceImmediately() {
  m_rebalanceService->wakeup();
}

void MQClientInstance::doRebalance() {
  LOG_INFO("Client factory:%s start dorebalance", m_clientId.c_str());
  if (getConsumerTableSize() > 0) {
    std::lock_guard<std::mutex> lock(m_consumerTableMutex);
    for (auto& it : m_consumerTable) {
      it.second->doRebalance();
    }
  }
  LOG_INFO("Client factory:%s finish dorebalance", m_clientId.c_str());
}

void MQClientInstance::doRebalanceByConsumerGroup(const std::string& consumerGroup) {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  if (m_consumerTable.find(consumerGroup) != m_consumerTable.end()) {
    try {
      LOG_INFO("Client factory:%s start dorebalance for consumer:%s", m_clientId.c_str(), consumerGroup.c_str());
      MQConsumer* pMQConsumer = m_consumerTable[consumerGroup];
      pMQConsumer->doRebalance();
    } catch (const std::exception& e) {
      LOG_ERROR(e.what());
    }
  }
}

MQProducer* MQClientInstance::selectProducer(const std::string& producerName) {
  std::lock_guard<std::mutex> lock(m_producerTableMutex);
  if (m_producerTable.find(producerName) != m_producerTable.end()) {
    return m_producerTable[producerName];
  }
  return nullptr;
}

bool MQClientInstance::addProducerToTable(const std::string& producerName, MQProducer* producer) {
  std::lock_guard<std::mutex> lock(m_producerTableMutex);
  if (m_producerTable.find(producerName) != m_producerTable.end())
    return false;
  m_producerTable[producerName] = producer;
  return true;
}

void MQClientInstance::eraseProducerFromTable(const std::string& producerName) {
  std::lock_guard<std::mutex> lock(m_producerTableMutex);
  if (m_producerTable.find(producerName) != m_producerTable.end()) {
    m_producerTable.erase(producerName);
  }
}

int MQClientInstance::getProducerTableSize() {
  std::lock_guard<std::mutex> lock(m_producerTableMutex);
  return m_producerTable.size();
}

void MQClientInstance::getTopicListFromTopicPublishInfo(std::set<std::string>& topicList) {
  std::lock_guard<std::mutex> lock(m_topicPublishInfoTableMutex);
  for (const auto& it : m_topicPublishInfoTable) {
    topicList.insert(it.first);
  }
}

void MQClientInstance::updateProducerTopicPublishInfo(const std::string& topic, TopicPublishInfoPtr publishInfo) {
  addTopicInfoToTable(topic, publishInfo);
}

MQConsumer* MQClientInstance::selectConsumer(const std::string& group) {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  if (m_consumerTable.find(group) != m_consumerTable.end()) {
    return m_consumerTable[group];
  }
  return NULL;
}

bool MQClientInstance::addConsumerToTable(const std::string& consumerName, MQConsumer* consumer) {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  if (m_consumerTable.find(consumerName) != m_consumerTable.end()) {
    return false;
  } else {
    m_consumerTable[consumerName] = consumer;
    return true;
  }
}

void MQClientInstance::eraseConsumerFromTable(const std::string& consumerName) {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  if (m_consumerTable.find(consumerName) != m_consumerTable.end()) {
    m_consumerTable.erase(consumerName);  // do not need free consumer, as it was allocated by user
  } else {
    LOG_WARN("could not find consumer:%s from table", consumerName.c_str());
  }
}

int MQClientInstance::getConsumerTableSize() {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  return m_consumerTable.size();
}

void MQClientInstance::getTopicListFromConsumerSubscription(std::set<std::string>& topicList) {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  for (const auto& it : m_consumerTable) {
    std::vector<SubscriptionData> result = it.second->subscriptions();
    for (const auto& sd : result) {
      topicList.insert(sd.getTopic());
    }
  }
}

void MQClientInstance::updateConsumerTopicSubscribeInfo(const std::string& topic,
                                                        std::vector<MQMessageQueue> subscribeInfo) {
  std::lock_guard<std::mutex> lock(m_consumerTableMutex);
  for (auto& it : m_consumerTable) {
    it.second->updateTopicSubscribeInfo(topic, subscribeInfo);
  }
}

void MQClientInstance::addTopicInfoToTable(const std::string& topic, TopicPublishInfoPtr topicPublishInfo) {
  std::lock_guard<std::mutex> lock(m_topicPublishInfoTableMutex);
  m_topicPublishInfoTable[topic] = topicPublishInfo;
}

void MQClientInstance::eraseTopicInfoFromTable(const std::string& topic) {
  std::lock_guard<std::mutex> lock(m_topicPublishInfoTableMutex);
  if (m_topicPublishInfoTable.find(topic) != m_topicPublishInfoTable.end()) {
    m_topicPublishInfoTable.erase(topic);
  }
}

TopicPublishInfoPtr MQClientInstance::getTopicPublishInfoFromTable(const std::string& topic) {
  std::lock_guard<std::mutex> lock(m_topicPublishInfoTableMutex);
  if (m_topicPublishInfoTable.find(topic) != m_topicPublishInfoTable.end()) {
    return m_topicPublishInfoTable[topic];
  }
  return TopicPublishInfoPtr();
}

bool MQClientInstance::isTopicInfoValidInTable(const std::string& topic) {
  std::lock_guard<std::mutex> lock(m_topicPublishInfoTableMutex);
  auto iter = m_topicPublishInfoTable.find(topic);
  if (iter != m_topicPublishInfoTable.end()) {
    return iter->second->ok();
  }
  return false;
}

TopicPublishInfoPtr MQClientInstance::tryToFindTopicPublishInfo(const std::string& topic) {
  auto topicPublishInfo = getTopicPublishInfoFromTable(topic);
  if (nullptr == topicPublishInfo || !topicPublishInfo->ok()) {
    updateTopicRouteInfoFromNameServer(topic);
    topicPublishInfo = getTopicPublishInfoFromTable(topic);
  }

  if (nullptr != topicPublishInfo && (topicPublishInfo->isHaveTopicRouterInfo() || topicPublishInfo->ok())) {
    return topicPublishInfo;
  } else {
    LOG_INFO("updateTopicRouteInfoFromNameServer with default");
    updateTopicRouteInfoFromNameServer(topic, true);
    return getTopicPublishInfoFromTable(topic);
  }
}

FindBrokerResult* MQClientInstance::findBrokerAddressInAdmin(const std::string& brokerName) {
  BrokerAddrMAP brokerTable(getBrokerAddrMap());
  bool found = false;
  bool slave = false;
  string brokerAddr;

  if (brokerTable.find(brokerName) != brokerTable.end()) {
    map<int, string> brokerMap(brokerTable[brokerName]);
    map<int, string>::iterator it1 = brokerMap.begin();
    if (it1 != brokerMap.end()) {
      slave = (it1->first != MASTER_ID);
      found = true;
      brokerAddr = it1->second;
    }
  }

  brokerTable.clear();
  if (found)
    return new FindBrokerResult(brokerAddr, slave);

  return NULL;
}

std::string MQClientInstance::findBrokerAddressInPublish(const std::string& brokerName) {
  BrokerAddrMAP brokerTable(getBrokerAddrMap());
  std::string brokerAddr;
  bool found = false;

  if (brokerTable.find(brokerName) != brokerTable.end()) {
    const auto& brokerMap = brokerTable[brokerName];
    auto it = brokerMap.find(MASTER_ID);
    if (it != brokerMap.end()) {
      brokerAddr = it->second;
      found = true;
    }
  }

  brokerTable.clear();
  if (found) {
    return brokerAddr;
  }

  return null;
}

FindBrokerResult* MQClientInstance::findBrokerAddressInSubscribe(const std::string& brokerName,
                                                                 int brokerId,
                                                                 bool onlyThisBroker) {
  string brokerAddr;
  bool slave = false;
  bool found = false;
  BrokerAddrMAP brokerTable(getBrokerAddrMap());

  if (brokerTable.find(brokerName) != brokerTable.end()) {
    map<int, string> brokerMap(brokerTable[brokerName]);
    if (!brokerMap.empty()) {
      auto iter = brokerMap.find(brokerId);
      if (iter != brokerMap.end()) {
        brokerAddr = iter->second;
        slave = (brokerId != MASTER_ID);
        found = true;
      } else if (!onlyThisBroker) {  // not only from master
        iter = brokerMap.begin();
        brokerAddr = iter->second;
        slave = iter->first != MASTER_ID;
        found = true;
      }
    }
  }

  brokerTable.clear();

  if (found) {
    return new FindBrokerResult(brokerAddr, slave);
  }

  return nullptr;
}

void MQClientInstance::findConsumerIds(const std::string& topic, const std::string& group, std::vector<string>& cids) {
  std::string brokerAddr = findBrokerAddrByTopic(topic);
  if (brokerAddr.empty()) {
    updateTopicRouteInfoFromNameServer(topic);
    brokerAddr = findBrokerAddrByTopic(topic);
  }

  if (!brokerAddr.empty()) {
    try {
      LOG_INFO("getConsumerIdList from broker:%s", brokerAddr.c_str());
      return m_clientAPIImpl->getConsumerIdListByGroup(brokerAddr, group, cids, 5000);
    } catch (MQException& e) {
      LOG_ERROR(e.what());
    }
  }
}

std::string MQClientInstance::findBrokerAddrByTopic(const std::string& topic) {
  auto topicRouteData = getTopicRouteData(topic);
  if (topicRouteData != nullptr) {
    return topicRouteData->selectBrokerAddr();
  }
  return "";
}

void MQClientInstance::resetOffset(const std::string& group,
                                   const std::string& topic,
                                   const map<MQMessageQueue, int64_t>& offsetTable) {
  DefaultMQPushConsumer* consumer = nullptr;
  try {
    auto* impl = selectConsumer(group);
    if (impl != nullptr && std::type_index(typeid(*impl)) == std::type_index(typeid(DefaultMQPushConsumer))) {
      consumer = static_cast<DefaultMQPushConsumer*>(impl);
    } else {
      LOG_INFO("[reset-offset] consumer dose not exist. group=%s", group.c_str());
      return;
    }
    consumer->suspend();

    auto processQueueTable = consumer->getRebalanceImpl()->getProcessQueueTable();
    for (const auto& it : processQueueTable) {
      const auto& mq = it.first;
      if (topic == mq.getTopic() && offsetTable.find(mq) != offsetTable.end()) {
        auto pq = it.second;
        pq->setDropped(true);
        pq->clearAllMsgs();
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));

    for (const auto& it : processQueueTable) {
      const auto& mq = it.first;
      auto it2 = offsetTable.find(mq);
      if (it2 != offsetTable.end()) {
        auto offset = it2->second;
        consumer->updateConsumeOffset(mq, offset);
        consumer->getRebalanceImpl()->removeUnnecessaryMessageQueue(mq, it.second);
        consumer->getRebalanceImpl()->removeProcessQueueDirectly(mq);
      }
    }
  } catch (...) {
    if (consumer != nullptr) {
      consumer->resume();
    }
    throw;
  }
  if (consumer != nullptr) {
    consumer->resume();
  }
}

ConsumerRunningInfo* MQClientInstance::consumerRunningInfo(const std::string& consumerGroup) {
  MQConsumer* consumer = selectConsumer(consumerGroup);
  if (consumer != nullptr) {
    std::unique_ptr<ConsumerRunningInfo> runningInfo(consumer->consumerRunningInfo());
    if (runningInfo != nullptr) {
      auto nsList = m_clientAPIImpl->getRemotingClient()->getNameServerAddressList();

      std::string nsAddr;
      for (const auto& addr : nsList) {
        nsAddr.append(addr);
      }

      runningInfo->setProperty(ConsumerRunningInfo::PROP_NAMESERVER_ADDR, nsAddr);
      if (consumer->consumeType() == CONSUME_PASSIVELY) {
        runningInfo->setProperty(ConsumerRunningInfo::PROP_CONSUME_TYPE, "CONSUME_PASSIVELY");
      } else {
        runningInfo->setProperty(ConsumerRunningInfo::PROP_CONSUME_TYPE, "CONSUME_ACTIVELY");
      }
      runningInfo->setProperty(ConsumerRunningInfo::PROP_CLIENT_VERSION, "V3_1_8");  // MQVersion::s_CurrentVersion ));

      return runningInfo.release();
    }
  }

  LOG_ERROR("no corresponding consumer found for group:%s", consumerGroup.c_str());
  return nullptr;
}

void MQClientInstance::addBrokerToAddrMap(const std::string& brokerName,
                                          const std::map<int, std::string>& brokerAddrs) {
  std::lock_guard<std::mutex> lock(m_brokerAddrlock);
  if (m_brokerAddrTable.find(brokerName) != m_brokerAddrTable.end()) {
    m_brokerAddrTable.erase(brokerName);
  }
  m_brokerAddrTable.emplace(brokerName, brokerAddrs);
}

void MQClientInstance::clearBrokerAddrMap() {
  std::lock_guard<std::mutex> lock(m_brokerAddrlock);
  m_brokerAddrTable.clear();
}

MQClientInstance::BrokerAddrMAP MQClientInstance::getBrokerAddrMap() {
  std::lock_guard<std::mutex> lock(m_brokerAddrlock);
  return m_brokerAddrTable;
}

}  // namespace rocketmq