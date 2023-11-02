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

package com.automq.rocketmq.test.factory;

import com.automq.rocketmq.broker.BrokerController;
import com.automq.rocketmq.common.config.BrokerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class BrokerFactory {

    /**
     * 创建一个默认的 Broker，参数来源于
     *
     * @return BrokerController
     */
    public static BrokerController getBroker() {
        BrokerConfig brokerConfig = BrokerConfigFactory.defaultConfig();
        return getBroker(brokerConfig);
    }

    /**
     * 根据 BrokerConfig 创建一个 Broker
     *
     * @param brokerConfig BrokerConfig
     * @return BrokerController
     */
    public static BrokerController getBroker(BrokerConfig brokerConfig) {
        try {
            return new BrokerController(brokerConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量生成 Broker
     *
     * @param n Broker 的数量
     * @return Map<String, BrokerController>，key是InstanceID(name 与其一致)
     */
    public static Map<String, BrokerController> batchBroker(int n) {
        Map<String, BrokerController> map = new HashMap<>();

        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < n; i++) {
            BrokerConfig brokerConfig = BrokerConfigFactory.indexedConfig(i, uuid);
            map.put(brokerConfig.instanceId(), getBroker(brokerConfig));
        }

        return map;
    }
}
