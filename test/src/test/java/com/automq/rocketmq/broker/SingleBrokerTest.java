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

package com.automq.rocketmq.broker;

import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.exception.RocketMQException;
import com.automq.rocketmq.test.factory.BrokerConfigFactory;
import com.automq.rocketmq.test.factory.BrokerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.rocketmq.broker.BrokerStartup.buildShutdownHook;

public class SingleBrokerTest {
    private BrokerController broker;
    private Logger logger = LoggerFactory.getLogger(SingleBrokerTest.class);

    @BeforeEach
    public void setUp() throws RocketMQException {
        String ymlPath = "/Users/ether/project/rocketmq-on-s3/test/src/main/resources/broker.yaml";
        BrokerConfig brokerConfig = BrokerConfigFactory.fromYml(ymlPath);
        brokerConfig.initEnvVar();
        brokerConfig.validate();
        broker = BrokerFactory.getBroker(brokerConfig);
    }

    @AfterEach
    public void tearDown() throws Exception {
        broker.shutdown();
    }

    @Test
    public void brokerStartUpTest() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(broker), "ShutdownHook"));
        broker.start();
    }
}
