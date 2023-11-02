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

package com.automq.rocketmq;

import com.automq.rocketmq.broker.BrokerController;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.test.factory.BrokerConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppTest.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting broker...");
        long start = System.currentTimeMillis();

        BrokerConfig brokerConfig = BrokerConfigFactory.fromYml("/Users/ether/project/rocketmq-on-s3/test/src/main/resources/broker.yaml");
        brokerConfig.initEnvVar();
        brokerConfig.validate();

        BrokerController controller = new BrokerController(brokerConfig);

        // Start the broker
        controller.start();
        LOGGER.info("Broker started, costs {} ms", System.currentTimeMillis() - start);
        controller.shutdown();
    }
}
