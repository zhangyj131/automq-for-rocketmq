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

import com.automq.rocketmq.test.factory.BrokerFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiBrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(MultiBrokerTest.class);

    private List<BrokerController> brokerList;

    private Map<String, BrokerController> brokerMap;

    private final int brokerNum = 3;

    @BeforeEach
    public void setUp() {
        Map<String, BrokerController> map = BrokerFactory.batchBroker(brokerNum);
        brokerList = new LinkedList<>();
        map.forEach((k, v) -> brokerList.add(v));
    }

    @AfterEach
    public void tearDown() {
        brokerList.forEach(broker -> {
            try {
                broker.shutdown();
            } catch (Exception ignored) {

            }
        });
    }

    @Test
    public void startUpTest() {
        brokerList.forEach(controller -> {
            try {
                controller.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
