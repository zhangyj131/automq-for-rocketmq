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
