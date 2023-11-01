package com.automq.rocketmq;

import com.automq.rocketmq.broker.BrokerController;
import com.automq.rocketmq.factory.BrokerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private static final BrokerController controller;

    static {
        controller = BrokerFactory.getBroker();
    }

    @Test
    public void runController() {
        try {
            controller.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void tearDown() {
        try {
            controller.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
