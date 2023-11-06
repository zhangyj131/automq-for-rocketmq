package com.automq.rocketmq.containers;

import com.automq.rocketmq.test.factory.LocalStackFactory;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;

public class LocalStackTest {

    private static final Logger log = LoggerFactory.getLogger(LocalStackTest.class);

    @Rule
    private static final LocalStackContainer LOCALSTACK;

    static {
        LOCALSTACK = LocalStackFactory.getInstance();
    }

    @BeforeAll
    public static void setup() {
        LOCALSTACK.start();
    }

    @AfterAll
    public static void shutdown() {
        LOCALSTACK.stop();
    }

    @Test
    public void printInformation() {
        log.info(LOCALSTACK.getContainerId());
        log.info(LOCALSTACK.getContainerName());
        log.info(String.valueOf(LOCALSTACK.getEndpoint()));
        log.info(LOCALSTACK.getAccessKey());
        log.info(LOCALSTACK.getSecretKey());
        log.info(LOCALSTACK.getRegion());
    }

    @Test
    public void createBucketTest() {
        LocalStackFactory.createBucket();
    }
}
