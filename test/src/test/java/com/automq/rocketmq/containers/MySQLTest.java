package com.automq.rocketmq.containers;

import com.automq.rocketmq.test.factory.MySQLFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;

public class MySQLTest {
    private static final Logger log = LoggerFactory.getLogger(MySQLTest.class);
    private static final MySQLContainer MYSQL;

    static {
        MYSQL = MySQLFactory.getInstance();
    }

    @BeforeAll
    public static void setup() {
        MYSQL.start();
    }

    @AfterAll
    public static void shutdown() {
        MYSQL.stop();
    }

    @Test
    public void printInformation() {
        log.info(MYSQL.getUsername());
        log.info(MYSQL.getPassword());
        log.info(MYSQL.getJdbcUrl());
        log.info(MYSQL.getDatabaseName());
    }
}
