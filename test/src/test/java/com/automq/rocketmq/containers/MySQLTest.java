package com.automq.rocketmq.containers;

import com.automq.rocketmq.test.factory.MySQLFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;

public class MySQLTest {
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
    public void test() {
        String username = MYSQL.getUsername();
        String password = MYSQL.getPassword();
    }
}
