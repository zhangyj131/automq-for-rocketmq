package com.automq.rocketmq.test.conf;

import com.automq.rocketmq.test.factory.LocalStackFactory;
import com.automq.rocketmq.test.factory.MySQLFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;

public class BasicConfig {
    protected static final LocalStackContainer LOCALSTACK;
    protected static final MySQLContainer MYSQL;
    protected static final String BUCKET_NAME;

    static {
        LOCALSTACK = LocalStackFactory.getInstance();
        MYSQL = MySQLFactory.getInstance();
        LOCALSTACK.start();
        MYSQL.start();
        LocalStackFactory.createBucket();
        BUCKET_NAME = LocalStackFactory.getBucketName();
    }
}
