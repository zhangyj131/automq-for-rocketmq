package com.automq.rocketmq.test.factory;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class MySQLFactory {
    private static final DockerImageName MYSQL_IMAGE;
    private static final MySQLContainer MYSQL;

    static  {
        MYSQL_IMAGE = DockerImageName.parse("mysql:8.0.32");
        MYSQL = new MySQLContainer(MYSQL_IMAGE);
    }

    /**
     * username:    test
     * <br>
     * password:    test
     * <br>
     * url:         jdbc:mysql://localhost:xxxx/test
     * <br>
     * database:    test
     * <br>
     * @return MySQLContainer
     */
    public static MySQLContainer getInstance() {
        return MYSQL;
    }
}
