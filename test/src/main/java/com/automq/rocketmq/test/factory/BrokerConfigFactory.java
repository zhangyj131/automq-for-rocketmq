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

package com.automq.rocketmq.test.factory;

import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.config.DatabaseConfig;
import com.automq.rocketmq.common.config.MetricsConfig;
import com.automq.rocketmq.common.config.S3StreamConfig;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.exception.RocketMQException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

public class BrokerConfigFactory {

    /**
     * 产生一个Broker的BrokerConfig，详细参数见方法体
     * <br>
     * - id:    test-broker-{uuid}
     * <br>
     * - name:  test-broker-{uuid}
     * <p></p>
     * 本地数据库默认配置为：
     * <br>
     * - url:       jdbc:mysql://localhost:3306/metadata
     * <br>
     * - username:  root
     * <br>
     * - password:  root
     *
     * @return BrokerConfig
     */
    public static BrokerConfig defaultConfig() {
        BrokerConfig brokerConfig = new BrokerConfig();
        UUID uuid = UUID.randomUUID();

        brokerConfig.setName(String.format("test-broker-%s", uuid));
        brokerConfig.setInstanceId(String.format("test-broker-%s", uuid));
        brokerConfig.setBindAddress("0.0.0.0:8081");
        brokerConfig.setInnerAccessKey("");
        brokerConfig.setInnerSecretKey("");

        MetricsConfig metricsConfig = brokerConfig.metrics();
        metricsConfig.setExporterType("OTLP_GRPC");
        metricsConfig.setGrpcExporterTarget("http://10.129.63.127:4317");
        metricsConfig.setGrpcExporterHeader("");
        metricsConfig.setGrpcExporterTimeOutInMills(31000);
        metricsConfig.setPeriodicExporterIntervalInMills(30000);
        metricsConfig.setPromExporterPort(5557);
        metricsConfig.setPromExporterHost("localhost");
        metricsConfig.setLabels("");
        metricsConfig.setExportInDelta(false);

        S3StreamConfig s3StreamConfig = brokerConfig.s3Stream();
        s3StreamConfig.setS3WALPath("/tmp/s3rocketmq/wal");
        s3StreamConfig.setS3Endpoint("http://minio.hellocorp.test");
        s3StreamConfig.setS3Bucket("lzh");
        s3StreamConfig.setS3Region("us-east-1");
        s3StreamConfig.setS3ForcePathStyle(true);
        s3StreamConfig.setS3AccessKey("kicaSWtNBPf7XCCsdW8Z");
        s3StreamConfig.setS3SecretKey("dgWxasHBEQBBXxAxBpFmp4VWIoQ0XyDU3gGdFnaT");

        DatabaseConfig databaseConfig = brokerConfig.db();
        databaseConfig.setUrl("jdbc:mysql://localhost:3306/metadata");
        databaseConfig.setUserName("root");
        databaseConfig.setPassword("root");

        brokerConfig.initEnvVar();
        try {
            brokerConfig.validate();
        } catch (RocketMQException e) {
            throw new RuntimeException(e);
        }

        return brokerConfig;
    }

    /**
     * 除数据库配置外其他都默认
     *
     * @param dbUrl      数据库链接
     * @param dbUserName 数据库用户
     * @param dbPassword 数据库用户密码
     * @return BrokerConfig
     */
    public static BrokerConfig defaultConfig(String dbUrl, String dbUserName, String dbPassword) {
        BrokerConfig brokerConfig = defaultConfig();
        DatabaseConfig databaseConfig = brokerConfig.db();

        databaseConfig.setUrl(dbUrl);
        databaseConfig.setUserName(dbUserName);
        databaseConfig.setPassword(dbPassword);

        return brokerConfig;
    }

    /**
     * 从yml文件中读取配置，yml中只包含单个 Broker 的配置
     *
     * @param ymlPath yml文件路径
     * @return BrokerConfig
     */
    public static BrokerConfig fromYml(String ymlPath) {
        try {
            String content = Files.readString(Path.of(ymlPath), StandardCharsets.UTF_8);
            Yaml yaml = new Yaml();
            yaml.setBeanAccess(BeanAccess.FIELD);
            return yaml.loadAs(content, BrokerConfig.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量生成 BrokerConfig 的一个
     *
     * @param idx       index，从0开始
     * @param uuid      每个批次需要传入一个固定的uuid
     * @param portStart 开始的端口，portStart + idx 是该 config 的端口
     * @return BrokerConfig
     */
    public static BrokerConfig indexedConfig(int idx, String uuid, int portStart) {
        BrokerConfig brokerConfig = new BrokerConfig();

        brokerConfig.setName(String.format("test-broker-%s-%d", uuid, idx));
        brokerConfig.setInstanceId(String.format("test-broker-%s-%d", uuid, idx));
        brokerConfig.setBindAddress("0.0.0.0:" + (portStart + idx));
        brokerConfig.setInnerAccessKey("");
        brokerConfig.setInnerSecretKey("");

        MetricsConfig metricsConfig = brokerConfig.metrics();
        metricsConfig.setExporterType("OTLP_GRPC");
        metricsConfig.setGrpcExporterTarget("http://10.129.63.127:4317");
        metricsConfig.setGrpcExporterHeader("");
        metricsConfig.setGrpcExporterTimeOutInMills(31000);
        metricsConfig.setPeriodicExporterIntervalInMills(30000);
        metricsConfig.setPromExporterPort(5557);
        metricsConfig.setPromExporterHost("localhost");
        metricsConfig.setLabels("");
        metricsConfig.setExportInDelta(false);

        S3StreamConfig s3StreamConfig = brokerConfig.s3Stream();
        s3StreamConfig.setS3WALPath("/tmp/s3rocketmq/%s/%s/wal".formatted(uuid, idx));
        s3StreamConfig.setS3Endpoint("http://minio.hellocorp.test");
        s3StreamConfig.setS3Bucket("lzh");
        s3StreamConfig.setS3Region("us-east-1");
        s3StreamConfig.setS3ForcePathStyle(true);
        s3StreamConfig.setS3AccessKey("kicaSWtNBPf7XCCsdW8Z");
        s3StreamConfig.setS3SecretKey("dgWxasHBEQBBXxAxBpFmp4VWIoQ0XyDU3gGdFnaT");

        DatabaseConfig databaseConfig = brokerConfig.db();
        databaseConfig.setUrl("jdbc:mysql://localhost:3306/metadata");
        databaseConfig.setUserName("root");
        databaseConfig.setPassword("root");

        StoreConfig storeConfig = brokerConfig.store();
        storeConfig.setKvPath("/tmp/rocketmq/%s/%s/kvstore".formatted(uuid, idx));

        brokerConfig.initEnvVar();
        try {
            brokerConfig.validate();
        } catch (RocketMQException e) {
            throw new RuntimeException(e);
        }

        return brokerConfig;
    }

    /**
     * 批量生成 BrokerConfig 的一个，默认开始的端口为8081
     *
     * @param idx  index，从0开始
     * @param uuid 每个批次需要传入一个固定的uuid
     * @return BrokerConfig
     */
    public static BrokerConfig indexedConfig(int idx, String uuid) {
        return indexedConfig(idx, uuid, 8081);
    }
}
