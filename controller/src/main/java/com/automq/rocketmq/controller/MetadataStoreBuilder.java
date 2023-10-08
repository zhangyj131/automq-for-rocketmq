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

package com.automq.rocketmq.controller;

import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class MetadataStoreBuilder {
    public static ControllerServiceImpl build(MetadataStore metadataStore) {
        return new ControllerServiceImpl(metadataStore);
    }
    public static MetadataStore build(BrokerConfig config) throws IOException {
        SqlSessionFactory sessionFactory = getSessionFactory(config.controller().dbUrl(), config.controller().dbUser(), config.controller().dbPassword());
        Node node;

        try (SqlSession session = sessionFactory.openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            node = nodeMapper.get(config.nodeId(), config.name(), config.instanceId(), config.volumeId());

            if (Objects.isNull(node)) {
                node = new Node() {
                    {
                        setEpoch(config.epoch());
                        setName(config.name());
                        setInstanceId(config.instanceId());
                        setVolumeId(config.volumeId());
                        setHostName(config.hostName());
                        setVpcId(config.vpcId());
                        setAddress(config.address());
                    }
                };
                nodeMapper.create(node);
            }
        }

        return new DefaultMetadataStore(new GrpcControllerClient(), sessionFactory, config);
    }

    private static SqlSessionFactory getSessionFactory(String dbUrl, String dbUser, String dbPassword) throws IOException {
        String resource = "database/mybatis-config.xml";
        InputStream inputStream = Resources.getResourceAsStream(resource);

        Properties properties = new Properties();
        properties.put("userName", dbUser);
        properties.put("password", dbPassword);
        properties.put("jdbcUrl", dbUrl + "?TC_REUSABLE=true");
        return new SqlSessionFactoryBuilder().build(inputStream, properties);
    }
}
