package com.automq.rocketmq.factory;

import com.automq.rocketmq.broker.BrokerController;
import com.automq.rocketmq.common.config.BrokerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class BrokerFactory {

    /**
     * 创建一个默认的 Broker，参数来源于
     *
     * @return BrokerController
     */
    public static BrokerController getBroker() {
        BrokerConfig brokerConfig = BrokerConfigFactory.defaultConfig();
        return getBroker(brokerConfig);
    }

    /**
     * 根据 BrokerConfig 创建一个 Broker
     *
     * @param brokerConfig BrokerConfig
     * @return BrokerController
     */
    public static BrokerController getBroker(BrokerConfig brokerConfig) {
        try {
            return new BrokerController(brokerConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量生成 Broker
     *
     * @param n Broker 的数量
     * @return Map<String, BrokerController>，key是InstanceID(name 与其一致)
     */
    public static Map<String, BrokerController> batchBroker(int n) {
        Map<String, BrokerController> map = new HashMap<>();

        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < n; i++) {
            BrokerConfig brokerConfig = BrokerConfigFactory.indexedConfig(i, uuid);
            map.put(brokerConfig.instanceId(), getBroker(brokerConfig));
        }

        return map;
    }
}
