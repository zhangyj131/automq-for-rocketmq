package com.automq.rocketmq;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UUIDTest {
    @Test
    public void uuidTest() {
        UUID uuid = UUID.randomUUID();
        Logger logger = LoggerFactory.getLogger(UUIDTest.class);
        logger.info(uuid.toString());
    }
}
