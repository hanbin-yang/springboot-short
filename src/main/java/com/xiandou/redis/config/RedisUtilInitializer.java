package com.xiandou.redis.config;

import com.xiandou.redis.core.DistributedLock;
import com.xiandou.utils.lock.RedisLockUtil;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

/**
 * RedisUtil 初始化配置。
 * 在 Spring 容器启动后注入 DistributedLock 到 RedisUtil 静态外观类。
 */
@Configuration
public class RedisUtilInitializer {

    private static final Logger logger = LoggerFactory.getLogger(RedisUtilInitializer.class);

    private final DistributedLock distributedLock;

    public RedisUtilInitializer(DistributedLock distributedLock) {
        this.distributedLock = distributedLock;
    }

    @PostConstruct
    public void init() {
        RedisLockUtil.init(distributedLock);
        logger.info("RedisLockUtil 已初始化，distributedLock={}", distributedLock);
    }
}
