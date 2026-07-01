package com.xiandou.redis.core;

import com.xiandou.redis.config.LuaScriptRegistry;
import com.xiandou.redis.constant.RedisKeyConstant;
import com.xiandou.redis.listener.SharedPubSubSubscriber;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的分布式 CountDownLatch。
 *
 * <p>参考 RedissonCountDownLatch 设计，使用 Redis STRING 类型存储计数，
 * 并通过 Pub/Sub 实现线程间通知。核心 API 允许初始化计数、递减计数
 * 以及等待计数归零。</p>
 *
 * <p>Redis Key 默认 7 天过期，可通过构造器 {@link #DistributedCountDownLatch(StringRedisTemplate, long)} 调整。</p>
 */
public class DistributedCountDownLatch {

    /** 默认过期时间：7 天 */
    public static final long DEFAULT_TTL_SECONDS = 604800L;

    private final StringRedisTemplate redisTemplate;
    private final SharedPubSubSubscriber pubSubSubscriber;
    private final long defaultTtlSeconds;

    public DistributedCountDownLatch(StringRedisTemplate redisTemplate) {
        this(redisTemplate, DEFAULT_TTL_SECONDS);
    }

    public DistributedCountDownLatch(StringRedisTemplate redisTemplate, long defaultTtlSeconds) {
        this(redisTemplate, defaultTtlSeconds, new SharedPubSubSubscriber(redisTemplate.getConnectionFactory()));
    }

    public DistributedCountDownLatch(StringRedisTemplate redisTemplate, long defaultTtlSeconds,
                                      SharedPubSubSubscriber pubSubSubscriber) {
        this.redisTemplate = redisTemplate;
        this.defaultTtlSeconds = defaultTtlSeconds > 0 ? defaultTtlSeconds : DEFAULT_TTL_SECONDS;
        this.pubSubSubscriber = pubSubSubscriber;
    }

    // ==================== 初始化 ====================

    /**
     * 尝试初始化计数（仅当 key 不存在时设置），并设置过期时间。
     */
    public boolean trySetCount(String name, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
        Long result = redisTemplate.execute(
                LuaScriptRegistry.SET_IF_ABSENT_SCRIPT,
                List.of(RedisKeyConstant.latchKey(name)),
                String.valueOf(count), String.valueOf(defaultTtlSeconds)
        );
        return result != null && result == 1L;
    }

    // ==================== 递减 ====================

    /**
     * 递减计数。当计数归零时，通过 Pub/Sub 通知所有等待线程。
     * 如果 Key 存在，每次递减均刷新过期时间。
     */
    public void countDown(String name) {
        String latchKey = RedisKeyConstant.latchKey(name);
        String channel = RedisKeyConstant.latchChannel(name);
        redisTemplate.execute(
                LuaScriptRegistry.LATCH_COUNTDOWN_SCRIPT,
                List.of(latchKey, channel),
                String.valueOf(defaultTtlSeconds)
        );
    }

    // ==================== 等待 ====================

    /**
     * 阻塞等待直到计数归零。
     */
    public void await(String name) throws InterruptedException {
        if (isCountZeroOrNotExists(name)) {
            return;
        }
        String channel = RedisKeyConstant.latchChannel(name);
        // 轮询+Pub/Sub 通知：等待时最长 1 秒超时，过后重新检查计数
        // Pub/Sub 通知可提前唤醒，通知丢失也不影响正确性
        while (!isCountZeroOrNotExists(name)) {
            pubSubSubscriber.await(channel, 1000);
        }
    }

    /**
     * 带超时的阻塞等待。
     * <p>采用轮询 + Pub/Sub（最多 1 秒间隔），即使 Pub/Sub 通知丢失也能快速恢复。</p>
     */
    public boolean await(String name, long timeout, TimeUnit unit) throws InterruptedException {
        if (isCountZeroOrNotExists(name)) {
            return true;
        }
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        String channel = RedisKeyConstant.latchChannel(name);
        while (System.currentTimeMillis() < deadline) {
            if (isCountZeroOrNotExists(name)) {
                return true;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return false;
            }
            // 每段最多等 1 秒，超时后重新检查计数
            pubSubSubscriber.await(channel, Math.min(remaining, 1000));
        }
        return isCountZeroOrNotExists(name);
    }

    // ==================== 查询 ====================

    /**
     * 获取当前计数。如果 Key 不存在（已过期），返回 0。
     */
    public long getCount(String name) {
        String val = redisTemplate.opsForValue().get(RedisKeyConstant.latchKey(name));
        return val == null ? 0L : Long.parseLong(val);
    }

    private boolean isCountZeroOrNotExists(String name) {
        String val = redisTemplate.opsForValue().get(RedisKeyConstant.latchKey(name));
        return val == null || Long.parseLong(val) <= 0;
    }
}
