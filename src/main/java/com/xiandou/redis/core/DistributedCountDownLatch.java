package com.xiandou.redis.core;

import com.xiandou.redis.config.LuaScriptRegistry;
import com.xiandou.redis.constant.RedisKeyConstant;
import com.xiandou.redis.listener.PubSubSubscriber;
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
 * <p>本类不依赖任何 utils 包下的类，保持独立。</p>
 */
public class DistributedCountDownLatch {

    private final StringRedisTemplate redisTemplate;

    public DistributedCountDownLatch(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 尝试初始化计数（仅当 key 不存在时设置）。
     *
     * @param name  latch 名称
     * @param count 初始计数
     * @return true 设置成功（新创建），false key 已存在
     * @throws IllegalArgumentException 当 count 为负数时
     */
    public boolean trySetCount(String name, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
        Long result = redisTemplate.execute(
                LuaScriptRegistry.SET_IF_ABSENT_SCRIPT,
                List.of(RedisKeyConstant.latchKey(name)),
                String.valueOf(count)
        );
        return result != null && result == 1L;
    }

    /**
     * 递减计数。当计数归零时，通过 Pub/Sub 通知所有等待线程。
     * 如果计数已为 0 或负数，操作仍安全执行（幂等）。
     *
     * @param name latch 名称
     */
    public void countDown(String name) {
        String latchKey = RedisKeyConstant.latchKey(name);
        String channel = RedisKeyConstant.latchChannel(name);
        redisTemplate.execute(
                LuaScriptRegistry.LATCH_COUNTDOWN_SCRIPT,
                List.of(latchKey, channel)
        );
    }

    /**
     * 阻塞等待直到计数归零。
     * <p>如果 key 不存在（未初始化），视为已归零并立即返回。</p>
     *
     * @param name latch 名称
     * @throws InterruptedException 等待时被中断
     */
    public void await(String name) throws InterruptedException {
        if (isCountZeroOrNotExists(name)) {
            return;
        }
        String channel = RedisKeyConstant.latchChannel(name);
        try (PubSubSubscriber subscriber = new PubSubSubscriber(
                redisTemplate.getConnectionFactory(), channel)) {
            // 再次检查，防止两次检查间 countDown 到 0
            if (isCountZeroOrNotExists(name)) {
                return;
            }
            subscriber.await(Long.MAX_VALUE);
        }
    }

    /**
     * 带超时的阻塞等待。
     * <p>如果 key 不存在（未初始化），视为已归零并立即返回 true。</p>
     *
     * @param name    latch 名称
     * @param timeout 最大等待时间
     * @param unit    时间单位
     * @return true 在超时前计数归零，false 超时未归零
     * @throws InterruptedException 等待时被中断
     */
    public boolean await(String name, long timeout, TimeUnit unit) throws InterruptedException {
        if (isCountZeroOrNotExists(name)) {
            return true;
        }
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        String channel = RedisKeyConstant.latchChannel(name);
        try (PubSubSubscriber subscriber = new PubSubSubscriber(
                redisTemplate.getConnectionFactory(), channel)) {
            // 再次检查，防止两次检查间 countDown 到 0
            if (isCountZeroOrNotExists(name)) {
                return true;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return false;
            }
            subscriber.await(remaining);
            // 被唤醒后再次检查计数
            return isCountZeroOrNotExists(name);
        }
    }

    /**
     * 获取当前计数。
     * <p>如果 key 不存在，返回 0。</p>
     *
     * @param name latch 名称
     * @return 当前计数
     */
    public long getCount(String name) {
        String val = redisTemplate.opsForValue().get(RedisKeyConstant.latchKey(name));
        return val == null ? 0L : Long.parseLong(val);
    }

    /**
     * 检查计数是否已归零或 key 不存在。
     * <p>key 不存在视为已归零，避免未初始化时永久阻塞。</p>
     */
    private boolean isCountZeroOrNotExists(String name) {
        String val = redisTemplate.opsForValue().get(RedisKeyConstant.latchKey(name));
        return val == null || Long.parseLong(val) <= 0;
    }
}
