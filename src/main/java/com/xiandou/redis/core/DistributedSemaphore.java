package com.xiandou.redis.core;

import com.xiandou.redis.config.LuaScriptRegistry;
import com.xiandou.redis.constant.RedisKeyConstant;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的分布式信号量。
 *
 * <p>核心 API 允许获取和释放指定数量的许可，使用 Lua 脚本保证原子性。
 * 使用 trySetPermits 进行初始化（仅当 key 不存在时设置），避免重复覆盖。</p>
 *
 * <p>本类不依赖任何 utils 包下的类，保持独立。</p>
 */
public class DistributedSemaphore {

    private final StringRedisTemplate redisTemplate;

    public DistributedSemaphore(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 尝试获取指定数量的许可，立即返回。
     *
     * @param name    信号量名称
     * @param permits 需要的许可数量
     * @return true 获取成功，false 许可不足
     */
    public boolean tryAcquire(String name, int permits) {
        if (permits <= 0) {
            return true;
        }
        Long result = redisTemplate.execute(
                LuaScriptRegistry.SEMAPHORE_ACQUIRE_SCRIPT,
                List.of(RedisKeyConstant.semaphoreKey(name)),
                String.valueOf(permits)
        );
        return result != null && result == 1L;
    }

    /**
     * 尝试获取指定数量的许可，带超时等待。
     * <p>在超时时间内轮询重试，每隔 50ms 尝试一次。</p>
     *
     * @param name    信号量名称
     * @param permits 需要的许可数量
     * @param timeout 最大等待时间
     * @param unit    时间单位
     * @return true 在超时前获取成功，false 超时未获取到
     * @throws InterruptedException 等待时被中断
     */
    public boolean tryAcquire(String name, int permits, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (permits <= 0) {
            return true;
        }
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        // 先尝试一次，避免不必要的等待
        if (tryAcquire(name, permits)) {
            return true;
        }
        long remaining = deadline - System.currentTimeMillis();
        while (remaining > 0) {
            Thread.sleep(Math.min(50, remaining));
            if (tryAcquire(name, permits)) {
                return true;
            }
            remaining = deadline - System.currentTimeMillis();
        }
        return false;
    }

    /**
     * 阻塞直到获取指定数量的许可。
     * <p>通过轮询方式持续尝试，适用于能保证最终可获取的场景。</p>
     *
     * @param name    信号量名称
     * @param permits 需要的许可数量
     * @throws InterruptedException 等待时被中断
     */
    public void acquire(String name, int permits) throws InterruptedException {
        if (permits <= 0) {
            return;
        }
        while (!tryAcquire(name, permits)) {
            Thread.sleep(50);
        }
    }

    /**
     * 释放指定数量的许可。
     *
     * @param name    信号量名称
     * @param permits 要释放的许可数量
     */
    public void release(String name, int permits) {
        if (permits <= 0) {
            return;
        }
        redisTemplate.execute(
                LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT,
                List.of(RedisKeyConstant.semaphoreKey(name)),
                RedisKeyConstant.semaphoreChannel(name),
                String.valueOf(permits)
        );
    }

    /**
     * 初始化信号量许可数（仅当 key 不存在时设置）。
     *
     * @param name    信号量名称
     * @param permits 许可数量
     * @return true 设置成功（新创建），false key 已存在
     * @throws IllegalArgumentException 当 permits 为负数时
     */
    public boolean trySetPermits(String name, int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("permits must not be negative");
        }
        Long result = redisTemplate.execute(
                LuaScriptRegistry.SET_IF_ABSENT_SCRIPT,
                List.of(RedisKeyConstant.semaphoreKey(name)),
                String.valueOf(permits)
        );
        return result != null && result == 1L;
    }

    /**
     * 获取当前可用许可数。
     *
     * @param name 信号量名称
     * @return 可用许可数，如果不存在则返回 0
     */
    public long availablePermits(String name) {
        String val = redisTemplate.opsForValue().get(RedisKeyConstant.semaphoreKey(name));
        return val == null ? 0L : Long.parseLong(val);
    }
}
