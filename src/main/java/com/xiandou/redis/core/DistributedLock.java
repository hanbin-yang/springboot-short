package com.xiandou.redis.core;

import com.xiandou.redis.config.LuaScriptRegistry;
import com.xiandou.redis.constant.RedisKeyConstant;
import com.xiandou.redis.listener.PubSubSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 RedisTemplate 的可重入分布式锁。
 * 参考 RedissonLock 源码设计：
 * - 可重入：Redis Hash 结构存储 {UUID:threadId → 重入计数}
 * - Watchdog 自动续期：默认 30s TTL，每 10s 续期一次
 * - Pub/Sub 等待通知：锁释放时发布消息，等待线程收到后重新竞争
 * - 集群兼容：所有 Key 包含 {tag} 哈希标签
 *
 * 本类不依赖 RedisUtil，保持独立。
 */
public class DistributedLock {
    private static final Logger log = LoggerFactory.getLogger(DistributedLock.class);

    /** 默认锁过期时间 */
    public static final long DEFAULT_LEASE_TIME_MS = 30000L;
    /** 默认 Watchdog 续期间隔 = leaseTime / 3 */
    private static final long WATCHDOG_INTERVAL_MS = DEFAULT_LEASE_TIME_MS / 3;

    private final StringRedisTemplate redisTemplate;
    private final String clientId = UUID.randomUUID().toString();

    /** Watchdog 定时任务条目：lockKey → RenewalEntry */
    private final ConcurrentHashMap<String, RenewalEntry> renewalMap = new ConcurrentHashMap<>();

    /** Watchdog 线程池 */
    private final ScheduledExecutorService watchdogExecutor;

    public DistributedLock(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.watchdogExecutor = Executors.newScheduledThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                r -> {
                    Thread t = new Thread(r, "redis-lock-watchdog");
                    t.setDaemon(true);
                    return t;
                });
    }

    // ==================== 公开 API ====================

    /**
     * 尝试获取锁，不等待。
     * @param name 锁名称
     * @param leaseTime 锁过期时间（-1 使用默认 30s + Watchdog）
     * @param unit 时间单位
     * @return true 获取成功
     */
    public boolean tryLock(String name, long leaseTime, TimeUnit unit) {
        return tryLock(name, 0, leaseTime, unit);
    }

    /**
     * 尝试获取锁，可等待。
     * @param name 锁名称
     * @param waitTime 最大等待时间（0=不等待）
     * @param leaseTime 锁过期时间（-1 使用默认 30s + Watchdog）
     * @param unit 时间单位
     * @return true 获取成功
     */
    public boolean tryLock(String name, long waitTime, long leaseTime, TimeUnit unit) {
        long leaseMs = leaseTime > 0 ? unit.toMillis(leaseTime) : DEFAULT_LEASE_TIME_MS;
        long waitMs = unit.toMillis(waitTime);
        long threadId = Thread.currentThread().getId();
        String lockKey = RedisKeyConstant.lockKey(name);
        String entryName = getEntryName(threadId);

        Long ttl = evalLock(lockKey, leaseMs, entryName);
        if (ttl == null) {
            // 加锁成功：默认 leaseTime 时启动 Watchdog
            if (leaseTime < 0) {
                scheduleRenewal(lockKey, entryName);
            }
            return true;
        }

        // 不等待则立即返回
        if (waitMs <= 0) {
            return false;
        }

        // 需要等待 → 订阅频道
        return waitForLock(name, lockKey, entryName, leaseMs, waitMs, leaseTime);
    }

    /**
     * 阻塞直到获取锁（自动 Watchdog 续期）。
     * @param name 锁名称
     */
    public void lock(String name) {
        lock(name, -1, TimeUnit.MILLISECONDS);
    }

    /**
     * 阻塞直到获取锁。
     * @param name 锁名称
     * @param leaseTime 锁过期时间（-1 使用默认 30s + Watchdog，>0 指定时间不续期）
     * @param unit 时间单位
     */
    public void lock(String name, long leaseTime, TimeUnit unit) {
        long leaseMs = leaseTime > 0 ? unit.toMillis(leaseTime) : DEFAULT_LEASE_TIME_MS;
        long threadId = Thread.currentThread().getId();
        String lockKey = RedisKeyConstant.lockKey(name);
        String entryName = getEntryName(threadId);

        while (true) {
            Long ttl = evalLock(lockKey, leaseMs, entryName);
            if (ttl == null) {
                if (leaseTime < 0) {
                    scheduleRenewal(lockKey, entryName);
                }
                return;
            }

            String channel = RedisKeyConstant.lockChannel(name);
            try (PubSubSubscriber subscriber = new PubSubSubscriber(
                    redisTemplate.getConnectionFactory(), channel)) {
                // 再次尝试
                ttl = evalLock(lockKey, leaseMs, entryName);
                if (ttl == null) {
                    if (leaseTime < 0) {
                        scheduleRenewal(lockKey, entryName);
                    }
                    return;
                }
                long waitMs = ttl > 0 ? ttl : DEFAULT_LEASE_TIME_MS;
                subscriber.await(waitMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for lock: " + name, e);
            }
        }
    }

    /**
     * 解锁。如果锁不属于当前线程，返回 false。
     * @param name 锁名称
     * @return true 解锁成功
     */
    public boolean unlock(String name) {
        long threadId = Thread.currentThread().getId();
        String lockKey = RedisKeyConstant.lockKey(name);
        String channel = RedisKeyConstant.lockChannel(name);
        String entryName = getEntryName(threadId);

        // 停止 Watchdog
        cancelRenewal(lockKey);

        Long result = redisTemplate.execute(
                LuaScriptRegistry.UNLOCK_SCRIPT,
                List.of(lockKey, channel),
                "0", String.valueOf(DEFAULT_LEASE_TIME_MS), entryName
        );
        if (result == null) {
            log.warn("解锁失败：锁不属于当前线程, key={}", lockKey);
            return false;
        }
        return true;
    }

    // ==================== 内部方法 ====================

    private String getEntryName(long threadId) {
        return clientId + ":" + threadId;
    }

    private Long evalLock(String lockKey, long leaseMs, String entryName) {
        try {
            return redisTemplate.execute(
                    LuaScriptRegistry.LOCK_SCRIPT,
                    List.of(lockKey),
                    String.valueOf(leaseMs), entryName
            );
        } catch (Exception e) {
            log.error("加锁异常: key={}", lockKey, e);
            return 0L;  // 异常时返回非 null 表示失败
        }
    }

    private boolean waitForLock(String name, String lockKey, String entryName,
                                 long leaseMs, long waitMs, long leaseTime) {
        String channel = RedisKeyConstant.lockChannel(name);
        long deadline = System.currentTimeMillis() + waitMs;

        try (PubSubSubscriber subscriber = new PubSubSubscriber(
                redisTemplate.getConnectionFactory(), channel)) {

            while (System.currentTimeMillis() < deadline) {
                Long ttl = evalLock(lockKey, leaseMs, entryName);
                if (ttl == null) {
                    if (leaseTime < 0) {
                        scheduleRenewal(lockKey, entryName);
                    }
                    return true;
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                long sleepMs = Math.min(ttl > 0 ? Math.min(ttl, remaining) : remaining, 1000);
                subscriber.await(sleepMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return false;
    }

    // ==================== Watchdog 看门狗 ====================

    private void scheduleRenewal(String lockKey, String entryName) {
        renewalMap.computeIfAbsent(lockKey, key -> {
            ScheduledFuture<?> future = watchdogExecutor.scheduleAtFixedRate(
                    () -> doRenew(key, entryName),
                    WATCHDOG_INTERVAL_MS, WATCHDOG_INTERVAL_MS, TimeUnit.MILLISECONDS
            );
            return new RenewalEntry(future, entryName);
        });
    }

    private void doRenew(String lockKey, String entryName) {
        try {
            Long result = redisTemplate.execute(
                    LuaScriptRegistry.RENEW_SCRIPT,
                    List.of(lockKey),
                    String.valueOf(DEFAULT_LEASE_TIME_MS), entryName
            );
            if (result == null || result == 0L) {
                log.warn("Watchdog 续期失败，锁已丢失: {}", lockKey);
                cancelRenewal(lockKey);
            }
        } catch (Exception e) {
            log.error("Watchdog 续期异常: {}", lockKey, e);
            cancelRenewal(lockKey);
        }
    }

    private void cancelRenewal(String lockKey) {
        RenewalEntry entry = renewalMap.remove(lockKey);
        if (entry != null && entry.future != null) {
            entry.future.cancel(false);
        }
    }

    // ==================== 内部类 ====================

    private static class RenewalEntry {
        final ScheduledFuture<?> future;
        final String entryName;
        RenewalEntry(ScheduledFuture<?> future, String entryName) {
            this.future = future;
            this.entryName = entryName;
        }
    }

    // ==================== 测试辅助方法 ====================

    public int getActiveWatchdogCount() {
        return renewalMap.size();
    }

    public String getClientId() {
        return clientId;
    }
}
