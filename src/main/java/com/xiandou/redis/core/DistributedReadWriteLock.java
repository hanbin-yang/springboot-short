package com.xiandou.redis.core;

import com.xiandou.redis.constant.RedisKeyConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的分布式读写锁。
 *
 * <p>参考 RedissonReadWriteLock 设计，使用 Redis HASH 结构存储锁持有者信息。
 * 同一资源在同一时间只允许一个写锁或任意多个读锁（写-写互斥、写-读互斥、读-读共享）。</p>
 *
 * <p>Redis Key 使用哈希标签 {rwlock}: 以兼容集群部署。</p>
 *
 * <p>Hash 结构：</p>
 * <pre>
 * Key: "{rwlock}:resource"
 * Type: HASH
 * Value: {
 *     "mode": "read" | "write",
 *     "write_lock:UUID:threadId": 1,   // 写锁持有者 + 重入计数
 *     "read_lock:UUID:threadId": 3     // 读锁持有者 + 重入计数
 * }
 * </pre>
 *
 * <p>互斥规则：</p>
 * <ul>
 *   <li>无锁：读写都通过</li>
 *   <li>读锁（其他线程）：写锁阻塞，读锁共享</li>
 *   <li>读锁（当前线程）：写锁阻塞，读锁重入</li>
 *   <li>写锁（其他线程）：读写都阻塞</li>
 *   <li>写锁（当前线程）：写锁重入，读锁可降级</li>
 * </ul>
 *
 * <p>本类不依赖 RedisUtil、RedisLockResult、VoidSupplier，保持独立。</p>
 */
public class DistributedReadWriteLock {

    private static final Logger log = LoggerFactory.getLogger(DistributedReadWriteLock.class);

    /** 默认锁过期时间（毫秒） */
    private static final long DEFAULT_LEASE_TIME_MS = 30000L;

    // ==================== Lua 脚本 ====================

    /**
     * 写锁获取脚本。
     *
     * <p>KEYS[1] = rwlockKey(name)</p>
     * <p>ARGV[1] = leaseTime（毫秒）</p>
     * <p>ARGV[2] = 写锁 entryName（write_lock:clientId:threadId）</p>
     *
     * <p>返回 nil 表示成功，返回 TTL 表示被其他线程持有。</p>
     */
    private static final RedisScript<Long> WRITE_LOCK_SCRIPT;

    /**
     * 读锁获取脚本（支持锁降级）。
     *
     * <p>KEYS[1] = rwlockKey(name)</p>
     * <p>ARGV[1] = leaseTime（毫秒）</p>
     * <p>ARGV[2] = 读锁 entryName（read_lock:clientId:threadId）</p>
     * <p>ARGV[3] = 写锁 entryName（write_lock:clientId:threadId，用于降级检查）</p>
     *
     * <p>返回 nil 表示成功，返回 TTL 表示被其他线程持有。</p>
     */
    private static final RedisScript<Long> READ_LOCK_SCRIPT;

    /**
     * 写锁释放脚本。
     *
     * <p>KEYS[1] = rwlockKey(name)</p>
     * <p>ARGV[1] = leaseTime（毫秒）</p>
     * <p>ARGV[2] = 写锁 entryName</p>
     *
     * <p>返回 null 表示非法解锁（非持有者），0 表示仍有重入，1 表示完全释放。</p>
     */
    private static final RedisScript<Long> WRITE_UNLOCK_SCRIPT;

    /**
     * 读锁释放脚本。
     *
     * <p>KEYS[1] = rwlockKey(name)</p>
     * <p>ARGV[1] = leaseTime（毫秒）</p>
     * <p>ARGV[2] = 读锁 entryName</p>
     *
     * <p>返回 null 表示非法解锁（非持有者），0 表示仍有重入，1 表示完全释放。</p>
     */
    private static final RedisScript<Long> READ_UNLOCK_SCRIPT;

    static {
        WRITE_LOCK_SCRIPT = createScript(
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    "redis.call('hset', KEYS[1], 'mode', 'write'); " +
                    "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "return nil; " +
                "end; " +
                "if (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                    "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "return nil; " +
                "end; " +
                "return redis.call('pttl', KEYS[1]);",
                Long.class
        );

        // 读锁获取：在给定脚本基础上增加降级支持（ARGV[3] 写锁 entryName）
        READ_LOCK_SCRIPT = createScript(
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false or mode == 'read') then " +
                    "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                    "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "return nil; " +
                "end; " +
                "if (mode == 'write') then " +
                    "if (redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                    "end; " +
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                    "end; " +
                "end; " +
                "return redis.call('pttl', KEYS[1]);",
                Long.class
        );

        WRITE_UNLOCK_SCRIPT = createScript(
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then " +
                    "return nil; " +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +
                "if (counter > 0) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "return 0; " +
                "end; " +
                "redis.call('hdel', KEYS[1], ARGV[2]); " +
                "local hlen = redis.call('hlen', KEYS[1]); " +
                "if (hlen == 1) then " +
                    "redis.call('del', KEYS[1]); " +
                "else " +
                    "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "end; " +
                "return 1;",
                Long.class
        );

        READ_UNLOCK_SCRIPT = createScript(
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then " +
                    "return nil; " +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +
                "if (counter > 0) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "return 0; " +
                "end; " +
                "redis.call('hdel', KEYS[1], ARGV[2]); " +
                "local hlen = redis.call('hlen', KEYS[1]); " +
                "if (hlen == 1) then " +
                    "redis.call('del', KEYS[1]); " +
                "else " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "end; " +
                "return 1;",
                Long.class
        );
    }

    private static <T> RedisScript<T> createScript(String script, Class<T> resultType) {
        DefaultRedisScript<T> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(script);
        redisScript.setResultType(resultType);
        return redisScript;
    }

    // ==================== 实例字段 ====================

    private final StringRedisTemplate redisTemplate;
    private final String clientId = UUID.randomUUID().toString();

    // ==================== 构造方法 ====================

    public DistributedReadWriteLock(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // ==================== 写锁 API ====================

    /**
     * 尝试获取写锁，不等待。
     *
     * @param name      锁名称
     * @param leaseTime 锁过期时间
     * @param unit      时间单位
     * @return true 获取成功
     */
    public boolean writeLock(String name, long leaseTime, TimeUnit unit) {
        return tryWriteLock(name, 0, leaseTime, unit);
    }

    /**
     * 尝试获取写锁，可等待。
     *
     * @param name      锁名称
     * @param waitTime  最大等待时间（0=不等待）
     * @param leaseTime 锁过期时间
     * @param unit      时间单位
     * @return true 获取成功
     */
    public boolean tryWriteLock(String name, long waitTime, long leaseTime, TimeUnit unit) {
        long leaseMs = leaseTime > 0 ? unit.toMillis(leaseTime) : DEFAULT_LEASE_TIME_MS;
        long waitMs = unit.toMillis(waitTime);
        long threadId = Thread.currentThread().getId();
        String lockKey = RedisKeyConstant.rwlockKey(name);
        String entryName = getWriteEntryName(threadId);

        // 首次尝试
        Long ttl = evalWriteLock(lockKey, leaseMs, entryName);
        if (ttl == null) {
            return true;
        }

        // 不等待
        if (waitMs <= 0) {
            return false;
        }

        // 轮询等待
        return waitForWriteLock(lockKey, entryName, leaseMs, waitMs);
    }

    /**
     * 释放写锁。如果锁不属于当前线程，返回 false。
     *
     * @param name 锁名称
     * @return true 解锁成功
     */
    public boolean unlockWrite(String name) {
        long threadId = Thread.currentThread().getId();
        String lockKey = RedisKeyConstant.rwlockKey(name);
        String entryName = getWriteEntryName(threadId);

        Long result = redisTemplate.execute(
                WRITE_UNLOCK_SCRIPT,
                List.of(lockKey),
                String.valueOf(DEFAULT_LEASE_TIME_MS), entryName
        );
        if (result == null) {
            log.warn("写锁解锁失败：锁不属于当前线程, key={}", lockKey);
            return false;
        }
        return true;
    }

    // ==================== 读锁 API ====================

    /**
     * 尝试获取读锁，不等待。
     *
     * @param name      锁名称
     * @param leaseTime 锁过期时间
     * @param unit      时间单位
     * @return true 获取成功
     */
    public boolean readLock(String name, long leaseTime, TimeUnit unit) {
        return tryReadLock(name, 0, leaseTime, unit);
    }

    /**
     * 尝试获取读锁，可等待。
     *
     * @param name      锁名称
     * @param waitTime  最大等待时间（0=不等待）
     * @param leaseTime 锁过期时间
     * @param unit      时间单位
     * @return true 获取成功
     */
    public boolean tryReadLock(String name, long waitTime, long leaseTime, TimeUnit unit) {
        long leaseMs = leaseTime > 0 ? unit.toMillis(leaseTime) : DEFAULT_LEASE_TIME_MS;
        long waitMs = unit.toMillis(waitTime);
        long threadId = Thread.currentThread().getId();
        String lockKey = RedisKeyConstant.rwlockKey(name);
        String readEntryName = getReadEntryName(threadId);
        String writeEntryName = getWriteEntryName(threadId);

        // 首次尝试
        Long ttl = evalReadLock(lockKey, leaseMs, readEntryName, writeEntryName);
        if (ttl == null) {
            return true;
        }

        // 不等待
        if (waitMs <= 0) {
            return false;
        }

        // 轮询等待
        return waitForReadLock(lockKey, readEntryName, writeEntryName, leaseMs, waitMs);
    }

    /**
     * 释放读锁。如果锁不属于当前线程，返回 false。
     *
     * @param name 锁名称
     * @return true 解锁成功
     */
    public boolean unlockRead(String name) {
        long threadId = Thread.currentThread().getId();
        String lockKey = RedisKeyConstant.rwlockKey(name);
        String entryName = getReadEntryName(threadId);

        Long result = redisTemplate.execute(
                READ_UNLOCK_SCRIPT,
                List.of(lockKey),
                String.valueOf(DEFAULT_LEASE_TIME_MS), entryName
        );
        if (result == null) {
            log.warn("读锁解锁失败：锁不属于当前线程, key={}", lockKey);
            return false;
        }
        return true;
    }

    // ==================== 内部方法 ====================

    private String getWriteEntryName(long threadId) {
        return "write_lock:" + clientId + ":" + threadId;
    }

    private String getReadEntryName(long threadId) {
        return "read_lock:" + clientId + ":" + threadId;
    }

    private Long evalWriteLock(String lockKey, long leaseMs, String entryName) {
        try {
            return redisTemplate.execute(
                    WRITE_LOCK_SCRIPT,
                    List.of(lockKey),
                    String.valueOf(leaseMs), entryName
            );
        } catch (Exception e) {
            log.error("写锁加锁异常: key={}", lockKey, e);
            return 0L;
        }
    }

    private Long evalReadLock(String lockKey, long leaseMs, String readEntryName, String writeEntryName) {
        try {
            return redisTemplate.execute(
                    READ_LOCK_SCRIPT,
                    List.of(lockKey),
                    String.valueOf(leaseMs), readEntryName, writeEntryName
            );
        } catch (Exception e) {
            log.error("读锁加锁异常: key={}", lockKey, e);
            return 0L;
        }
    }

    private boolean waitForWriteLock(String lockKey, String entryName, long leaseMs, long waitMs) {
        long deadline = System.currentTimeMillis() + waitMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            Long ttl = evalWriteLock(lockKey, leaseMs, entryName);
            if (ttl == null) {
                return true;
            }
            if (System.currentTimeMillis() >= deadline) {
                break;
            }
        }
        return false;
    }

    private boolean waitForReadLock(String lockKey, String readEntryName, String writeEntryName,
                                    long leaseMs, long waitMs) {
        long deadline = System.currentTimeMillis() + waitMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            Long ttl = evalReadLock(lockKey, leaseMs, readEntryName, writeEntryName);
            if (ttl == null) {
                return true;
            }
            if (System.currentTimeMillis() >= deadline) {
                break;
            }
        }
        return false;
    }

    // ==================== 测试辅助方法 ====================

    public String getClientId() {
        return clientId;
    }
}
