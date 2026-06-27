package com.xiandou.utils.lock;

import com.xiandou.redis.core.DistributedLock;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Redis 工具类——纯外观层，委托给 DistributedLock。
 * 保持与原始 API 完全兼容。
 */
public class RedisLockUtil {
    private static volatile DistributedLock distributedLock;

    public static void init(DistributedLock lock) {
        if (lock == null) {
            throw new IllegalArgumentException("DistributedLock must not be null");
        }
        distributedLock = lock;
    }

    private static DistributedLock getDistributedLock() {
        DistributedLock lock = distributedLock;
        if (lock == null) {
            throw new IllegalStateException("RedisLockUtil 未初始化，请确保 RedisUtilInitializer 已正确注入");
        }
        return lock;
    }

    // ==================== executeTryLock 系列 ====================

    public static <V> RedisLockResult<V> executeTryLock(String keyName, long waitSeconds, Supplier<V> supplier) {
        return executeTryLock(keyName, waitSeconds, -1, TimeUnit.SECONDS, supplier);
    }

    public static RedisLockResult<Void> executeTryLock(String keyName, long waitSeconds, VoidSupplier supplier) {
        return executeTryLock(keyName, waitSeconds, -1, TimeUnit.SECONDS, supplier);
    }

    public static <V> RedisLockResult<V> executeTryLock(String keyName, long waitSeconds, long expireSeconds, Supplier<V> supplier) {
        return executeTryLock(keyName, waitSeconds, expireSeconds, TimeUnit.SECONDS, supplier);
    }

    public static RedisLockResult<Void> executeTryLock(String keyName, long waitSeconds, long expireSeconds, VoidSupplier supplier) {
        return executeTryLock(keyName, waitSeconds, expireSeconds, TimeUnit.SECONDS, supplier);
    }

    public static <V> RedisLockResult<V> executeTryLock(String keyName, long waitTime, long expireTime, TimeUnit timeUnit, Supplier<V> supplier) {
        return doExecuteTryLock(keyName, waitTime, expireTime, timeUnit, supplier);
    }

    public static RedisLockResult<Void> executeTryLock(String keyName, long waitTime, long expireTime, TimeUnit timeUnit, VoidSupplier supplier) {
        return doExecuteTryLock(keyName, waitTime, expireTime, timeUnit, supplier);
    }

    private static <V> RedisLockResult<V> doExecuteTryLock(String keyName, long waitTime, long expireTime, TimeUnit timeUnit, Object supplier) {
        // -1的时候才能启动看门狗
        long leaseMs = expireTime >= 0 ? timeUnit.toMillis(expireTime) : -1;
        long waitMs = timeUnit.toMillis(waitTime);

        boolean locked = getDistributedLock().tryLock(keyName, waitMs, leaseMs, TimeUnit.MILLISECONDS);
        if (!locked) {
            return RedisLockResult.fail();
        }

        try {
            if (supplier instanceof Supplier) {
                @SuppressWarnings("unchecked")
                V result = ((Supplier<V>) supplier).get();
                return RedisLockResult.success(result);
            }
            ((VoidSupplier) supplier).get();
        } finally {
            getDistributedLock().unlock(keyName);
        }
        return RedisLockResult.success(null);
    }

    // ==================== executeLock 系列 ====================

    public static <V> V executeLock(String keyName, long leaseSeconds, Supplier<V> supplier) {
        return doExecuteLock(keyName, leaseSeconds, TimeUnit.SECONDS, supplier);
    }

    public static <V> V executeLock(String keyName, long leaseTime, TimeUnit timeUnit, Supplier<V> supplier) {
        return doExecuteLock(keyName, leaseTime, timeUnit, supplier);
    }

    public static void executeLock(String keyName, long leaseTime, TimeUnit timeUnit, VoidSupplier supplier) {
        doExecuteLock(keyName, leaseTime, timeUnit, supplier);
    }

    private static <V> V doExecuteLock(String keyName, long leaseTime, TimeUnit timeUnit, Object supplier) {
        // -1的时候才能启动看门狗
        long leaseMs = leaseTime >= 0 ? timeUnit.toMillis(leaseTime) : -1;
        DistributedLock lock = getDistributedLock();
        lock.lock(keyName, leaseMs, TimeUnit.MILLISECONDS);

        try {
            if (supplier instanceof Supplier) {
                @SuppressWarnings("unchecked")
                V result = ((Supplier<V>) supplier).get();
                return result;
            }
            ((VoidSupplier) supplier).get();
        } finally {
            lock.unlock(keyName);
        }
        return null;
    }
}
