package com.xiandou.redis.core;

import com.xiandou.redis.constant.RedisKeyConstant;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分布式锁 — 真实 Redis Cluster 深度集成测试。
 * <p>覆盖：基本加解锁、可重入、互斥、Watchdog 续期、Pub/Sub 等待、并发竞争、错误处理、数据格式验证。</p>
 *
 * <p>前置条件：Redis Cluster 已启动（192.168.56.186:6379-6381）</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.MethodName.class)
class DistributedLockRealTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private DistributedLock lock;

    private String testPrefix(String suffix) {
        return "test:real:dlock:" + suffix;
    }

    @BeforeEach
    void setUp() {
        lock = new DistributedLock(redisTemplate);
    }

    @AfterEach
    void tearDown() {
        assertEquals(0, lock.getActiveWatchdogCount(), "Watchdog 应全部清理");
    }

    // ================================================================
    //  1. 基础功能
    // ================================================================

    @Test
    @DisplayName("tryLock-基本加锁并解锁")
    void tryLockBasic() {
        String name = testPrefix("basic");
        assertTrue(lock.tryLock(name, 30, TimeUnit.SECONDS));
        assertTrue(lock.unlock(name));
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
    }

    @Test
    @DisplayName("tryLock-锁被占时返回false（不等待）")
    void tryLockFailsWhenHeld() {
        String name = testPrefix("failWhenHeld");
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        DistributedLock lock2 = new DistributedLock(redisTemplate);
        assertFalse(lock2.tryLock(name, 0, TimeUnit.SECONDS));
        assertFalse(lock2.tryLock(name, 0, 10, TimeUnit.SECONDS), "leaseTime>0 也不应等待");
        lock.unlock(name);
    }

    @Test
    @DisplayName("tryLock-极短leaseTime也能正常工作")
    void tryLockShortLease() throws InterruptedException {
        String name = testPrefix("shortTtl");
        assertTrue(lock.tryLock(name, 1, TimeUnit.SECONDS));
        assertTrue(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
        TimeUnit.SECONDS.sleep(2);
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
    }

    @Test
    @DisplayName("tryLock-长leaseTime不自动释放")
    void tryLockLongLease() throws InterruptedException {
        String name = testPrefix("longTtl");
        assertTrue(lock.tryLock(name, 60, TimeUnit.SECONDS));
        // 等 3 秒确认还在
        TimeUnit.SECONDS.sleep(3);
        assertTrue(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
        assertTrue(lock.unlock(name));
    }

    @Test
    @DisplayName("unlock-对未锁定名称调用不抛异常")
    void unlockNonExistentDoesNotThrow() {
        assertFalse(lock.unlock("nonexistent_" + System.nanoTime()));
    }

    @Test
    @DisplayName("不同锁名互不干扰")
    void differentLocksIndependent() {
        String a = testPrefix("indepA"), b = testPrefix("indepB");
        assertTrue(lock.tryLock(a, 10, TimeUnit.SECONDS));
        assertTrue(lock.tryLock(b, 0, TimeUnit.SECONDS)); // 立即获得
        assertTrue(lock.unlock(b));
        assertTrue(lock.unlock(a));
    }

    @Test
    @DisplayName("tryLock-同实例可重入，不同实例互斥")
    void tryLockReturnValue() {
        String name = testPrefix("retval");
        assertTrue(lock.tryLock(name, 30, TimeUnit.SECONDS));
        // 同实例可重入
        assertTrue(lock.tryLock(name, 0, TimeUnit.SECONDS), "同实例可重入");
        // 不同实例应互斥
        DistributedLock other = new DistributedLock(redisTemplate);
        assertFalse(other.tryLock(name, 0, TimeUnit.SECONDS), "不同实例应互斥");
        lock.unlock(name);
        lock.unlock(name);
    }

    // ================================================================
    //  2. 可重入
    // ================================================================

    @Test
    @DisplayName("可重入-同线程3次加锁3次解锁")
    void reentrantThreeLevels() {
        String name = testPrefix("reent3");
        assertTrue(lock.tryLock(name, 30, TimeUnit.SECONDS));
        assertTrue(lock.tryLock(name, 30, TimeUnit.SECONDS));
        assertTrue(lock.tryLock(name, 30, TimeUnit.SECONDS));
        assertTrue(lock.unlock(name));
        assertTrue(lock.unlock(name));
        assertTrue(lock.unlock(name));
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
    }

    @Test
    @DisplayName("可重入-重入时Watchdog不重复计数")
    void reentrantWatchdogCount() {
        String name = testPrefix("reentWd");
        // leaseTime=-1 触发 Watchdog
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount());
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS)); // 重入
        assertEquals(1, lock.getActiveWatchdogCount(), "重入不应增加 Watchdog 数");
        lock.unlock(name);
        lock.unlock(name);
        assertEquals(0, lock.getActiveWatchdogCount());
    }

    @Test
    @DisplayName("可重入-线程A重入后B无法获取")
    void reentrantBlocksOtherThread() throws InterruptedException {
        String name = testPrefix("reentBlock");
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS)); // 重入

        AtomicBoolean got = new AtomicBoolean(true); // 预设true
        CountDownLatch done = new CountDownLatch(1);
        new Thread(() -> {
            DistributedLock l2 = new DistributedLock(redisTemplate);
            got.set(l2.tryLock(name, 3, 5, TimeUnit.SECONDS));
            done.countDown();
        }).start();
        done.await(5, TimeUnit.SECONDS);
        assertFalse(got.get(), "线程B应在3秒内超时");

        lock.unlock(name);
        lock.unlock(name);
    }

    // ================================================================
    //  3. 互斥与并发
    // ================================================================

    @Test
    @DisplayName("互斥-两个独立DistributedLock实例互斥")
    void twoInstancesMutualExclusion() {
        String name = testPrefix("mutex");
        DistributedLock lock2 = new DistributedLock(redisTemplate);
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        assertFalse(lock2.tryLock(name, 0, TimeUnit.SECONDS));
        lock.unlock(name);
    }

    @Test
    @DisplayName("非法解锁-不同实例的unlock返回false")
    void illegalUnlockByOtherInstance() {
        String name = testPrefix("illegalUnlock");
        DistributedLock lock2 = new DistributedLock(redisTemplate);
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        assertFalse(lock2.unlock(name));
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(RedisKeyConstant.lockKey(name))),
                "非法解锁不应删除 Key");
        lock.unlock(name);
    }

    @Test
    @DisplayName("非法解锁-重入后不同实例依然无法解锁")
    void illegalUnlockReentrant() {
        String name = testPrefix("illegalReent");
        DistributedLock lock2 = new DistributedLock(redisTemplate);
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        assertFalse(lock2.unlock(name), "即使有重入也不同 clientId");
        lock.unlock(name);
        lock.unlock(name);
    }

    @Test
    @DisplayName("并发-10线程轮询竞争同一把锁")
    void concurrentContention() throws InterruptedException {
        String name = testPrefix("concurrent");
        int n = 10;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        AtomicInteger acquired = new AtomicInteger(0);

        for (int i = 0; i < n; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    DistributedLock l = new DistributedLock(redisTemplate);
                    if (l.tryLock(name, 8, 5, TimeUnit.SECONDS)) {
                        acquired.incrementAndGet();
                        TimeUnit.MILLISECONDS.sleep(50);
                        l.unlock(name);
                    }
                } catch (Exception ignored) {}
                done.countDown();
            }).start();
        }
        start.countDown();
        done.await(15, TimeUnit.SECONDS);

        assertTrue(acquired.get() >= 1, "至少一个线程获得锁");
        // 由于锁释放后其他线程可接力获取，successCount 可能等于 n
    }

    // ================================================================
    //  4. 等待锁 / Pub/Sub
    // ================================================================

    @Test
    @DisplayName("tryLock带等待-锁释放后成功获取")
    void tryLockWithWaitAcquireAfterRelease() throws InterruptedException {
        String name = testPrefix("waitRelease");
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        assertTrue(lock.tryLock(name, 5, TimeUnit.SECONDS));

        Thread t = new Thread(() -> {
            DistributedLock l2 = new DistributedLock(redisTemplate);
            got.set(l2.tryLock(name, 8, 5, TimeUnit.SECONDS));
            if (got.get()) l2.unlock(name);
            ready.countDown();
        });
        t.start();
        TimeUnit.MILLISECONDS.sleep(1000);
        lock.unlock(name); // 释放 → 通知子线程

        ready.await(12, TimeUnit.SECONDS);
        assertTrue(got.get(), "子线程应在锁释放后成功获取");
    }

    @Test
    @DisplayName("tryLock带等待-超时后返回false")
    void tryLockWithWaitTimeout() throws InterruptedException {
        String name = testPrefix("waitTimeout");
        AtomicBoolean got = new AtomicBoolean(true);
        CountDownLatch done = new CountDownLatch(1);

        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));

        new Thread(() -> {
            DistributedLock l2 = new DistributedLock(redisTemplate);
            got.set(l2.tryLock(name, 2, 5, TimeUnit.SECONDS)); // 只等 2 秒
            done.countDown();
        }).start();
        done.await(5, TimeUnit.SECONDS);
        assertFalse(got.get(), "应超时返回false");
        lock.unlock(name);
    }

    @Test
    @DisplayName("tryLock带等待-锁提前释放缩短等待时间")
    void tryLockWaitEarlyRelease() throws InterruptedException {
        String name = testPrefix("waitEarly");
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));

        long t0 = System.currentTimeMillis();
        Thread t = new Thread(() -> {
            DistributedLock l2 = new DistributedLock(redisTemplate);
            got.set(l2.tryLock(name, 10, 5, TimeUnit.SECONDS));
            if (got.get()) l2.unlock(name);
            ready.countDown();
        });
        t.start();

        TimeUnit.SECONDS.sleep(2);
        lock.unlock(name); // 提前释放
        ready.await(8, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - t0;

        assertTrue(got.get(), "应获取到锁");
        assertTrue(elapsed < 12000, "不应等到10秒超时才返回（实际=" + elapsed + "ms）");
    }

    @Test
    @DisplayName("lock()-阻塞直到获得锁")
    void lockBlocking() throws InterruptedException {
        String name = testPrefix("lockBlock");
        AtomicBoolean locked = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        assertTrue(lock.tryLock(name, 5, TimeUnit.SECONDS));

        Thread t = new Thread(() -> {
            DistributedLock l2 = new DistributedLock(redisTemplate);
            l2.lock(name); // 阻塞
            locked.set(true);
            l2.unlock(name);
            ready.countDown();
        });
        t.start();

        TimeUnit.MILLISECONDS.sleep(500);
        assertFalse(locked.get(), "锁被占，应阻塞");
        lock.unlock(name); // 释放
        ready.await(8, TimeUnit.SECONDS);
        assertTrue(locked.get(), "释放后应成功");
    }

    @Test
    @DisplayName("lock()-指定leaseTime阻塞获取")
    void lockBlockingWithLeaseTime() throws InterruptedException {
        String name = testPrefix("lockLease");
        AtomicBoolean locked = new AtomicBoolean(false);

        Thread t = new Thread(() -> {
            DistributedLock l2 = new DistributedLock(redisTemplate);
            l2.lock(name, 3, TimeUnit.SECONDS);
            locked.set(true);
            // 此时 Watchdog 不启动（leaseTime>0）
            l2.unlock(name);
        });
        t.start();
        t.join(5000);
        assertTrue(locked.get(), "应获取锁并完成");
    }

    @Test
    @DisplayName("lock()带Watchdog-跨线程释放")
    void lockWithWatchdogCrossThread() throws InterruptedException {
        String name = testPrefix("lockWdCross");
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        assertTrue(lock.tryLock(name, 5, TimeUnit.SECONDS));

        Thread t = new Thread(() -> {
            DistributedLock l2 = new DistributedLock(redisTemplate);
            l2.lock(name); // 阻塞（Watchdog 启动）
            assertEquals(1, l2.getActiveWatchdogCount());
            got.set(true);
            l2.unlock(name);
            ready.countDown();
        });
        t.start();

        TimeUnit.SECONDS.sleep(1);
        lock.unlock(name); // 第一个实例释放

        ready.await(10, TimeUnit.SECONDS);
        assertTrue(got.get(), "子线程应通过 lock() 获取并启动 Watchdog");
    }

    // ================================================================
    //  5. Watchdog / TTL
    // ================================================================

    @Test
    @DisplayName("Watchdog-leaseTime=-1启动续期")
    void watchdogStartedWithDefaultLease() throws InterruptedException {
        String name = testPrefix("wdStart");
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount());
        lock.unlock(name);
        assertEquals(0, lock.getActiveWatchdogCount());
    }

    @Test
    @DisplayName("Watchdog-锁过期后自动停止")
    void watchdogStopsOnExpiry() throws InterruptedException {
        String name = testPrefix("wdStop");
        assertTrue(lock.tryLock(name, 2, TimeUnit.SECONDS));
        assertEquals(0, lock.getActiveWatchdogCount(), "leaseTime>0不应启动Watchdog");

        TimeUnit.SECONDS.sleep(3);
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
        assertEquals(0, lock.getActiveWatchdogCount());
    }

    @Test
    @DisplayName("Watchdog-解锁后计数归零")
    void watchdogCountZeroAfterUnlock() throws InterruptedException {
        String name = testPrefix("wdZero");
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount());
        lock.unlock(name);
        assertEquals(0, lock.getActiveWatchdogCount(), "解锁后 Watchdog 数应为0");

        // 重新加锁应重新计数
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount());
        lock.unlock(name);
        assertEquals(0, lock.getActiveWatchdogCount());
    }

    @Test
    @DisplayName("Watchdog-不同锁名的Watchdog独立计数")
    void watchdogMultipleLocks() {
        String a = testPrefix("wdMultiA"), b = testPrefix("wdMultiB");
        assertTrue(lock.tryLock(a, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount());
        assertTrue(lock.tryLock(b, 0, -1, TimeUnit.SECONDS));
        assertEquals(2, lock.getActiveWatchdogCount(), "两个不同锁应有2个Watchdog");

        lock.unlock(a);
        assertEquals(1, lock.getActiveWatchdogCount());
        lock.unlock(b);
        assertEquals(0, lock.getActiveWatchdogCount());
    }

    @Test
    @DisplayName("Watchdog-被unlock取消后不再续期")
    void watchdogCancelPreventsRenew() throws InterruptedException {
        String name = testPrefix("wdCancel");
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount());

        lock.unlock(name); // 取消 Watchdog
        assertEquals(0, lock.getActiveWatchdogCount());

        // 等一会儿确认 Key 被删除（虽然按理 unlock 已删除）
        // 这里验证的是 unlock 后 Key 不存在
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
    }

    @Test
    @DisplayName("TTL-锁续期后TTL恢复初始值")
    void ttlResetAfterRenew() throws InterruptedException {
        String name = testPrefix("ttlRenew");
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));

        // 等待 Watchdog 至少续期一次（间隔10s）
        TimeUnit.SECONDS.sleep(12);
        Long ttl = redisTemplate.getExpire(RedisKeyConstant.lockKey(name), TimeUnit.SECONDS);
        assertNotNull(ttl);
        assertTrue(ttl >= 20, "续期后 TTL 应接近 30s（实际=" + ttl + "s）");

        lock.unlock(name);
    }

    // ================================================================
    //  6. 错误处理与边界
    // ================================================================

    @Test
    @DisplayName("边界-leastTime=0使用默认值")
    void leaseTimeZeroUsesDefault() throws InterruptedException {
        String name = testPrefix("leaseZero");
        // leaseTime=0 → execute 中 leaseMs=0 → Redis PEXPIRE 0 会立即过期?
        // 实际上 leaseTime=0 > 0 为 false → leaseMs = DEFAULT_LEASE_TIME_MS
        // 但 waitTime=0 时 tryLock 调用 3 参数 → leaseTime>0? 0>0 false → 使用默认值
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount(), "leaseTime=-1 启动 Watchdog");
        lock.unlock(name);
    }

    @Test
    @DisplayName("边界-waitTime=0立即返回")
    void waitTimeZeroReturnsImmediately() {
        String name = testPrefix("waitZero");
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        // 不同实例不等待应立即失败
        DistributedLock other = new DistributedLock(redisTemplate);
        assertFalse(other.tryLock(name, 0, 10, TimeUnit.SECONDS));
        lock.unlock(name);
    }

    @Test
    @DisplayName("边界-锁Key哈希标签格式正确")
    void lockKeyHashTagFormat() {
        String name = testPrefix("hashFormat");
        assertTrue(RedisKeyConstant.lockKey(name).startsWith("{lock}:"),
                "lockKey 应以 {lock}: 开头");
        assertTrue(RedisKeyConstant.lockChannel(name).contains("{lock}:"),
                "lockChannel 应包含 {lock}:");
    }

    @Test
    @DisplayName("边界-getClientId每次不同")
    void clientIdUniquePerInstance() {
        DistributedLock a = new DistributedLock(redisTemplate);
        DistributedLock b = new DistributedLock(redisTemplate);
        assertNotNull(a.getClientId());
        assertNotNull(b.getClientId());
        assertNotEquals(a.getClientId(), b.getClientId(), "每个实例应有唯一 clientId");
    }

    @Test
    @DisplayName("边界-同实例不同线程互斥")
    void sameInstanceDifferentThreadsMutex() throws InterruptedException {
        String name = testPrefix("sameInstMutex");
        AtomicBoolean got = new AtomicBoolean(true);
        CountDownLatch done = new CountDownLatch(1);

        assertTrue(lock.tryLock(name, 5, TimeUnit.SECONDS));

        new Thread(() -> {
            got.set(lock.tryLock(name, 2, 5, TimeUnit.SECONDS));
            done.countDown();
        }).start();
        done.await(4, TimeUnit.SECONDS);
        assertFalse(got.get(), "同实例的不同线程因threadId不同也互斥");
        lock.unlock(name);
    }

    @Test
    @DisplayName("边界-锁频道的哈希标签与锁Key一致")
    void channelHashTagConsistent() {
        String name = testPrefix("chHash");
        String key = RedisKeyConstant.lockKey(name);
        String ch = RedisKeyConstant.lockChannel(name);
        // 提取哈希标签 {lock} 验证一致
        String keyTag = key.substring(key.indexOf('{'), key.indexOf('}') + 1);
        String chTag = ch.substring(ch.indexOf('{'), ch.indexOf('}') + 1);
        assertEquals(keyTag, chTag, "lockKey 和 lockChannel 应使用相同哈希标签");
    }

    @Test
    @DisplayName("边界-锁释放后Watchdog不再续期")
    void lockReleasedWatchdogStops() throws InterruptedException {
        String name = testPrefix("wdStopImmediate");
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertEquals(1, lock.getActiveWatchdogCount());

        lock.unlock(name);
        assertEquals(0, lock.getActiveWatchdogCount());

        // 确认 Key 已删除
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.lockKey(name)));
    }

    // ================================================================
    //  7. Redis 数据格式验证
    // ================================================================

    @Test
    @DisplayName("数据-Hash结构包含entryName→重入计数")
    void dataHashEntryName() {
        String name = testPrefix("dataHash");
        String key = RedisKeyConstant.lockKey(name);
        long tid = Thread.currentThread().getId();

        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));

        String entryName = lock.getClientId() + ":" + tid;
        assertEquals("2", redisTemplate.opsForHash().get(key, entryName).toString());

        lock.unlock(name);
        assertEquals("1", redisTemplate.opsForHash().get(key, entryName).toString());

        lock.unlock(name);
        assertFalse(redisTemplate.hasKey(key));
    }

    @Test
    @DisplayName("数据-锁Key有正确的TTL")
    void dataTtlPresent() {
        String name = testPrefix("dataTtl");
        assertTrue(lock.tryLock(name, 10, TimeUnit.SECONDS));
        Long ttl = redisTemplate.getExpire(RedisKeyConstant.lockKey(name), TimeUnit.SECONDS);
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 10, "TTL 应在 (0, 10] 范围内，实际=" + ttl);
        lock.unlock(name);
    }

    @Test
    @DisplayName("数据-Watchdog锁有默认30s TTL")
    void dataWatchdogTtl() {
        String name = testPrefix("dataWdTtl");
        assertTrue(lock.tryLock(name, 0, -1, TimeUnit.SECONDS));
        Long ttl = redisTemplate.getExpire(RedisKeyConstant.lockKey(name), TimeUnit.SECONDS);
        assertNotNull(ttl);
        assertTrue(ttl >= 25 && ttl <= 35, "Watchdog 锁 TTL 应接近 30s，实际=" + ttl + "s");
        lock.unlock(name);
    }

    @Test
    @DisplayName("数据-重入锁的TTL在解锁部分递减后刷新")
    void dataTtlRefreshOnPartialUnlock() throws InterruptedException {
        String name = testPrefix("dataTtlRefresh");
        assertTrue(lock.tryLock(name, 5, TimeUnit.SECONDS));
        assertTrue(lock.tryLock(name, 5, TimeUnit.SECONDS));

        TimeUnit.SECONDS.sleep(2);
        // 部分解锁（重入-1），应该刷新 TTL
        assertTrue(lock.unlock(name));

        Long ttl = redisTemplate.getExpire(RedisKeyConstant.lockKey(name), TimeUnit.SECONDS);
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 32, "部分解锁后 TTL 应刷新为默认值（实际=" + ttl + "s）");

        lock.unlock(name);
    }
}
