package com.xiandou.utils;

import com.xiandou.redis.constant.RedisKeyConstant;
import com.xiandou.redis.core.DistributedLock;
import com.xiandou.utils.lock.RedisLockResult;
import com.xiandou.utils.lock.RedisLockUtil;
import com.xiandou.utils.lock.VoidSupplier;
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
 * RedisUtil（锁外观层）— 真实 Redis Cluster 深度集成测试。
 * <p>覆盖：executeTryLock / executeLock 两大系列、Supplier/VoidSupplier 变体、
 * 锁占返回失败、异常解锁、重入、阻塞等待。</p>
 *
 * <p>前置条件：Redis Cluster 已启动（192.168.56.186:6379-6381）</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.MethodName.class)
class RedisUtilRealTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    /** 每次测试用独立的锁名前缀 */
    private final AtomicReference<String> lockPrefix = new AtomicReference<>("");

    @BeforeEach
    void setUp() {
        DistributedLock lock = new DistributedLock(redisTemplate);
        RedisLockUtil.init(lock);
        lockPrefix.set("test:real:util:" + System.nanoTime() + ":");
    }

    private String lk(String method) {
        return lockPrefix.get() + method;
    }

    // ================================================================
    //  1. executeTryLock — 基础
    // ================================================================

    @Test
    @DisplayName("executeTryLock-加锁成功并返回结果")
    void executeTryLockSuccess() {
        String key = lk("success");
        RedisLockResult<String> r = RedisLockUtil.executeTryLock(key, 5, () -> "ok");
        assertFalse(r.isFailure());
        assertEquals("ok", r.getObj());
    }

    @Test
    @DisplayName("executeTryLock-锁被占返回失败")
    void executeTryLockLockHeld() {
        String key = lk("held");
        DistributedLock lock = new DistributedLock(redisTemplate);
        assertTrue(lock.tryLock(key, 10, TimeUnit.SECONDS));

        RedisLockResult<String> r = RedisLockUtil.executeTryLock(key, 0, () -> "should not run");
        assertTrue(r.isFailure());
        assertNull(r.getObj());
        lock.unlock(key);
    }

    @Test
    @DisplayName("executeTryLock-VoidSupplier变体")
    void executeTryLockVoid() {
        String key = lk("void");
        AtomicBoolean ran = new AtomicBoolean(false);
        RedisLockResult<Void> r = RedisLockUtil.executeTryLock(key, 5, () -> ran.set(true));
        assertFalse(r.isFailure());
        assertTrue(ran.get());
    }

    @Test
    @DisplayName("executeTryLock-带expireSeconds参数")
    void executeTryLockWithExpire() {
        String key = lk("expire");
        String result = RedisLockUtil.executeTryLock(key, 5, 3, (Supplier<String>) () -> "exp").getObj();
        assertEquals("exp", result);
    }

    @Test
    @DisplayName("executeTryLock-带expireSeconds+VoidSupplier")
    void executeTryLockWithExpireVoid() {
        String key = lk("expVoid");
        AtomicBoolean ran = new AtomicBoolean(false);
        RedisLockUtil.executeTryLock(key, 5, 3, () -> ran.set(true));
        assertTrue(ran.get());
    }

    @Test
    @DisplayName("executeTryLock-全参数TimeUnit版本+Supplier")
    void executeTryLockFullParamSupplier() {
        String key = lk("fullSup");
        String r = RedisLockUtil.executeTryLock(key, 3, 30, TimeUnit.SECONDS,
                (Supplier<String>) () -> "full").getObj();
        assertEquals("full", r);
    }

    @Test
    @DisplayName("executeTryLock-全参数TimeUnit版本+VoidSupplier")
    void executeTryLockFullParamVoid() {
        String key = lk("fullVoid");
        AtomicBoolean ran = new AtomicBoolean(false);
        RedisLockUtil.executeTryLock(key, 3, 30, TimeUnit.SECONDS, () -> ran.set(true));
        assertTrue(ran.get());
    }

    // ================================================================
    //  2. executeTryLock — 异常与错误
    // ================================================================

    @Test
    @DisplayName("executeTryLock-Supplier抛异常后锁释放")
    void executeTryLockExceptionUnlocks() {
        String key = lk("exUnlock");
        String lockKey = RedisKeyConstant.lockKey(key);

        assertThrows(RuntimeException.class, () ->
            RedisLockUtil.executeTryLock(key, 5,
                (Supplier<String>) () -> { throw new RuntimeException("oops"); })
        );
        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(lockKey)), "异常后锁 Key 应删除");
    }

    @Test
    @DisplayName("executeTryLock-异常解锁后其他线程可获取")
    void executeTryLockExceptionThenOtherCanLock() throws InterruptedException {
        String key = lk("exThenLock");
        String lockKey = RedisKeyConstant.lockKey(key);

        assertThrows(RuntimeException.class, () ->
            RedisLockUtil.executeTryLock(key, 5,
                (Supplier<String>) () -> { throw new RuntimeException("oops"); })
        );

        TimeUnit.MILLISECONDS.sleep(200);
        DistributedLock other = new DistributedLock(redisTemplate);
        assertTrue(other.tryLock(key, 5, TimeUnit.SECONDS), "异常释放后其他线程应能获取");
        other.unlock(key);
    }

    @Test
    @DisplayName("executeTryLock-VoidSupplier抛异常后解锁")
    void executeTryLockVoidExceptionUnlocks() {
        String key = lk("exVoid");
        assertThrows(RuntimeException.class, () ->
            RedisLockUtil.executeTryLock(key, 5, (VoidSupplier) () -> { throw new RuntimeException("void oops"); })
        );
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.lockKey(key)));
    }

    // ================================================================
    //  3. executeLock — 阻塞
    // ================================================================

    @Test
    @DisplayName("executeLock-阻塞获取并返回结果")
    void executeLockBlocking() {
        String key = lk("blocking");
        String result = RedisLockUtil.executeLock(key, 30, TimeUnit.SECONDS,
                (Supplier<String>) () -> "blocking ok");
        assertEquals("blocking ok", result);
    }

    @Test
    @DisplayName("executeLock-VoidSupplier变体")
    void executeLockVoid() {
        String key = lk("blockVoid");
        AtomicBoolean ran = new AtomicBoolean(false);
        RedisLockUtil.executeLock(key, 30, TimeUnit.SECONDS, (VoidSupplier) () -> ran.set(true));
        assertTrue(ran.get());
    }

    @Test
    @DisplayName("executeLock-锁被占时阻塞直到获得")
    void executeLockBlocksUntilAvailable() throws InterruptedException {
        String key = lk("blockWait");
        DistributedLock lock = new DistributedLock(redisTemplate);
        assertTrue(lock.tryLock(key, 10, TimeUnit.SECONDS));

        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            ready.countDown();
            String r = RedisLockUtil.executeLock(key, 5, TimeUnit.SECONDS,
                    (Supplier<String>) () -> "obtained");
            if ("obtained".equals(r)) got.set(true);
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        lock.unlock(key); // 释放 → 子线程获得

        t.join(8000);
        assertTrue(got.get(), "锁释放后 executeLock 应执行");
    }

    @Test
    @DisplayName("executeLock-VoidSupplier阻塞直至获得")
    void executeLockVoidBlocks() throws InterruptedException {
        String key = lk("blockVoidWait");
        DistributedLock lock = new DistributedLock(redisTemplate);
        assertTrue(lock.tryLock(key, 10, TimeUnit.SECONDS));

        AtomicBoolean ran = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            ready.countDown();
            RedisLockUtil.executeLock(key, 5, TimeUnit.SECONDS,
                    (VoidSupplier) () -> ran.set(true));
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        lock.unlock(key);

        t.join(8000);
        assertTrue(ran.get(), "释放后 VoidSupplier 应执行");
    }

    @Test
    @DisplayName("executeLock-Supplier异常后仍解锁")
    void executeLockExceptionUnlocks() {
        String key = lk("blockEx");
        String lockKey = RedisKeyConstant.lockKey(key);

        assertThrows(RuntimeException.class, () ->
            RedisLockUtil.executeLock(key, 30, TimeUnit.SECONDS,
                (Supplier<String>) () -> { throw new RuntimeException("ex"); })
        );
        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(lockKey)), "异常后锁 Key 应删除");
    }

    // ================================================================
    //  4. 重入与并发
    // ================================================================

    @Test
    @DisplayName("重入-executeTryLock嵌套调用（同一线程）")
    void reentrantExecuteTryLock() {
        String key = lk("reentrant");
        RedisLockResult<String> outer = RedisLockUtil.executeTryLock(key, 5,
                () -> {
                    // 内部再次获取同一把锁（重入）
                    RedisLockResult<String> inner = RedisLockUtil.executeTryLock(key, 0,
                            () -> "inner");
                    return inner.isFailure() ? "inner_failed" : "outer:" + inner.getObj();
                });
        assertFalse(outer.isFailure());
        assertEquals("outer:inner", outer.getObj());
    }

    @Test
    @DisplayName("重入-executeLock嵌套调用（同一线程）")
    void reentrantExecuteLock() {
        String key = lk("reentrantLock");
        String result = RedisLockUtil.executeLock(key, 30, TimeUnit.SECONDS,
                (Supplier<String>) () ->
                    RedisLockUtil.executeLock(key, 30, TimeUnit.SECONDS,
                        (Supplier<String>) () -> "nested"));
        assertEquals("nested", result);
    }

    @Test
    @DisplayName("并发-多个executeTryLock竞争同一锁名")
    void concurrentExecuteTryLock() throws InterruptedException {
        String key = lk("concurrent");
        int n = 5;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        AtomicInteger success = new AtomicInteger(0);

        for (int i = 0; i < n; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    RedisLockResult<String> r = RedisLockUtil.executeTryLock(key, 5,
                            () -> {
                                try { TimeUnit.MILLISECONDS.sleep(100); } catch (InterruptedException ignored) {}
                                return "done";
                            });
                    if (!r.isFailure()) success.incrementAndGet();
                } catch (Exception ignored) {}
                done.countDown();
            }).start();
        }

        start.countDown();
        done.await(15, TimeUnit.SECONDS);

        assertTrue(success.get() >= 1, "至少一个线程成功");
    }

    @Test
    @DisplayName("并发-executeLock和executeTryLock互斥")
    void concurrencyLockAndTryLock() throws InterruptedException {
        String key = lk("mixMutex");
        AtomicBoolean tryLockGot = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        // 用 executeLock 先锁住（ready 在回调中触发，确保锁已持有）
        Thread holder = new Thread(() -> {
            RedisLockUtil.executeLock(key, 5, TimeUnit.SECONDS, (VoidSupplier) () -> {
                ready.countDown();
                try { TimeUnit.SECONDS.sleep(3); } catch (InterruptedException ignored) {}
            });
        });
        holder.start();

        ready.await();
        // 此时锁一定被持有（executeLock 内部已获取锁才执行回调）

        // executeTryLock 应失败
        RedisLockResult<String> r = RedisLockUtil.executeTryLock(key, 1, () -> "try");
        assertTrue(r.isFailure(), "锁被占时 executeTryLock 应返回失败");

        holder.join();
    }

    // ================================================================
    //  5. 边界与错误
    // ================================================================

    @Test
    @DisplayName("边界-executeTryLock等待时间0立即返回失败")
    void executeTryLockWaitZero() {
        String key = lk("waitZero");
        DistributedLock lock = new DistributedLock(redisTemplate);
        assertTrue(lock.tryLock(key, 10, TimeUnit.SECONDS));

        RedisLockResult<String> r = RedisLockUtil.executeTryLock(key, 0, () -> "x");
        assertTrue(r.isFailure());
        lock.unlock(key);
    }

    @Test
    @DisplayName("边界-executeTryLock等待时间负数视为0")
    void executeTryLockWaitNegative() {
        String key = lk("waitNeg");
        // waitTime 视为 0，锁空闲则立即获取
        RedisLockResult<String> r = RedisLockUtil.executeTryLock(key, -1, () -> "negOk");
        assertFalse(r.isFailure());
        assertEquals("negOk", r.getObj());
    }

    @Test
    @DisplayName("边界-executeLock不启动Watchdog（指定expireTime>0）")
    void executeLockNoWatchdog() throws InterruptedException {
        String key = lk("noWd");
        String lockKey = RedisKeyConstant.lockKey(key);

        // expireTime=3s，不启动 Watchdog
        RedisLockUtil.executeLock(key, 3, TimeUnit.SECONDS, (VoidSupplier) () -> {
            // 锁存在
            assertNotNull(redisTemplate.hasKey(lockKey));
        });

        // 等锁过期
        TimeUnit.SECONDS.sleep(4);
        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(lockKey)), "指定过期时间后不应续期");
    }

    @Test
    @DisplayName("边界-6个重载方法均正常工作")
    void allOverloadsWork() {
        String prefix = lk("ov");

        // 1. waitSeconds + Supplier
        assertNotNull(RedisLockUtil.executeTryLock(prefix + "a", 3, () -> "s1").getObj());
        // 2. waitSeconds + VoidSupplier
        assertFalse(RedisLockUtil.executeTryLock(prefix + "b", 3, () -> {}).isFailure());
        // 3. waitSeconds + expireSeconds + Supplier
        assertNotNull(RedisLockUtil.executeTryLock(prefix + "c", 3, 5, (Supplier<String>) () -> "s3").getObj());
        // 4. waitSeconds + expireSeconds + VoidSupplier
        assertFalse(RedisLockUtil.executeTryLock(prefix + "d", 3, 5, () -> {}).isFailure());
        // 5. waitTime + expireTime + TimeUnit + Supplier
        assertNotNull(RedisLockUtil.executeTryLock(prefix + "e", 3, 30, TimeUnit.SECONDS,
                (Supplier<String>) () -> "s5").getObj());
        // 6. waitTime + expireTime + TimeUnit + VoidSupplier
        assertFalse(RedisLockUtil.executeTryLock(prefix + "f", 3, 30, TimeUnit.SECONDS,
                () -> {}).isFailure());
    }
}
