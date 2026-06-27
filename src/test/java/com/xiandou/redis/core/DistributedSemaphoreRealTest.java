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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分布式信号量 — 真实 Redis Cluster 深度集成测试。
 * <p>覆盖：初始化、获取/释放许可、阻塞等待、超时、并发竞争、边界条件。</p>
 *
 * <p>前置条件：Redis Cluster 已启动（192.168.56.186:6379-6381）</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.MethodName.class)
class DistributedSemaphoreRealTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private DistributedSemaphore semaphore;

    private String testPrefix(String suffix) {
        return "test:real:sem:" + suffix;
    }

    private void cleanup(String name) {
        redisTemplate.delete(RedisKeyConstant.semaphoreKey(name));
    }

    @BeforeEach
    void setUp() {
        semaphore = new DistributedSemaphore(redisTemplate);
    }

    // ================================================================
    //  1. 初始化
    // ================================================================

    @Test
    @DisplayName("trySetPermits-初始化成功并返回正确计数")
    void trySetPermitsSuccess() {
        String name = testPrefix("init");
        assertTrue(semaphore.trySetPermits(name, 5));
        assertEquals(5, semaphore.availablePermits(name));
        cleanup(name);
    }

    @Test
    @DisplayName("trySetPermits-Key已存在时返回false")
    void trySetPermitsExistsReturnsFalse() {
        String name = testPrefix("exists");
        assertTrue(semaphore.trySetPermits(name, 3));
        assertFalse(semaphore.trySetPermits(name, 10));
        assertEquals(3, semaphore.availablePermits(name), "值不应被覆盖");
        cleanup(name);
    }

    @Test
    @DisplayName("trySetPermits-负数抛出IllegalArgumentException")
    void trySetPermitsNegativeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> semaphore.trySetPermits(testPrefix("neg"), -1));
    }

    @Test
    @DisplayName("trySetPermits-许可数0创建空信号量")
    void trySetPermitsZero() {
        String name = testPrefix("zeroInit");
        assertTrue(semaphore.trySetPermits(name, 0));
        assertEquals(0, semaphore.availablePermits(name));
        cleanup(name);
    }

    @Test
    @DisplayName("availablePermits-Key不存在返回0")
    void availablePermitsNoKey() {
        assertEquals(0, semaphore.availablePermits("nonexistent:" + System.nanoTime()));
    }

    @Test
    @DisplayName("availablePermits-许可耗尽时返回0")
    void availablePermitsDepleted() {
        String name = testPrefix("depleted");
        semaphore.trySetPermits(name, 2);
        semaphore.tryAcquire(name, 2);
        assertEquals(0, semaphore.availablePermits(name));
        cleanup(name);
    }

    // ================================================================
    //  2. 获取与释放
    // ================================================================

    @Test
    @DisplayName("tryAcquire-许可充足时获取成功")
    void tryAcquireSuccess() {
        String name = testPrefix("acqOk");
        semaphore.trySetPermits(name, 5);
        assertTrue(semaphore.tryAcquire(name, 2));
        assertEquals(3, semaphore.availablePermits(name));
        cleanup(name);
    }

    @Test
    @DisplayName("tryAcquire-许可不足时返回false")
    void tryAcquireInsufficient() {
        String name = testPrefix("acqFail");
        semaphore.trySetPermits(name, 1);
        assertFalse(semaphore.tryAcquire(name, 3));
        assertEquals(1, semaphore.availablePermits(name), "许可数不变");
        cleanup(name);
    }

    @Test
    @DisplayName("tryAcquire-许可数为0时返回false")
    void tryAcquireZeroPermits() {
        String name = testPrefix("acqZero");
        semaphore.trySetPermits(name, 0);
        assertFalse(semaphore.tryAcquire(name, 1));
        cleanup(name);
    }

    @Test
    @DisplayName("tryAcquire-申请0许可直接返回true")
    void tryAcquireZeroRequest() {
        assertTrue(semaphore.tryAcquire("any", 0));
    }

    @Test
    @DisplayName("release-归还许可后计数增加")
    void releaseIncreasesPermits() {
        String name = testPrefix("relOk");
        semaphore.trySetPermits(name, 3);
        semaphore.release(name, 2);
        assertEquals(5, semaphore.availablePermits(name));
        cleanup(name);
    }

    @Test
    @DisplayName("release-未初始化时也能释放（INCR创建Key）")
    void releaseOnNonExistent() {
        String name = testPrefix("relNew");
        semaphore.release(name, 3);
        assertEquals(3, semaphore.availablePermits(name));
        cleanup(name);
    }

    @Test
    @DisplayName("release-释放0许可不改变计数")
    void releaseZero() {
        String name = testPrefix("relZero");
        semaphore.trySetPermits(name, 3);
        semaphore.release(name, 0);
        assertEquals(3, semaphore.availablePermits(name));
        cleanup(name);
    }

    @Test
    @DisplayName("acquire & release 完整生命周期")
    void fullLifecycle() throws InterruptedException {
        String name = testPrefix("lifecycle");
        cleanup(name); // 清理前次可能的残留
        boolean set = semaphore.trySetPermits(name, 3);
        assertTrue(set, "trySetPermits 应成功创建新 Key");
        assertEquals(3, semaphore.availablePermits(name), "初始化后许可数应为 3");

        // 获取 2 个
        assertTrue(semaphore.tryAcquire(name, 2), "应成功获取 2 个许可");
        assertEquals(1, semaphore.availablePermits(name));

        // 获取剩余的 1 个
        assertTrue(semaphore.tryAcquire(name, 1));
        assertEquals(0, semaphore.availablePermits(name));

        // 无法获取
        assertFalse(semaphore.tryAcquire(name, 1));

        // 归还 3 个
        semaphore.release(name, 3);
        assertEquals(3, semaphore.availablePermits(name));

        // 重新获取
        assertTrue(semaphore.tryAcquire(name, 3));
        assertEquals(0, semaphore.availablePermits(name));

        cleanup(name);
    }

    // ================================================================
    //  3. 阻塞等待
    // ================================================================

    @Test
    @DisplayName("acquire-许可证不足时阻塞直到有许可")
    void acquireBlocksUntilAvailable() throws InterruptedException {
        String name = testPrefix("block");
        semaphore.trySetPermits(name, 0);
        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                semaphore.acquire(name, 1);
                acquired.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        assertFalse(acquired.get(), "无许可应阻塞");

        semaphore.release(name, 1);
        t.join(5000);
        assertTrue(acquired.get(), "释放后应获取到");
        cleanup(name);
    }

    @Test
    @DisplayName("tryAcquire带超时-许可释放后成功获取")
    void tryAcquireWithTimeoutSuccess() throws InterruptedException {
        String name = testPrefix("timeoutSuccess");
        semaphore.trySetPermits(name, 0);
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                got.set(semaphore.tryAcquire(name, 1, 5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(300);
        semaphore.release(name, 1); // 释放许可

        t.join(6000);
        assertTrue(got.get(), "许可释放后应在超时前获取到");
        cleanup(name);
    }

    @Test
    @DisplayName("tryAcquire带超时-超时返回false")
    void tryAcquireWithTimeoutExpires() throws InterruptedException {
        String name = testPrefix("timeoutExpire");
        semaphore.trySetPermits(name, 0);

        long start = System.currentTimeMillis();
        boolean result = semaphore.tryAcquire(name, 1, 2, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(result, "无许可应超时");
        assertTrue(elapsed >= 1800, "应等待约2秒（实际=" + elapsed + "ms）");
        cleanup(name);
    }

    @Test
    @DisplayName("tryAcquire带超时-许可充足时立即返回（不等待）")
    void tryAcquireWithTimeoutPermitsAvailable() throws InterruptedException {
        String name = testPrefix("timeoutImmediate");
        semaphore.trySetPermits(name, 5);

        long start = System.currentTimeMillis();
        boolean result = semaphore.tryAcquire(name, 2, 10, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(result, "许可充足应成功");
        assertTrue(elapsed < 1000, "应立即返回，不应等待（实际=" + elapsed + "ms）");
        cleanup(name);
    }

    // ================================================================
    //  4. 并发竞争
    // ================================================================

    @Test
    @DisplayName("并发-多线程竞争同一信号量")
    void concurrentSemaphore() throws InterruptedException {
        String name = testPrefix("concurrent");
        cleanup(name);
        int totalPermits = 3;
        int threadCount = 10;
        semaphore.trySetPermits(name, totalPermits);

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    if (semaphore.tryAcquire(name, 1, 2, TimeUnit.SECONDS)) {
                        successCount.incrementAndGet();
                        TimeUnit.MILLISECONDS.sleep(100);
                        semaphore.release(name, 1);
                    }
                } catch (Exception ignored) {}
                done.countDown();
            }).start();
        }

        start.countDown();
        done.await(10, TimeUnit.SECONDS);

        // 最多 totalPermits 个同时获得，但由于释放后其他线程也能拿，总数可能 > totalPermits
        assertTrue(successCount.get() >= totalPermits, "至少应有" + totalPermits + "个线程成功（实际=" + successCount.get() + "）");
        assertEquals(totalPermits, semaphore.availablePermits(name), "最终许可应全部归还");
        cleanup(name);
    }

    @Test
    @DisplayName("并发-同一许可数多线程轮转")
    void concurrentPermitRotation() throws InterruptedException {
        String name = testPrefix("rotation");
        cleanup(name);
        int permits = 2;
        int threads = 5;
        int rounds = 3;
        semaphore.trySetPermits(name, permits);

        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger totalAcquired = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    for (int r = 0; r < rounds; r++) {
                        if (semaphore.tryAcquire(name, 1, 3, TimeUnit.SECONDS)) {
                            totalAcquired.incrementAndGet();
                            TimeUnit.MILLISECONDS.sleep(50);
                            semaphore.release(name, 1);
                        }
                    }
                } catch (Exception ignored) {}
                done.countDown();
            }).start();
        }

        done.await(15, TimeUnit.SECONDS);
        assertEquals(permits, semaphore.availablePermits(name), "全部归还");
        assertTrue(totalAcquired.get() > 0, "应有一定数量获取成功");
        cleanup(name);
    }

    // ================================================================
    //  5. 边界与错误
    // ================================================================

    @Test
    @DisplayName("边界-信号量Key哈希标签格式")
    void semaphoreKeyHashTag() {
        String name = testPrefix("hashTag");
        assertTrue(RedisKeyConstant.semaphoreKey(name).startsWith("{semaphore}:"),
                "Key 应以 {semaphore}: 开头");
        assertTrue(RedisKeyConstant.semaphoreChannel(name).contains("{semaphore}:"),
                "Channel 应包含 {semaphore}:");
    }

    @Test
    @DisplayName("边界-tryAcquire超时被中断")
    void tryAcquireInterrupted() throws InterruptedException {
        String name = testPrefix("interrupt");
        semaphore.trySetPermits(name, 0);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                semaphore.tryAcquire(name, 1, 5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        t.interrupt();
        t.join(3000);
        assertTrue(interrupted.get(), "中断后应抛出 InterruptedException");
        cleanup(name);
    }

    @Test
    @DisplayName("边界-acquire阻塞被中断")
    void acquireInterrupted() throws InterruptedException {
        String name = testPrefix("acquireInt");
        semaphore.trySetPermits(name, 0);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                semaphore.acquire(name, 1);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        t.interrupt();
        t.join(3000);
        assertTrue(interrupted.get(), "中断后应抛出 InterruptedException");
        cleanup(name);
    }

    // ================================================================
    //  6. TTL 过期边界
    // ================================================================

    @Test
    @DisplayName("TTL-过期后tryAcquire返回false")
    void ttlExpiredTryAcquireReturnsFalse() throws InterruptedException {
        String name = testPrefix("ttlExpAcq");
        DistributedSemaphore shortTtl = new DistributedSemaphore(redisTemplate, 2);
        shortTtl.trySetPermits(name, 3);
        TimeUnit.SECONDS.sleep(3);
        assertFalse(shortTtl.tryAcquire(name, 1), "过期后许可不可用");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-过期后trySetPermits可重新初始化")
    void ttlExpiredTrySetPermitsSucceeds() throws InterruptedException {
        String name = testPrefix("ttlExpSet");
        DistributedSemaphore shortTtl = new DistributedSemaphore(redisTemplate, 2);
        assertTrue(shortTtl.trySetPermits(name, 3));
        TimeUnit.SECONDS.sleep(3);
        assertTrue(shortTtl.trySetPermits(name, 5), "过期后 trySetPermits 应成功");
        assertEquals(5, shortTtl.availablePermits(name));
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-过期后availablePermits返回0")
    void ttlExpiredAvailablePermitsZero() throws InterruptedException {
        String name = testPrefix("ttlExpAvail");
        DistributedSemaphore shortTtl = new DistributedSemaphore(redisTemplate, 2);
        shortTtl.trySetPermits(name, 3);
        assertEquals(3, shortTtl.availablePermits(name));
        TimeUnit.SECONDS.sleep(3);
        assertEquals(0, shortTtl.availablePermits(name), "过期后可用许可为0");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-release在过期Key上创建新Key（INCRBY行为）")
    void ttlExpiredReleaseCreatesKey() throws InterruptedException {
        String name = testPrefix("ttlExpRel");
        DistributedSemaphore shortTtl = new DistributedSemaphore(redisTemplate, 2);
        shortTtl.trySetPermits(name, 3);
        TimeUnit.SECONDS.sleep(3);
        shortTtl.release(name, 5);
        assertEquals(5, shortTtl.availablePermits(name), "release 在过期 Key 上应通过 INCRBY 创建新许可");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-活跃信号量的acquire刷新过期时间")
    void ttlActiveAcquireRefreshesTtl() throws InterruptedException {
        String name = testPrefix("ttlAcRefresh");
        DistributedSemaphore shortTtl = new DistributedSemaphore(redisTemplate, 3);
        shortTtl.trySetPermits(name, 5);
        TimeUnit.SECONDS.sleep(2);
        assertTrue(shortTtl.tryAcquire(name, 1), "acquire 刷新 TTL 后应可获取");
        TimeUnit.SECONDS.sleep(2);
        assertEquals(4, shortTtl.availablePermits(name), "TTL 被 acquire 续期，许可仍存在");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-活跃信号量的release刷新过期时间")
    void ttlActiveReleaseRefreshesTtl() throws InterruptedException {
        String name = testPrefix("ttlRelRefresh");
        DistributedSemaphore shortTtl = new DistributedSemaphore(redisTemplate, 3);
        shortTtl.trySetPermits(name, 2);
        TimeUnit.SECONDS.sleep(2);
        shortTtl.release(name, 1);
        TimeUnit.SECONDS.sleep(2);
        assertEquals(3, shortTtl.availablePermits(name), "release 续期后许可仍存在");
        cleanup(name);
    }
}
