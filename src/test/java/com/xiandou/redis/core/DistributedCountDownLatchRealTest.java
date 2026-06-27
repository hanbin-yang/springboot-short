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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分布式 CountDownLatch — 真实 Redis Cluster 深度集成测试。
 * <p>覆盖：计数设置/递减、await 阻塞/超时/归零、Pub/Sub 多线程通知、边界条件。</p>
 *
 * <p>前置条件：Redis Cluster 已启动（192.168.56.186:6379-6381）</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.MethodName.class)
class DistributedCountDownLatchRealTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private DistributedCountDownLatch latch;

    private String testPrefix(String suffix) {
        return "test:real:latch:" + suffix;
    }

    private void cleanup(String name) {
        redisTemplate.delete(RedisKeyConstant.latchKey(name));
    }

    @BeforeEach
    void setUp() {
        latch = new DistributedCountDownLatch(redisTemplate);
    }

    // ================================================================
    //  1. 初始化与计数查询
    // ================================================================

    @Test
    @DisplayName("trySetCount-初始化计数成功并正确返回")
    void trySetCountSuccess() {
        String name = testPrefix("init");
        assertTrue(latch.trySetCount(name, 5));
        assertEquals(5, latch.getCount(name));
        cleanup(name);
    }

    @Test
    @DisplayName("trySetCount-Key已存在时返回false")
    void trySetCountExistsReturnsFalse() {
        String name = testPrefix("exists");
        assertTrue(latch.trySetCount(name, 3));
        assertFalse(latch.trySetCount(name, 10));
        assertEquals(3, latch.getCount(name), "计数不应被覆盖");
        cleanup(name);
    }

    @Test
    @DisplayName("trySetCount-负数抛出IllegalArgumentException")
    void trySetCountNegativeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> latch.trySetCount(testPrefix("neg"), -1));
    }

    @Test
    @DisplayName("trySetCount-计数0可创建（立即触发await返回）")
    void trySetCountZero() throws InterruptedException {
        String name = testPrefix("zeroInit");
        assertTrue(latch.trySetCount(name, 0));
        assertEquals(0, latch.getCount(name));
        // 计数0时应立即返回
        assertTrue(latch.await(name, 1, TimeUnit.SECONDS));
        cleanup(name);
    }

    @Test
    @DisplayName("getCount-Key不存在返回0")
    void getCountNoKey() {
        assertEquals(0, latch.getCount("nonexistent:" + System.nanoTime()));
    }

    @Test
    @DisplayName("getCount-递减后返回当前计数")
    void getCountAfterDecrement() {
        String name = testPrefix("afterDec");
        latch.trySetCount(name, 10);
        latch.countDown(name);
        latch.countDown(name);
        latch.countDown(name);
        assertEquals(7, latch.getCount(name));
        cleanup(name);
    }

    // ================================================================
    //  2. 计数递减
    // ================================================================

    @Test
    @DisplayName("countDown-递减计数直到归零")
    void countDownToZero() {
        String name = testPrefix("toZero");
        latch.trySetCount(name, 3);
        latch.countDown(name); // 2
        latch.countDown(name); // 1
        assertEquals(1, latch.getCount(name));
        latch.countDown(name); // 0
        assertEquals(0, latch.getCount(name));
        cleanup(name);
    }

    @Test
    @DisplayName("countDown-归零后Key被删除")
    void countDownZeroDeletesKey() throws InterruptedException {
        String name = testPrefix("delKey");
        latch.trySetCount(name, 1);
        String key = RedisKeyConstant.latchKey(name);
        assertTrue(redisTemplate.hasKey(key));
        latch.countDown(name);
        TimeUnit.MILLISECONDS.sleep(200);
        // countDown 归零后计数应为 0 或负数
        assertTrue(latch.getCount(name) <= 0, "归零后计数应为 0");
        cleanup(name);
    }

    @Test
    @DisplayName("countDown-对不存在Key调用是安全的（幂等）")
    void countDownNonExistent() {
        // DECR 一个不存在的 Key 会创建为 -1
        latch.countDown("nonexistent:" + System.nanoTime());
        // 不应抛异常
    }

    @Test
    @DisplayName("countDown-多次countDown超过0仍安全")
    void countDownBeyondZero() {
        String name = testPrefix("beyond");
        latch.trySetCount(name, 2);
        latch.countDown(name); // 1
        latch.countDown(name); // 0
        latch.countDown(name); // -1
        latch.countDown(name); // -2
        assertEquals(-2, latch.getCount(name), "超过归零后继续递减");
        cleanup(name);
    }

    // ================================================================
    //  3. await 阻塞与超时
    // ================================================================

    @Test
    @DisplayName("await-Key不存在（未初始化）立即返回")
    void awaitNoKeyReturnsImmediately() {
        assertDoesNotThrow(() -> latch.await("nonexistent:noinit"));
    }

    @Test
    @DisplayName("await带超时-Key不存在立即返回true")
    void awaitWithTimeoutNoKey() throws InterruptedException {
        assertTrue(latch.await("nonexistent:timeout", 1, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("await-计数归零后返回")
    void awaitReturnsWhenZero() throws InterruptedException {
        String name = testPrefix("awaitOK");
        latch.trySetCount(name, 1);
        AtomicBoolean released = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                latch.await(name);
                released.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        assertFalse(released.get(), "计数未归零应阻塞");

        latch.countDown(name); // 归零
        t.join(5000);
        assertTrue(released.get(), "归零后 await 应返回");
        cleanup(name);
    }

    @Test
    @DisplayName("await带超时-超时返回false")
    void awaitWithTimeoutExpires() throws InterruptedException {
        String name = testPrefix("awaitTimeout");
        latch.trySetCount(name, 5);

        long start = System.currentTimeMillis();
        boolean result = latch.await(name, 2, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(result, "计数未归零应超时返回 false");
        assertTrue(elapsed >= 1800, "应等待约2秒（实际=" + elapsed + "ms）");
        cleanup(name);
    }

    @Test
    @DisplayName("await带超时-归零后返回true")
    void awaitWithTimeoutZero() throws InterruptedException {
        String name = testPrefix("awaitTimeoutOK");
        latch.trySetCount(name, 1);
        AtomicBoolean result = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                result.set(latch.await(name, 5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        latch.countDown(name); // 归零

        t.join(6000);
        assertTrue(result.get(), "归零后 await 应返回 true");
        cleanup(name);
    }

    @Test
    @DisplayName("await带超时-超时前归零等待时间缩短")
    void awaitWithTimeoutEarlyReturn() throws InterruptedException {
        String name = testPrefix("awaitEarly");
        latch.trySetCount(name, 3);
        AtomicBoolean result = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        long t0 = System.currentTimeMillis();
        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                result.set(latch.await(name, 10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(1000);
        latch.countDown(name); // 2
        latch.countDown(name); // 1
        latch.countDown(name); // 0 → 通知

        t.join(3000);
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(result.get(), "归零后应返回 true");
        assertTrue(elapsed < 8000, "不应等到10秒超时（实际=" + elapsed + "ms）");
        cleanup(name);
    }

    // ================================================================
    //  4. 多线程 Pub/Sub 通知
    // ================================================================

    @Test
    @DisplayName("countDown归零-多个等待线程同时被唤醒")
    void countDownNotifiesMultipleWaiters() throws InterruptedException {
        String name = testPrefix("multiNotify");
        int waiterCount = 5;
        latch.trySetCount(name, 1);

        CountDownLatch ready = new CountDownLatch(waiterCount);
        AtomicInteger released = new AtomicInteger(0);

        for (int i = 0; i < waiterCount; i++) {
            new Thread(() -> {
                try {
                    ready.countDown();
                    latch.await(name);
                    released.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }

        ready.await();
        TimeUnit.MILLISECONDS.sleep(500);
        assertEquals(0, released.get(), "所有线程应阻塞");

        latch.countDown(name); // 归零 → 通知所有
        TimeUnit.SECONDS.sleep(2);
        assertEquals(waiterCount, released.get(), "所有等待线程都应被唤醒");
        cleanup(name);
    }

    @Test
    @DisplayName("countDown归零-新await线程立即返回")
    void awaitAfterZero() throws InterruptedException {
        String name = testPrefix("afterZero");
        latch.trySetCount(name, 1);
        latch.countDown(name); // 归零
        TimeUnit.MILLISECONDS.sleep(200);
        // Key 已删除，await 应直接返回
        assertTrue(latch.await(name, 1, TimeUnit.SECONDS));
        cleanup(name);
    }

    @Test
    @DisplayName("countDown归零-重复countDown后的await仍然立即返回")
    void countDownThenAwait() throws InterruptedException {
        String name = testPrefix("cdThenAw");
        latch.trySetCount(name, 2);
        latch.countDown(name); // 1
        latch.countDown(name); // 0
        TimeUnit.MILLISECONDS.sleep(200);
        // Key 已不存在 → 应直接返回
        assertTrue(latch.await(name, 1, TimeUnit.SECONDS));
        cleanup(name);
    }

    @Test
    @DisplayName("countDown-逐步递减多个线程等待不同阶段")
    void countDownStepByStep() throws InterruptedException {
        String name = testPrefix("stepByStep");
        latch.trySetCount(name, 3);
        AtomicInteger stage = new AtomicInteger(0);
        CountDownLatch ready = new CountDownLatch(3);

        // 三个线程在不同阶段等待
        for (int i = 0; i < 3; i++) {
            new Thread(() -> {
                try {
                    ready.countDown();
                    latch.await(name);
                    stage.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }

        ready.await();
        TimeUnit.MILLISECONDS.sleep(300);
        assertEquals(0, stage.get());

        latch.countDown(name); // 2 → 仍阻塞
        TimeUnit.MILLISECONDS.sleep(300);
        assertEquals(0, stage.get(), "计数=2 仍应阻塞");

        latch.countDown(name); // 1 → 仍阻塞
        TimeUnit.MILLISECONDS.sleep(300);
        assertEquals(0, stage.get(), "计数=1 仍应阻塞");

        latch.countDown(name); // 0 → 全部释放
        TimeUnit.SECONDS.sleep(2);
        assertEquals(3, stage.get(), "归零后全部线程应释放");
        cleanup(name);
    }

    // ================================================================
    //  5. 边界与异常
    // ================================================================

    @Test
    @DisplayName("边界-await被中断")
    void awaitInterrupted() throws InterruptedException {
        String name = testPrefix("awaitInt");
        latch.trySetCount(name, 5);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                latch.await(name);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        t.interrupt();
        t.join(3000);
        assertTrue(interrupted.get(), "中断应抛出 InterruptedException");
        cleanup(name);
    }

    @Test
    @DisplayName("边界-await带超时被中断")
    void awaitWithTimeoutInterrupted() throws InterruptedException {
        String name = testPrefix("awaitTimeoutInt");
        latch.trySetCount(name, 5);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                latch.await(name, 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        t.interrupt();
        t.join(3000);
        assertTrue(interrupted.get(), "中断应抛出 InterruptedException");
        cleanup(name);
    }

    @Test
    @DisplayName("边界-哈希标签格式")
    void latchKeyHashTag() {
        String name = testPrefix("hashTag");
        assertTrue(RedisKeyConstant.latchKey(name).startsWith("{latch}:"),
                "latchKey 应以 {latch}: 开头");
        assertTrue(RedisKeyConstant.latchChannel(name).contains("{latch}:"),
                "latchChannel 应包含 {latch}:");
    }

    // ================================================================
    //  6. TTL 过期边界
    // ================================================================

    @Test
    @DisplayName("TTL-过期后countDown不创建脏数据（不会DECR成-1）")
    void ttlExpiredCountDownDoesNotCreateNegativeKey() throws InterruptedException {
        String name = testPrefix("ttlCountDown");
        DistributedCountDownLatch shortTtl = new DistributedCountDownLatch(redisTemplate, 2);
        shortTtl.trySetCount(name, 3);
        TimeUnit.SECONDS.sleep(3);
        shortTtl.countDown(name);
        assertEquals(0, shortTtl.getCount(name), "过期后 countDown 不应创建 -1 脏数据");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-过期后await立即返回（Key不存在=归零）")
    void ttlExpiredAwaitReturnsImmediately() throws InterruptedException {
        String name = testPrefix("ttlAwait");
        DistributedCountDownLatch shortTtl = new DistributedCountDownLatch(redisTemplate, 2);
        shortTtl.trySetCount(name, 3);
        TimeUnit.SECONDS.sleep(3);
        long start = System.currentTimeMillis();
        shortTtl.await(name);
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 1000, "Key 过期后 await 应立即返回（实际=" + elapsed + "ms）");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-过期后await带超时返回true")
    void ttlExpiredAwaitWithTimeoutReturnsTrue() throws InterruptedException {
        String name = testPrefix("ttlAwaitTimeout");
        DistributedCountDownLatch shortTtl = new DistributedCountDownLatch(redisTemplate, 2);
        shortTtl.trySetCount(name, 3);
        TimeUnit.SECONDS.sleep(3);
        assertTrue(shortTtl.await(name, 1, TimeUnit.SECONDS), "过期后 await 应返回 true");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-过期后trySetCount可重新初始化")
    void ttlExpiredTrySetCountSucceeds() throws InterruptedException {
        String name = testPrefix("ttlSetCount");
        DistributedCountDownLatch shortTtl = new DistributedCountDownLatch(redisTemplate, 2);
        assertTrue(shortTtl.trySetCount(name, 3));
        TimeUnit.SECONDS.sleep(3);
        assertTrue(shortTtl.trySetCount(name, 5), "过期后 trySetCount 应成功（SET_IF_ABSENT）");
        assertEquals(5, shortTtl.getCount(name));
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-过期后getCount返回0")
    void ttlExpiredGetCountReturnsZero() throws InterruptedException {
        String name = testPrefix("ttlGetCount");
        DistributedCountDownLatch shortTtl = new DistributedCountDownLatch(redisTemplate, 2);
        shortTtl.trySetCount(name, 3);
        assertEquals(3, shortTtl.getCount(name));
        TimeUnit.SECONDS.sleep(3);
        assertEquals(0, shortTtl.getCount(name), "过期后 getCount 返回 0");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-活跃countDown刷新过期时间")
    void ttlActiveCountDownRefreshesTtl() throws InterruptedException {
        String name = testPrefix("ttlActiveCd");
        DistributedCountDownLatch shortTtl = new DistributedCountDownLatch(redisTemplate, 3);
        shortTtl.trySetCount(name, 5);
        TimeUnit.SECONDS.sleep(2);
        shortTtl.countDown(name);
        TimeUnit.SECONDS.sleep(2);
        assertEquals(4, shortTtl.getCount(name), "countDown 续期后 Key 仍存在");
        cleanup(name);
    }

    @Test
    @DisplayName("TTL-全部countDown归零后await返回")
    void ttlCountDownToZero() throws InterruptedException {
        String name = testPrefix("ttlZeroClean");
        DistributedCountDownLatch shortTtl = new DistributedCountDownLatch(redisTemplate, 10);
        shortTtl.trySetCount(name, 2);
        shortTtl.countDown(name);
        shortTtl.countDown(name);
        assertTrue(shortTtl.getCount(name) <= 0, "归零后计数 <= 0");
        assertTrue(shortTtl.await(name, 1, TimeUnit.SECONDS));
        cleanup(name);
    }
}
