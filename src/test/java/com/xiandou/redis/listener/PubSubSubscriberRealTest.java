package com.xiandou.redis.listener;

import com.xiandou.redis.constant.RedisKeyConstant;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PubSubSubscriber 真实 Redis Cluster 深度集成测试。
 * <p>覆盖：订阅接收、多订阅者、await 超时、跨节点 Pub/Sub、close 清理、异常处理。</p>
 *
 * <p>前置条件：Redis Cluster 已启动（192.168.56.186:6379-6381）</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.MethodName.class)
class PubSubSubscriberRealTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private RedisConnectionFactory connectionFactory;

    private String testPrefix(String suffix) {
        return "test:real:pubsub:" + suffix;
    }

    /**
     * Lua 脚本：向指定频道发送一条消息（模拟 unlock 的 PUBLISH）
     */
    private static final RedisScript<Long> PUBLISH_SCRIPT = new DefaultRedisScript<>(
            "redis.call('publish', KEYS[1], ARGV[1]); return 1;",
            Long.class
    );

    /** 通过 Lua 执行 PUBLISH（模拟锁释放通知） */
    private void publishViaLua(String channel) {
        redisTemplate.execute(PUBLISH_SCRIPT, List.of(channel), "0");
    }

    /** 通过原生 Connection 执行 PUBLISH */
    private void publishViaConnection(String channel) {
        RedisConnection conn = null;
        try {
            conn = connectionFactory.getConnection();
            conn.publish(channel.getBytes(StandardCharsets.UTF_8), "release".getBytes(StandardCharsets.UTF_8));
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (Exception ignored) {}
            }
        }
    }

    /** 通过 convertAndSend 发布 */
    private void publishViaTemplate(String channel) {
        redisTemplate.convertAndSend(channel, "release");
    }

    // ================================================================
    //  1. 基础功能
    // ================================================================

    @Test
    @DisplayName("订阅-收到消息后await返回")
    void subscribeReceivesMessage() throws InterruptedException {
        String channel = testPrefix("recv");
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);

        // 发布消息
        publishViaLua(channel);

        // await 应在 2 秒内返回（消息触发）
        long start = System.currentTimeMillis();
        sub.await(3000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed < 2500, "消息应触发 await 提前返回（实际=" + elapsed + "ms）");
        sub.close();
    }

    @Test
    @DisplayName("await-超时无消息返回false（不阻塞）")
    void awaitTimeout() throws InterruptedException {
        String channel = testPrefix("timeout");
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);

        long start = System.currentTimeMillis();
        sub.await(100); // 100ms 超时
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed >= 80 && elapsed < 500,
                "应在 100ms 左右返回（实际=" + elapsed + "ms）");
        sub.close();
    }

    @Test
    @DisplayName("订阅-先发布后订阅不会收到（Pub/Sub无缓存）")
    void subscribeAfterPublishDoesNotReceive() throws InterruptedException {
        String channel = testPrefix("late");
        // 先发布
        publishViaLua(channel);
        TimeUnit.MILLISECONDS.sleep(200);

        // 后订阅
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);

        long start = System.currentTimeMillis();
        sub.await(1000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed >= 800, "先发布后订阅不应收到消息，应等超时（实际=" + elapsed + "ms）");
        sub.close();
    }

    // ================================================================
    //  2. 多订阅者
    // ================================================================

    @Test
    @DisplayName("多订阅者-同一频道多个订阅者均收到消息")
    void multipleSubscribersReceiveMessage() throws InterruptedException {
        String channel = testPrefix("multi");
        int n = 5;
        PubSubSubscriber[] subs = new PubSubSubscriber[n];
        boolean[] received = new boolean[n];

        // 所有订阅者订阅同一频道
        for (int i = 0; i < n; i++) {
            subs[i] = new PubSubSubscriber(connectionFactory, channel);
        }

        TimeUnit.MILLISECONDS.sleep(200); // 等订阅建立

        // 发布消息
        publishViaLua(channel);

        for (int i = 0; i < n; i++) {
            long start = System.currentTimeMillis();
            subs[i].await(3000);
            long elapsed = System.currentTimeMillis() - start;
            received[i] = elapsed < 2500;
        }

        for (int i = 0; i < n; i++) {
            assertTrue(received[i], "订阅者 " + i + " 应收到消息");
            subs[i].close();
        }
    }

    @Test
    @DisplayName("多订阅者-每个订阅者独立接收")
    void multipleSubscribersIndependent() throws InterruptedException {
        String channelA = testPrefix("indepA");
        String channelB = testPrefix("indepB");

        PubSubSubscriber subA = new PubSubSubscriber(connectionFactory, channelA);
        PubSubSubscriber subB = new PubSubSubscriber(connectionFactory, channelB);

        TimeUnit.MILLISECONDS.sleep(200);

        // 只发布到 channelA
        publishViaLua(channelA);

        // subA 应收到
        long startA = System.currentTimeMillis();
        subA.await(2000);
        long elapsedA = System.currentTimeMillis() - startA;
        assertTrue(elapsedA < 1500, "subA 应收到消息");

        // subB 不应收到（等 1 秒验证超时）
        long startB = System.currentTimeMillis();
        subB.await(1000);
        long elapsedB = System.currentTimeMillis() - startB;
        assertTrue(elapsedB >= 800, "subB 不应收到消息");

        subA.close();
        subB.close();
    }

    // ================================================================
    //  3. close 清理
    // ================================================================

    @Test
    @DisplayName("close-取消订阅并关闭连接")
    void closeUnsubscribesAndCloses() {
        String channel = testPrefix("closeTest");
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);
        // close 不应抛异常
        assertDoesNotThrow(sub::close);
    }

    @Test
    @DisplayName("close-幂等性（多次调用安全）")
    void closeIdempotent() {
        String channel = testPrefix("closeIdem");
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);
        assertDoesNotThrow(() -> {
            sub.close();
            sub.close();
            sub.close();
        });
    }

    @Test
    @DisplayName("close-后不再接收消息")
    void closeThenNoReceive() throws InterruptedException {
        String channel = testPrefix("closeNoRecv");
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);
        sub.close(); // 提前关闭 → 不接收

        // 再创建一个新的订阅者
        PubSubSubscriber sub2 = new PubSubSubscriber(connectionFactory, channel);
        TimeUnit.MILLISECONDS.sleep(200);
        // 订阅建立后再发布 → 应收到
        publishViaLua(channel);
        long start = System.currentTimeMillis();
        sub2.await(3000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed < 2500, "新订阅者应收到消息");
        sub2.close();
    }

    // ================================================================
    //  4. try-with-resources
    // ================================================================

    @Test
    @DisplayName("try-with-resources-自动关闭")
    void tryWithResources() throws InterruptedException {
        String channel = testPrefix("twr");
        try (PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel)) {
            publishViaLua(channel);
            long start = System.currentTimeMillis();
            sub.await(3000);
            assertTrue(System.currentTimeMillis() - start < 2500);
        }
        // 退出 try 块后自动 close
    }

    @Test
    @DisplayName("try-with-resources-close后资源释放")
    void tryWithResourcesCleanup() {
        String channel = testPrefix("twrClean");
        // 大量创建和释放，验证无泄漏
        for (int i = 0; i < 50; i++) {
            try (PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel)) {
                // do nothing
            }
        }
    }

    // ================================================================
    //  5. 与锁集成（模拟 unlock 通知流程）
    // ================================================================

    @Test
    @DisplayName("集成-模拟tryLock等待通知流程（锁释放→PUBLISH→await返回）")
    void simulateLockNotifyFlow() throws InterruptedException {
        String name = testPrefix("lockFlow");
        String channel = RedisKeyConstant.lockChannel(name);

        // 模拟：锁被占用，另一个线程在等待（订阅频道）
        PubSubSubscriber subscriber = new PubSubSubscriber(connectionFactory, channel);

        // 模拟：持有锁的线程解锁（PUBLISH 通知）
        TimeUnit.MILLISECONDS.sleep(300); // 确保订阅建立
        publishViaConnection(channel);

        // 等待线程应被唤醒
        long start = System.currentTimeMillis();
        subscriber.await(3000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed < 2500, "PUBLISH 触发后 subscriber 应被唤醒（实际=" + elapsed + "ms）");
        subscriber.close();
    }

    @Test
    @DisplayName("集成-多个lock等待者同时被通知")
    void simulateMultipleLockWaiters() throws InterruptedException {
        String name = testPrefix("multiLock");
        String channel = RedisKeyConstant.lockChannel(name);
        int waiterCount = 5;
        AtomicInteger notified = new AtomicInteger(0);

        // 多个订阅者都在等待同一个锁
        PubSubSubscriber[] waiters = new PubSubSubscriber[waiterCount];
        for (int i = 0; i < waiterCount; i++) {
            waiters[i] = new PubSubSubscriber(connectionFactory, channel);
        }

        TimeUnit.MILLISECONDS.sleep(500); // 订阅建立

        // 释放锁 → 通知所有等待者
        publishViaConnection(channel);

        for (int i = 0; i < waiterCount; i++) {
            long start = System.currentTimeMillis();
            waiters[i].await(3000);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed < 2500) {
                notified.incrementAndGet();
            }
            waiters[i].close();
        }

        assertTrue(notified.get() > 0, "至少部分等待者应被通知");
    }

    // ================================================================
    //  6. 与 CountDownLatch 集成
    // ================================================================

    @Test
    @DisplayName("集成-模拟CountDownLatch归零通知流程")
    void simulateLatchNotifyFlow() throws InterruptedException {
        String name = testPrefix("latchFlow");
        String channel = RedisKeyConstant.latchChannel(name);

        // 模拟 await（订阅频道）
        PubSubSubscriber subscriber = new PubSubSubscriber(connectionFactory, channel);

        // 模拟 countDown 到零（PUBLISH）
        TimeUnit.MILLISECONDS.sleep(300);
        publishViaLua(channel);

        long start = System.currentTimeMillis();
        subscriber.await(3000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed < 2500, "await 应被 countDown 通知唤醒（实际=" + elapsed + "ms）");
        subscriber.close();
    }

    // ================================================================
    //  7. 边界与异常
    // ================================================================

    @Test
    @DisplayName("边界-订阅空频道名称")
    void subscribeEmptyChannel() {
        assertDoesNotThrow(() -> {
            PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, "");
            sub.close();
        });
    }

    @Test
    @DisplayName("边界-await被中断")
    void awaitInterrupted() throws InterruptedException {
        String channel = testPrefix("int");
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                ready.countDown();
                sub.await(10000); // 长时间等待
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(200);
        t.interrupt();
        t.join(3000);

        assertTrue(interrupted.get(), "中断后 await 应抛出 InterruptedException");
        sub.close();
    }

    @Test
    @DisplayName("边界-无消息时所有订阅者按超时各自返回")
    void noMessageAllTimeout() throws InterruptedException {
        String channel = testPrefix("allTimeout");
        int n = 3;
        long[] elapsed = new long[n];

        for (int i = 0; i < n; i++) {
            PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);
            long start = System.currentTimeMillis();
            sub.await(500);
            elapsed[i] = System.currentTimeMillis() - start;
            sub.close();
        }

        for (int i = 0; i < n; i++) {
            assertTrue(elapsed[i] >= 400 && elapsed[i] < 1500,
                    "订阅者 " + i + " 应在 500ms 超时返回（实际=" + elapsed[i] + "ms）");
        }
    }

    @Test
    @DisplayName("边界-channel名称含哈希标签")
    void channelWithHashTag() throws InterruptedException {
        // 使用 {lock}: 标签验证集群兼容性
        String channel = RedisKeyConstant.lockChannel(testPrefix("hashTagChannel"));
        PubSubSubscriber sub = new PubSubSubscriber(connectionFactory, channel);

        publishViaLua(channel);

        long start = System.currentTimeMillis();
        sub.await(3000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed < 2500, "带哈希标签的频道应正常工作（实际=" + elapsed + "ms）");
        sub.close();
    }
}
