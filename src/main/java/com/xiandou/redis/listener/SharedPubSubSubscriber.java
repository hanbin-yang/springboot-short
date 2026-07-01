package com.xiandou.redis.listener;

import com.xiandou.redis.constant.RedisKeyConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 共享 Pub/Sub 订阅器 —— 参考 Redisson 设计。
 *
 * <p>所有锁/Latch 频道共享一条 PSUBSCRIBE 连接，不再每个等待线程创建独立订阅。
 * 当消息到达时，对应频道的所有等待线程被同时唤醒。</p>
 *
 * <p>内置自动重连：当连接因网络超时/防火墙/Redis 超时断开后，
 * 后台线程自动重建订阅（每次断开后等待 5 秒重试）。</p>
 */
public class SharedPubSubSubscriber implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SharedPubSubSubscriber.class);

    private static final long RECONNECT_DELAY_MS = 5000;

    private final RedisConnectionFactory connectionFactory;
    private final ConcurrentHashMap<String, List<CompletableFuture<Void>>> pendingMap = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean latchSignaled = new AtomicBoolean(false);
    private final CountDownLatch readyLatch = new CountDownLatch(1);
    private volatile boolean closed = false;
    private volatile RedisConnection connection;
    private Thread subscriberThread;

    public SharedPubSubSubscriber(RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * 等待频道消息（共享订阅，不会每次创建新连接）。
     *
     * @param channel   要订阅的频道
     * @param timeoutMs 超时毫秒（<=0 不等待）
     * @return true 收到消息，false 超时
     * @throws InterruptedException 等待被中断
     */
    public boolean await(String channel, long timeoutMs) throws InterruptedException {
        if (timeoutMs <= 0) {
            return false;
        }
        startSubscriberThread();
        // 等待订阅就绪（最多等 1 秒）
        readyLatch.await(1000, TimeUnit.MILLISECONDS);
        // 短暂等待确保 PSUBSCRIBE 命令已发送到 Redis 服务端
        Thread.sleep(50);

        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingMap.compute(channel, (k, list) -> {
            if (list == null) list = new CopyOnWriteArrayList<>();
            list.add(future);
            return list;
        });

        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (ExecutionException e) {
            log.warn("PubSub await 异常 channel={}", channel, e);
            return false;
        } finally {
            pendingMap.computeIfPresent(channel, (k, list) -> {
                list.remove(future);
                return list.isEmpty() ? null : list;
            });
        }
    }

    /** 启动后台订阅线程（仅首次启动） */
    private void startSubscriberThread() {
        if (!started.get() && started.compareAndSet(false, true)) {
            subscriberThread = new Thread(this::runSubscriptionLoop, "shared-pubsub");
            subscriberThread.setDaemon(true);
            subscriberThread.start();
        }
    }

    /** 订阅循环：pSubscribe → 断开 → 自动重连 */
    private void runSubscriptionLoop() {
        while (!closed) {
            try {
                RedisConnection conn = connectionFactory.getConnection();
                this.connection = conn;
                log.info("SharedPubSubSubscriber 连接成功，准备订阅，模式={}", RedisKeyConstant.CHANNEL_PATTERN);

                // 标记就绪（仅首次，重连时不再 countDown）
                if (latchSignaled.compareAndSet(false, true)) {
                    readyLatch.countDown();
                }
                conn.pSubscribe((message, pattern) -> {
                    String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
                    List<CompletableFuture<Void>> futures = pendingMap.remove(channel);
                    if (futures != null) {
                        for (CompletableFuture<Void> f : futures) {
                            f.complete(null);
                        }
                    }
                }, RedisKeyConstant.CHANNEL_PATTERN.getBytes(StandardCharsets.UTF_8));

                // 走到这里说明连接已断开（pSubscribe 返回）
                try { conn.close(); } catch (Exception ignored) {}
                this.connection = null;
                log.warn("SharedPubSubSubscriber 连接断开，{}ms 后重连...", RECONNECT_DELAY_MS);
            } catch (Exception e) {
                this.connection = null;
                if (!closed) {
                    log.warn("SharedPubSubSubscriber 异常，{}ms 后重连: {}", RECONNECT_DELAY_MS, e.toString());
                }
            }

            if (!closed) {
                try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) { break; }
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        RedisConnection conn = this.connection;
        if (conn != null) {
            try { conn.close(); } catch (Exception ignored) {}
            this.connection = null;
        }
        if (subscriberThread != null) {
            subscriberThread.interrupt();
        }
        log.info("SharedPubSubSubscriber 已关闭");
    }
}
