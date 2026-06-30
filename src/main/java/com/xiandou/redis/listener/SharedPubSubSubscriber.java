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
 * <p>模式匹配 {@code redistemplate_*} 覆盖锁和 Latch 的全部频道前缀。</p>
 */
public class SharedPubSubSubscriber implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SharedPubSubSubscriber.class);

    private final RedisConnectionFactory connectionFactory;
    private final ConcurrentHashMap<String, List<CompletableFuture<Void>>> pendingMap = new ConcurrentHashMap<>();
    private volatile RedisConnection connection;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public SharedPubSubSubscriber(RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * 等待频道消息（共享订阅，不会每次创建新连接）。
     *
     * @param channel  要订阅的频道
     * @param timeoutMs 超时毫秒（<=0 不等待）
     * @return true 收到消息，false 超时
     * @throws InterruptedException 等待被中断
     */
    public boolean await(String channel, long timeoutMs) throws InterruptedException {
        if (timeoutMs <= 0) {
            return false;
        }
        ensureSubscribed();

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

    /** 确保共享订阅连接已建立（仅首次真正创建连接） */
    private void ensureSubscribed() {
        if (initialized.compareAndSet(false, true)) {
            RedisConnection conn = connectionFactory.getConnection();
            conn.pSubscribe((message, pattern) -> {
                String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
                List<CompletableFuture<Void>> futures = pendingMap.remove(channel);
                if (futures != null) {
                    for (CompletableFuture<Void> f : futures) {
                        f.complete(null);
                    }
                }
            }, RedisKeyConstant.CHANNEL_PATTERN.getBytes(StandardCharsets.UTF_8));
            this.connection = conn;
            log.info("SharedPubSubSubscriber 已连接，模式={}", RedisKeyConstant.CHANNEL_PATTERN);
        }
    }

    @Override
    public void close() {
        RedisConnection conn = this.connection;
        if (conn != null) {
            try { conn.close(); } catch (Exception ignored) {}
        }
    }
}
