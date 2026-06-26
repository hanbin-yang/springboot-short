package com.xiandou.redis.listener;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Pub/Sub 订阅助手。订阅 Redis 频道并在收到消息时释放等待线程。
 * 实现 AutoCloseable 确保退出时取消订阅，防止订阅泄漏。
 */
public class PubSubSubscriber implements AutoCloseable {
    private final RedisConnection connection;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean subscribed = true;

    public PubSubSubscriber(RedisConnectionFactory factory, String channel) {
        this.connection = factory.getConnection();
        byte[] channelBytes = channel.getBytes(StandardCharsets.UTF_8);
        this.connection.subscribe((message, pattern) -> latch.countDown(), channelBytes);
    }

    public void await(long timeoutMs) throws InterruptedException {
        latch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (subscribed) {
            try {
                if (connection.getSubscription() != null) {
                    connection.getSubscription().unsubscribe();
                }
            } catch (Exception ignored) {
            }
            try {
                connection.close();
            } catch (Exception ignored) {
            }
            subscribed = false;
        }
    }
}
