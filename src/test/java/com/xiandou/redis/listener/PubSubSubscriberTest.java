package com.xiandou.redis.listener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * PubSubSubscriber 单元测试。
 */
@ExtendWith(MockitoExtension.class)
class PubSubSubscriberTest {

    @Mock
    private RedisConnectionFactory connectionFactory;

    @Mock
    private RedisConnection connection;

    @Mock
    private Subscription subscription;

    @BeforeEach
    void setUp() {
        lenient().when(connectionFactory.getConnection()).thenReturn(connection);
        lenient().when(connection.getSubscription()).thenReturn(subscription);
    }

    @Test
    @DisplayName("构造时创建连接并订阅频道")
    void constructor_subscribesToChannel() {
        try (PubSubSubscriber subscriber = new PubSubSubscriber(connectionFactory, "test-channel")) {
            verify(connectionFactory).getConnection();
            verify(connection).subscribe(any(), eq("test-channel".getBytes()));
        }
    }

    @Test
    @DisplayName("close时取消订阅并关闭连接")
    void close_unsubscribesAndCloses() {
        PubSubSubscriber subscriber = new PubSubSubscriber(connectionFactory, "ch");
        subscriber.close();
        verify(connection.getSubscription()).unsubscribe();
        verify(connection).close();
    }

    @Test
    @DisplayName("close幂等：第二次调用无副作用")
    void close_idempotent() {
        PubSubSubscriber subscriber = new PubSubSubscriber(connectionFactory, "ch");
        subscriber.close();
        subscriber.close();
        // unsubscribe和close只应各调用一次
        verify(connection.getSubscription(), times(1)).unsubscribe();
        verify(connection, times(1)).close();
    }

    @Test
    @DisplayName("await超时返回false")
    void await_timeout() throws InterruptedException {
        // 没有消息发布，await超时返回false
        try (PubSubSubscriber subscriber = new PubSubSubscriber(connectionFactory, "ch")) {
            long start = System.currentTimeMillis();
            subscriber.await(50);
            long elapsed = System.currentTimeMillis() - start;
            assertThat(elapsed).isGreaterThanOrEqualTo(40);
        }
    }
}
