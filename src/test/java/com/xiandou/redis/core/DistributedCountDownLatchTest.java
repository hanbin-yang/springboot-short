package com.xiandou.redis.core;

import com.xiandou.redis.config.LuaScriptRegistry;
import com.xiandou.redis.constant.RedisKeyConstant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DistributedCountDownLatch 全面单元测试。
 * 使用 Mockito 模拟 RedisTemplate，不启动真实 Redis。
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class DistributedCountDownLatchTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private RedisConnectionFactory connectionFactory;

    @Mock
    private RedisConnection connection;

    @Mock
    private Subscription subscription;

    private DistributedCountDownLatch latch;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.getConnectionFactory()).thenReturn(connectionFactory);
        lenient().when(connectionFactory.getConnection()).thenReturn(connection);
        lenient().when(connection.getSubscription()).thenReturn(subscription);

        // Default stubs:
        // 3 参数 execute(RedisScript, List, Object...) — 对应 SET_IF_ABSENT
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());
        // 2 参数 execute(RedisScript, List) — 对应 LATCH_COUNTDOWN（无 vararg）
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList());

        latch = new DistributedCountDownLatch(redisTemplate);
    }

    // ========================================================================
    // 1. trySetCount 初始化
    // ========================================================================

    @Test
    @DisplayName("trySetCount 初始化成功：返回 true")
    void trySetCount_initSuccess() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = latch.trySetCount("test", 5);

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("trySetCount 已存在：返回 false")
    void trySetCount_alreadyExists() {
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = latch.trySetCount("test", 5);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("trySetCount 使用正确的脚本、key 和 count 参数")
    void trySetCount_usesCorrectParams() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        latch.trySetCount("myKey", 10);

        verify(redisTemplate).execute(
                eq(LuaScriptRegistry.SET_IF_ABSENT_SCRIPT),
                eq(List.of(RedisKeyConstant.latchKey("myKey"))),
                eq("10")
        );
    }

    // ========================================================================
    // 2. countDown
    // ========================================================================

    @Test
    @DisplayName("countDown 使用正确的脚本、key 和 channel 参数")
    void countDown_usesCorrectParams() {
        latch.countDown("myKey");

        verify(redisTemplate).execute(
                eq(LuaScriptRegistry.LATCH_COUNTDOWN_SCRIPT),
                eq(List.of(RedisKeyConstant.latchKey("myKey"), RedisKeyConstant.latchChannel("myKey")))
        );
    }

    @Test
    @DisplayName("countDown 时已为 0 或负数：安全幂等，不抛异常")
    void countDown_atZero_isIdempotent() {
        // decr 后返回 -1，仍安全执行
        doReturn(-1L).when(redisTemplate).execute(any(RedisScript.class), anyList());

        latch.countDown("zeroKey");

        verify(redisTemplate).execute(
                eq(LuaScriptRegistry.LATCH_COUNTDOWN_SCRIPT),
                anyList()
        );
    }

    // ========================================================================
    // 3. await 完成 / 超时
    // ========================================================================

    @Test
    @DisplayName("countDown 到 0 后 await 完成")
    void countDown_toZero_awaitCompletes() throws InterruptedException {
        // countDown 返回 0（decr 后值为 0）
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList());

        latch.countDown("test");

        // await 时 opsForValue().get 返回 "0"
        doReturn("0").when(valueOperations).get(anyString());

        latch.await("test");
        // 到达此处表示等待完成，未阻塞
    }

    @Test
    @DisplayName("await 超时返回 false")
    void await_timeout() throws InterruptedException {
        // 计数 > 0
        doReturn("5").when(valueOperations).get(anyString());

        long start = System.currentTimeMillis();
        boolean result = latch.await("test", 30, TimeUnit.MILLISECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(result).isFalse();
        // 应在短时间内超时返回（30ms 左右）
        assertThat(elapsed).isLessThan(5000);
    }

    // ========================================================================
    // 4. await 时计数已归零
    // ========================================================================

    @Test
    @DisplayName("await 时计数已为 0，直接返回 true")
    void await_whenCountZero_returnsTrue() throws InterruptedException {
        doReturn("0").when(valueOperations).get(anyString());

        boolean result = latch.await("test", 100, TimeUnit.MILLISECONDS);

        assertThat(result).isTrue();
        // 不应创建订阅
        verify(redisTemplate, never()).getConnectionFactory();
    }

    // ========================================================================
    // 5. getCount
    // ========================================================================

    @Test
    @DisplayName("getCount 读取当前计数")
    void getCount_readsValue() {
        doReturn("8").when(valueOperations).get(anyString());

        long count = latch.getCount("test");

        assertThat(count).isEqualTo(8L);
    }

    @Test
    @DisplayName("getCount key 不存在时返回 0")
    void getCount_keyNotExists() {
        doReturn(null).when(valueOperations).get(anyString());

        long count = latch.getCount("test");

        assertThat(count).isEqualTo(0L);
    }

    @Test
    @DisplayName("getCount 使用正确的 key")
    void getCount_usesCorrectKey() {
        latch.getCount("cntKey");

        verify(valueOperations).get(RedisKeyConstant.latchKey("cntKey"));
    }

    // ========================================================================
    // 6. await 未初始化场景
    // ========================================================================

    @Test
    @DisplayName("未 trySetCount 直接 await：key 不存在视为已归零")
    void await_withoutTrySetCount() throws InterruptedException {
        // key 不存在
        doReturn(null).when(valueOperations).get(anyString());

        latch.await("test");
        // 未阻塞，正常返回
    }

    // ========================================================================
    // 7. await 完成后再次 countDown（幂等）
    // ========================================================================

    @Test
    @DisplayName("await 完成后再次 countDown 无影响")
    void countDown_afterAwait_isIdempotent() throws InterruptedException {
        // 计数为 0
        doReturn("0").when(valueOperations).get(anyString());

        // await 直接返回
        latch.await("test");

        // 再次 countDown 不抛异常
        doReturn(-1L).when(redisTemplate).execute(any(RedisScript.class), anyList());
        latch.countDown("test");
    }

    // ========================================================================
    // 8. Lua 脚本结构验证
    // ========================================================================

    @Test
    @DisplayName("SET_IF_ABSENT_SCRIPT 包含 exists/set 命令")
    void luaScript_setIfAbsent_containsExistsAndSet() {
        String s = LuaScriptRegistry.SET_IF_ABSENT_SCRIPT.getScriptAsString();
        assertThat(s).contains("exists");
        assertThat(s).contains("set");
    }

    @Test
    @DisplayName("LATCH_COUNTDOWN_SCRIPT 包含 decr/publish 命令")
    void luaScript_latchCountdown_containsDecrAndPublish() {
        String s = LuaScriptRegistry.LATCH_COUNTDOWN_SCRIPT.getScriptAsString();
        assertThat(s).contains("decr");
        assertThat(s).contains("publish");
    }

    // ========================================================================
    // 9. trySetCount 负数抛异常
    // ========================================================================

    @Test
    @DisplayName("trySetCount 负数抛 IllegalArgumentException")
    void trySetCount_negative_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> latch.trySetCount("test", -1));
    }

    // ========================================================================
    // 10. countDown 验证频道发布
    // ========================================================================

    @Test
    @DisplayName("countDown 传递 2 个 keys（key + channel）")
    void countDown_usesTwoKeys() {
        latch.countDown("dualKey");

        verify(redisTemplate).execute(
                any(RedisScript.class),
                eq(List.of(RedisKeyConstant.latchKey("dualKey"), RedisKeyConstant.latchChannel("dualKey")))
        );
    }
}
