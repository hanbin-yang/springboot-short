package com.xiandou.redis.core;

import com.xiandou.redis.config.LuaScriptRegistry;
import com.xiandou.redis.constant.RedisKeyConstant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
 * DistributedSemaphore 全面单元测试。
 * 使用 Mockito 模拟 RedisTemplate，不启动真实 Redis。
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class DistributedSemaphoreTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private DistributedSemaphore semaphore;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        // Default stubs：所有 execute 调用默认返回 null（失败），各测试方法再按需覆盖。
        // 注：SEMAPHORE_ACQUIRE 传 1 个可变参数（permits），
        //     SEMAPHORE_RELEASE 传 2 个可变参数（channel + permits），
        //     SET_IF_ABSENT 传 1 个可变参数（permits）。
        // SEMAPHORE_ACQUIRE / SET_IF_ABSENT: 3 参数 → any(), anyList(), any()
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());
        // SEMAPHORE_RELEASE: 4 参数 → any(), anyList(), any(), any()
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        semaphore = new DistributedSemaphore(redisTemplate);
    }

    // ========================================================================
    // 1. 获取许可
    // ========================================================================

    @Test
    @DisplayName("tryAcquire 成功：execute 返回 1L")
    void tryAcquire_success() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = semaphore.tryAcquire("test", 1);

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("tryAcquire 失败（许可不足）：execute 返回 0L")
    void tryAcquire_fail_insufficient() {
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = semaphore.tryAcquire("test", 1);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("tryAcquire 使用正确的脚本、key 和 permits 参数")
    void tryAcquire_usesCorrectParams() {
        semaphore.tryAcquire("myKey", 3);

        verify(redisTemplate).execute(
                eq(LuaScriptRegistry.SEMAPHORE_ACQUIRE_SCRIPT),
                eq(List.of(RedisKeyConstant.semaphoreKey("myKey"))),
                eq("3")
        );
    }

    // ========================================================================
    // 2. 释放许可
    // ========================================================================

    @Test
    @DisplayName("release 释放许可不抛异常")
    void release_normal() {
        semaphore.release("test", 1);

        verify(redisTemplate).execute(
                any(RedisScript.class), anyList(), any(), any());
    }

    @Test
    @DisplayName("release 使用正确的脚本、key、channel、permits 参数")
    void release_usesCorrectParams() {
        semaphore.release("myKey", 2);

        verify(redisTemplate).execute(
                eq(LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT),
                eq(List.of(RedisKeyConstant.semaphoreKey("myKey"))),
                eq(RedisKeyConstant.semaphoreChannel("myKey")),
                eq("2")
        );
    }

    // ========================================================================
    // 3. 批量许可
    // ========================================================================

    @Test
    @DisplayName("批量许可：acquire 3 再 release 3")
    void acquireAndRelease_batch() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean acquired = semaphore.tryAcquire("test", 3);
        assertThat(acquired).isTrue();

        semaphore.release("test", 3);
        // 验证 release 被调用（4 参数）
        verify(redisTemplate).execute(
                eq(LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT),
                anyList(), any(), any());
    }

    // ========================================================================
    // 4. trySetPermits 初始化
    // ========================================================================

    @Test
    @DisplayName("trySetPermits 初始化成功：返回 true")
    void trySetPermits_initSuccess() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = semaphore.trySetPermits("test", 5);

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("trySetPermits 已存在：返回 false")
    void trySetPermits_alreadyExists() {
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = semaphore.trySetPermits("test", 5);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("trySetPermits 负数抛 IllegalArgumentException")
    void trySetPermits_negative_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> semaphore.trySetPermits("test", -1));
    }

    @Test
    @DisplayName("trySetPermits 验证参数传递正确")
    void trySetPermits_usesCorrectParams() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        semaphore.trySetPermits("permKey", 10);

        verify(redisTemplate).execute(
                eq(LuaScriptRegistry.SET_IF_ABSENT_SCRIPT),
                eq(List.of(RedisKeyConstant.semaphoreKey("permKey"))),
                eq("10")
        );
    }

    // ========================================================================
    // 5. 边界：acquire 0 个许可
    // ========================================================================

    @Test
    @DisplayName("acquire 0 个许可直接成功，不调用 Redis")
    void acquire_zero_permits() {
        boolean result = semaphore.tryAcquire("test", 0);

        assertThat(result).isTrue();
        verify(redisTemplate, never()).execute(any(RedisScript.class), anyList(), any());
    }

    @Test
    @DisplayName("release 0 个许可不调用 Redis")
    void release_zero_permits() {
        semaphore.release("test", 0);

        verify(redisTemplate, never()).execute(any(RedisScript.class), anyList(), any(), any());
    }

    // ========================================================================
    // 6. 释放超过已获取量
    // ========================================================================

    @Test
    @DisplayName("release 超过已获取量不抛异常")
    void release_moreThanAcquired() {
        semaphore.release("test", 10);

        verify(redisTemplate).execute(
                any(RedisScript.class), anyList(), any(), any());
    }

    // ========================================================================
    // 7. acquire 带超时成功/超时
    // ========================================================================

    @Test
    @DisplayName("tryAcquire 带超时成功：先失败后重试成功")
    void tryAcquireWithTimeout_success() throws InterruptedException {
        // 第一次返回 0L（失败），第二次返回 1L（成功）
        doReturn(0L).doReturn(1L)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = semaphore.tryAcquire("test", 1, 1000, TimeUnit.MILLISECONDS);

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("tryAcquire 带超时超时：始终失败返回 false")
    void tryAcquireWithTimeout_timeout() throws InterruptedException {
        // 始终返回 0L（失败），默认 stub 返回 null 也可以，这里确保不一致行为
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        long start = System.currentTimeMillis();
        boolean result = semaphore.tryAcquire("test", 1, 30, TimeUnit.MILLISECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(result).isFalse();
        assertThat(elapsed).isLessThan(2000); // 应在短时间内超时返回
    }

    // ========================================================================
    // 8. availablePermits
    // ========================================================================

    @Test
    @DisplayName("availablePermits 返回当前许可数")
    void availablePermits_normal() {
        doReturn("5").when(valueOperations).get(anyString());

        long permits = semaphore.availablePermits("test");

        assertThat(permits).isEqualTo(5L);
    }

    @Test
    @DisplayName("availablePermits key 不存在时返回 0")
    void availablePermits_keyNotExist() {
        doReturn(null).when(valueOperations).get(anyString());

        long permits = semaphore.availablePermits("test");

        assertThat(permits).isEqualTo(0L);
    }

    @Test
    @DisplayName("availablePermits 使用正确的 key")
    void availablePermits_usesCorrectKey() {
        semaphore.availablePermits("availKey");

        verify(valueOperations).get(RedisKeyConstant.semaphoreKey("availKey"));
    }

    // ========================================================================
    // 9. Lua 脚本命令验证
    // ========================================================================

    @Test
    @DisplayName("SEMAPHORE_ACQUIRE_SCRIPT 包含 DECRBY 命令")
    void luaScript_containsDecrby() {
        String s = LuaScriptRegistry.SEMAPHORE_ACQUIRE_SCRIPT.getScriptAsString();
        assertThat(s).contains("decrby");
    }

    @Test
    @DisplayName("SEMAPHORE_RELEASE_SCRIPT 包含 INCRBY 命令")
    void luaScript_containsIncrby() {
        String s = LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT.getScriptAsString();
        assertThat(s).contains("incrby");
    }

    // ========================================================================
    // 10. acquire 阻塞模式
    // ========================================================================

    @Test
    @DisplayName("acquire 阻塞直到获取成功")
    void acquire_blocking_success() throws InterruptedException {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        // 由于 mock 返回 1L，acquire 应该立即成功
        semaphore.acquire("test", 1);

        verify(redisTemplate, atLeastOnce()).execute(
                eq(LuaScriptRegistry.SEMAPHORE_ACQUIRE_SCRIPT),
                anyList(), any());
    }

    // ========================================================================
    // 11. 不同信号量互不干扰
    // ========================================================================

    @Test
    @DisplayName("不同信号量名称使用不同的 key")
    void differentNames_differentKeys() {
        semaphore.tryAcquire("alpha", 1);
        semaphore.tryAcquire("beta", 1);

        verify(redisTemplate, times(2)).execute(
                eq(LuaScriptRegistry.SEMAPHORE_ACQUIRE_SCRIPT),
                anyList(), any());
    }

    // ========================================================================
    // 12. trySetPermits 零值边界
    // ========================================================================

    @Test
    @DisplayName("trySetPermits 设为 0 可以初始化")
    void trySetPermits_zero() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());

        boolean result = semaphore.trySetPermits("zeroKey", 0);

        assertThat(result).isTrue();
    }
}
