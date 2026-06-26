package com.xiandou.redis.core;

import com.xiandou.redis.constant.RedisKeyConstant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DistributedReadWriteLock 全面单元测试。
 * 使用 Mockito 模拟 RedisTemplate，不启动真实 Redis。
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class DistributedReadWriteLockTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    private DistributedReadWriteLock rwLock;

    @BeforeEach
    void setUp() {
        // 默认 stubs：所有 execute 调用默认返回 null
        // 写锁/解锁使用 4 参数（script, keys, arg1, arg2）
        // 读锁获取使用 5 参数（script, keys, arg1, arg2, arg3）
        lenient().doReturn(null).when(redisTemplate).execute(
                any(RedisScript.class), anyList());
        lenient().doReturn(null).when(redisTemplate).execute(
                any(RedisScript.class), anyList(), any());
        lenient().doReturn(null).when(redisTemplate).execute(
                any(RedisScript.class), anyList(), any(), any());
        lenient().doReturn(null).when(redisTemplate).execute(
                any(RedisScript.class), anyList(), any(), any(), any());

        rwLock = new DistributedReadWriteLock(redisTemplate);
    }

    /**
     * 获取当前线程的写锁 entryName = write_lock:clientId:threadId
     */
    private String getWriteEntryName() {
        return "write_lock:" + rwLock.getClientId() + ":" + Thread.currentThread().getId();
    }

    /**
     * 获取当前线程的读锁 entryName = read_lock:clientId:threadId
     */
    private String getReadEntryName() {
        return "read_lock:" + rwLock.getClientId() + ":" + Thread.currentThread().getId();
    }

    // ========================================================================
    // 1. 写锁互斥：T1写锁→T2写锁失败（mock 第2次返回 TTL）
    // ========================================================================

    @Test
    @DisplayName("写锁互斥：连续调用两次，第一次成功第二次失败")
    void writeLock_mutex_firstSuccess_secondFail() {
        // 第一次返回 null（成功），第二次返回 TTL（失败）
        doReturn(null).doReturn(5000L)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean first = rwLock.writeLock("testLock", 30000, TimeUnit.MILLISECONDS);
        boolean second = rwLock.writeLock("testLock", 30000, TimeUnit.MILLISECONDS);

        assertThat(first).isTrue();
        assertThat(second).isFalse();
    }

    // ========================================================================
    // 2. 读锁共享：T1读锁→T2读锁成功（都返回 null）
    // ========================================================================

    @Test
    @DisplayName("读锁共享：连续调用两次都成功")
    void readLock_shared_bothSuccess() {
        // 读锁使用 5 参数 execute（script, keys, leaseMs, readEntry, writeEntry）
        // 两次都返回 null
        doReturn(null).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        boolean first = rwLock.readLock("sharedLock", 30000, TimeUnit.MILLISECONDS);
        boolean second = rwLock.readLock("sharedLock", 30000, TimeUnit.MILLISECONDS);

        assertThat(first).isTrue();
        assertThat(second).isTrue();
    }

    // ========================================================================
    // 3. 写锁阻塞读锁：T1写锁→T2读锁失败
    // ========================================================================

    @Test
    @DisplayName("写锁阻塞读锁：写锁已持有时读锁获取失败")
    void writeLock_blocks_readLock() {
        // 写锁返回 null（成功）
        doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());
        // 读锁返回 TTL（失败）
        doReturn(5000L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        boolean writeOk = rwLock.writeLock("mutexLock", 30000, TimeUnit.MILLISECONDS);
        boolean readFail = rwLock.readLock("mutexLock", 30000, TimeUnit.MILLISECONDS);

        assertThat(writeOk).isTrue();
        assertThat(readFail).isFalse();
    }

    // ========================================================================
    // 4. 读锁阻塞写锁：T1读锁→T2写锁失败
    // ========================================================================

    @Test
    @DisplayName("读锁阻塞写锁：读锁已持有时写锁获取失败")
    void readLock_blocks_writeLock() {
        // 读锁返回 null（成功）
        doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());
        // 写锁返回 TTL（失败）
        doReturn(5000L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean readOk = rwLock.readLock("mutexLock", 30000, TimeUnit.MILLISECONDS);
        boolean writeFail = rwLock.writeLock("mutexLock", 30000, TimeUnit.MILLISECONDS);

        assertThat(readOk).isTrue();
        assertThat(writeFail).isFalse();
    }

    // ========================================================================
    // 5. 写锁可重入
    // ========================================================================

    @Test
    @DisplayName("写锁可重入：连续获取写锁都成功")
    void writeLock_reentrant() {
        doReturn(null).doReturn(null).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        assertThat(rwLock.writeLock("reentrant", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(rwLock.writeLock("reentrant", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(rwLock.writeLock("reentrant", 30000, TimeUnit.MILLISECONDS)).isTrue();
    }

    // ========================================================================
    // 6. 读锁可重入
    // ========================================================================

    @Test
    @DisplayName("读锁可重入：连续获取读锁都成功")
    void readLock_reentrant() {
        doReturn(null).doReturn(null).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        assertThat(rwLock.readLock("reentrant", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(rwLock.readLock("reentrant", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(rwLock.readLock("reentrant", 30000, TimeUnit.MILLISECONDS)).isTrue();
    }

    // ========================================================================
    // 7. 锁降级（写→读）
    // ========================================================================

    @Test
    @DisplayName("锁降级：持有写锁后可获取读锁")
    void lock_downgrade_writeToRead() {
        // 调用顺序：writeLock(4-arg) → readLock(5-arg) → unlockWrite(4-arg) → unlockRead(4-arg)
        // 4-arg 顺序：第1次 null（writeLock），第2次 1L（unlockWrite），第3次 1L（unlockRead）
        doReturn(null).doReturn(1L).doReturn(1L)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());
        // 读锁（5 参数）返回 null（降级成功）
        doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        // 获取写锁
        assertThat(rwLock.writeLock("downgradeLock", 30000, TimeUnit.MILLISECONDS)).isTrue();
        // 读锁降级
        assertThat(rwLock.readLock("downgradeLock", 30000, TimeUnit.MILLISECONDS)).isTrue();

        // 释放写锁
        assertThat(rwLock.unlockWrite("downgradeLock")).isTrue();
        // 释放读锁
        assertThat(rwLock.unlockRead("downgradeLock")).isTrue();
    }

    // ========================================================================
    // 8. 非法解锁验证 - unlockWrite 非持有者
    // ========================================================================

    @Test
    @DisplayName("非法解锁写锁：非持有者调用 unlockWrite 返回 false")
    void unlockWrite_illegal_notOwner() {
        // 默认 stub 返回 null（非法解锁）
        boolean result = rwLock.unlockWrite("notMyLock");
        assertThat(result).isFalse();
    }

    // ========================================================================
    // 9. Lua 脚本结构验证
    // ========================================================================

    @Test
    @DisplayName("写锁获取脚本包含 hset/hincrby/pexpire/hexists/pttl")
    void writeLockScript_containsRequiredCommands() throws Exception {
        java.lang.reflect.Field field = DistributedReadWriteLock.class
                .getDeclaredField("WRITE_LOCK_SCRIPT");
        field.setAccessible(true);
        RedisScript<Long> script = (RedisScript<Long>) field.get(null);
        String s = script.getScriptAsString();
        assertThat(s).contains("hset").contains("hincrby")
                .contains("pexpire").contains("hexists").contains("pttl");
    }

    @Test
    @DisplayName("读锁获取脚本包含 hget/hset/hincrby/hexists/pttl")
    void readLockScript_containsRequiredCommands() throws Exception {
        java.lang.reflect.Field field = DistributedReadWriteLock.class
                .getDeclaredField("READ_LOCK_SCRIPT");
        field.setAccessible(true);
        RedisScript<Long> script = (RedisScript<Long>) field.get(null);
        String s = script.getScriptAsString();
        assertThat(s).contains("hget").contains("hset").contains("hincrby")
                .contains("hexists").contains("pttl");
    }

    @Test
    @DisplayName("写锁释放脚本包含 hdel/hlen/hset/del/pexpire")
    void writeUnlockScript_containsRequiredCommands() throws Exception {
        java.lang.reflect.Field field = DistributedReadWriteLock.class
                .getDeclaredField("WRITE_UNLOCK_SCRIPT");
        field.setAccessible(true);
        RedisScript<Long> script = (RedisScript<Long>) field.get(null);
        String s = script.getScriptAsString();
        assertThat(s).contains("hdel").contains("hlen").contains("hset")
                .contains("del").contains("pexpire");
    }

    @Test
    @DisplayName("读锁释放脚本包含 hdel/hlen/del/pexpire")
    void readUnlockScript_containsRequiredCommands() throws Exception {
        java.lang.reflect.Field field = DistributedReadWriteLock.class
                .getDeclaredField("READ_UNLOCK_SCRIPT");
        field.setAccessible(true);
        RedisScript<Long> script = (RedisScript<Long>) field.get(null);
        String s = script.getScriptAsString();
        assertThat(s).contains("hdel").contains("hlen")
                .contains("del").contains("pexpire");
    }

    // ========================================================================
    // 10. 非法解锁验证 - unlockRead 非持有者
    // ========================================================================

    @Test
    @DisplayName("非法解锁读锁：非持有者调用 unlockRead 返回 false")
    void unlockRead_illegal_notOwner() {
        // 默认 stub 返回 null（非法解锁）
        boolean result = rwLock.unlockRead("notMyLock");
        assertThat(result).isFalse();
    }

    // ========================================================================
    // 11. 读锁释放后写锁可获取
    // ========================================================================

    @Test
    @DisplayName("读锁释放后写锁可获取：读锁→解锁→写锁成功")
    void readLock_unlock_then_writeLock_success() {
        // 读锁（5 参数）返回 null（成功）
        doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());
        // 读锁解锁（4 参数）返回 1L（成功）；写锁（4 参数）后续返回 null
        doReturn(1L).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        assertThat(rwLock.readLock("cycleLock", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(rwLock.unlockRead("cycleLock")).isTrue();
        assertThat(rwLock.writeLock("cycleLock", 30000, TimeUnit.MILLISECONDS)).isTrue();
    }

    // ========================================================================
    // 12. 写锁释放后读锁可获取
    // ========================================================================

    @Test
    @DisplayName("写锁释放后读锁可获取：写锁→解锁→读锁成功")
    void writeLock_unlock_then_readLock_success() {
        // 调用顺序：writeLock(4-arg) → unlockWrite(4-arg) → readLock(5-arg)
        // 4-arg 顺序：第1次 null（writeLock），第2次 1L（unlockWrite）
        doReturn(null).doReturn(1L)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());
        // 读锁（5 参数）返回 null（成功）
        doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        assertThat(rwLock.writeLock("cycleLock", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(rwLock.unlockWrite("cycleLock")).isTrue();
        assertThat(rwLock.readLock("cycleLock", 30000, TimeUnit.MILLISECONDS)).isTrue();
    }

    // ========================================================================
    // 13. 不同锁名称互不干扰
    // ========================================================================

    @Test
    @DisplayName("不同锁名称使用不同的 Redis key")
    void differentNames_differentKeys() {
        ArgumentCaptor<List<String>> keysCaptor = ArgumentCaptor.forClass(List.class);

        rwLock.writeLock("alpha", 30000, TimeUnit.MILLISECONDS);
        rwLock.writeLock("beta", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate, times(2)).execute(
                any(RedisScript.class), keysCaptor.capture(), any(), any());
        List<List<String>> allKeys = keysCaptor.getAllValues();
        assertThat(allKeys.get(0).get(0)).isEqualTo("{rwlock}:alpha");
        assertThat(allKeys.get(1).get(0)).isEqualTo("{rwlock}:beta");
    }

    // ========================================================================
    // 14. Redis 超时异常安全
    // ========================================================================

    @Test
    @DisplayName("Redis 异常时 writeLock 返回 false")
    void redisException_writeLock_returnsFalse() {
        doThrow(new RuntimeException("Redis connection refused"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = rwLock.writeLock("errKey", 30000, TimeUnit.MILLISECONDS);
        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("Redis 异常时 readLock 返回 false")
    void redisException_readLock_returnsFalse() {
        doThrow(new RuntimeException("Redis connection refused"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        boolean result = rwLock.readLock("errKey", 30000, TimeUnit.MILLISECONDS);
        assertThat(result).isFalse();
    }

    // ========================================================================
    // 15. entryName 格式验证
    // ========================================================================

    @Test
    @DisplayName("写锁 entryName 格式验证：write_lock:clientId:threadId")
    void writeLock_entryName_format() {
        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);

        rwLock.writeLock("formatTest", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(
                any(RedisScript.class), anyList(), argsCaptor.capture());
        Object[] args = argsCaptor.getValue();
        String entryName = (String) args[1];
        assertThat(entryName).startsWith("write_lock:");
        assertThat(entryName).contains(":");
        assertThat(entryName).startsWith("write_lock:" + rwLock.getClientId());
    }

    @Test
    @DisplayName("读锁 entryName 格式验证：read_lock:clientId:threadId")
    void readLock_entryName_format() {
        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);

        rwLock.readLock("formatTest", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(
                any(RedisScript.class), anyList(), argsCaptor.capture());
        Object[] args = argsCaptor.getValue();
        String entryName = (String) args[1];
        assertThat(entryName).startsWith("read_lock:");
        assertThat(entryName).contains(":");
        assertThat(entryName).startsWith("read_lock:" + rwLock.getClientId());
    }

    // ========================================================================
    // 16. 读写锁 key 前缀验证
    // ========================================================================

    @Test
    @DisplayName("读写锁 Key 使用 {rwlock}: 哈希标签前缀")
    void rwlockKey_usesHashTag() {
        ArgumentCaptor<List<String>> keysCaptor = ArgumentCaptor.forClass(List.class);

        rwLock.writeLock("tagTest", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(
                any(RedisScript.class), keysCaptor.capture(), any(), any());
        assertThat(keysCaptor.getValue().get(0)).isEqualTo("{rwlock}:tagTest");
    }

    // ========================================================================
    // 17. tryWriteLock 带等待超时
    // ========================================================================

    @Test
    @DisplayName("tryWriteLock 带等待超时：始终失败返回 false")
    void tryWriteLock_withTimeout_timeout() throws InterruptedException {
        // 始终返回 TTL（锁被占）
        doReturn(5000L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        long start = System.currentTimeMillis();
        boolean result = rwLock.tryWriteLock("timeoutLock", 30, 30000, TimeUnit.MILLISECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(result).isFalse();
        assertThat(elapsed).isLessThan(2000); // 应在短时间内超时返回
    }

    // ========================================================================
    // 18. tryReadLock 带等待超时
    // ========================================================================

    @Test
    @DisplayName("tryReadLock 带等待超时：始终失败返回 false")
    void tryReadLock_withTimeout_timeout() throws InterruptedException {
        // 始终返回 TTL（锁被占）
        doReturn(5000L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        long start = System.currentTimeMillis();
        boolean result = rwLock.tryReadLock("timeoutLock", 30, 30000, TimeUnit.MILLISECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(result).isFalse();
        assertThat(elapsed).isLessThan(2000); // 应在短时间内超时返回
    }

    // ========================================================================
    // 19. 写锁解锁返回 true（重入残留）
    // ========================================================================

    @Test
    @DisplayName("写锁解锁仍有重入：execute返回0L → unlockWrite返回true")
    void unlockWrite_reentrantRemaining() {
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        assertThat(rwLock.unlockWrite("reentrantKey")).isTrue();
    }

    // ========================================================================
    // 20. 读锁解锁返回 true（重入残留）
    // ========================================================================

    @Test
    @DisplayName("读锁解锁仍有重入：execute返回0L → unlockRead返回true")
    void unlockRead_reentrantRemaining() {
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        assertThat(rwLock.unlockRead("reentrantKey")).isTrue();
    }
}
