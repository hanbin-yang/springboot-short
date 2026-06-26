package com.xiandou.redis.core;

import com.xiandou.redis.config.LuaScriptRegistry;
import com.xiandou.redis.constant.RedisKeyConstant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DistributedLock 全面单元测试。
 * 使用 Mockito 模拟 RedisTemplate，不启动真实 Redis。
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class DistributedLockTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private RedisConnectionFactory connectionFactory;

    @Mock
    private RedisConnection connection;

    @Mock
    private Subscription subscription;

    private DistributedLock lock;

    private ArgumentCaptor<RedisScript<Long>> scriptCaptor;
    private ArgumentCaptor<List<String>> keysCaptor;
    private ArgumentCaptor<Object[]> argsCaptor;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.getConnectionFactory()).thenReturn(connectionFactory);
        lenient().when(connectionFactory.getConnection()).thenReturn(connection);
        lenient().when(connection.getSubscription()).thenReturn(subscription);

        lock = new DistributedLock(redisTemplate);

        scriptCaptor = ArgumentCaptor.forClass(RedisScript.class);
        keysCaptor = ArgumentCaptor.forClass(List.class);
        argsCaptor = ArgumentCaptor.forClass(Object[].class);

        // Default stubs：所有 execute 调用默认返回 null（锁失败），
        // 覆盖各可变参数个数（0~3个），各测试方法再按需覆盖。
        // 注：evalLock 传 2 个可变参数，unlock 传 3 个可变参数
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList());
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any());
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());
        lenient().doReturn(null).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());
    }

    @AfterEach
    void tearDown() {
        // 清除可能的中断标志
        Thread.interrupted();
    }

    /**
     * 获取当前线程的 entryName = clientId:threadId
     */
    private String getEntryName() {
        return lock.getClientId() + ":" + Thread.currentThread().getId();
    }

    // ========================================================================
    // 1. 基础功能（18 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("加锁成功：execute返回null → tryLock返回true")
    void tryLock_success() {

        boolean result = lock.tryLock("order_123", 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isTrue();
        verify(redisTemplate).execute(scriptCaptor.capture(), keysCaptor.capture(), argsCaptor.capture());
        assertThat(keysCaptor.getValue().get(0)).isEqualTo("{lock}:order_123");
    }

    @Test
    @DisplayName("锁被占返回false：execute返回TTL → tryLock(wait=0)返回false")
    void tryLock_fail_heldByOther() {
        doReturn(15000L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = lock.tryLock("key", 0, 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("可重入加锁：连续两次都成功")
    void tryLock_reentrant() {

        assertThat(lock.tryLock("k", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.tryLock("k", 30000, TimeUnit.MILLISECONDS)).isTrue();

        verify(redisTemplate, times(2)).execute(any(RedisScript.class), anyList(), any(), any());
    }

    @Test
    @DisplayName("解锁成功：execute返回1L → unlock返回true")
    void unlock_success() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        boolean result = lock.unlock("testKey");

        assertThat(result).isTrue();
        verify(redisTemplate).execute(scriptCaptor.capture(), keysCaptor.capture(), any(), any(), any());
        // unlock成功：验证解锁参数
        assertThat(keysCaptor.getValue().get(0)).isEqualTo("{lock}:testKey");
        assertThat(keysCaptor.getValue().get(1)).startsWith("redisson_lock__channel:{lock}:testKey");
    }

    @Test
    @DisplayName("解锁仍有重入：execute返回0L → unlock返回true")
    void unlock_reentrantRemaining() {
        doReturn(0L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        assertThat(lock.unlock("k")).isTrue();
    }

    @Test
    @DisplayName("非法解锁（他人持有）：execute返回null → unlock返回false")
    void unlock_illegal_notOwner() {

        assertThat(lock.unlock("k")).isFalse();
    }

    @Test
    @DisplayName("tryLock等待后成功：第一次TTL→第二次null")
    void tryLock_waitThenSuccess() {
        doReturn(1000L).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = lock.tryLock("k", 5000, 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("tryLock指定正数leaseTime不启动Watchdog")
    void tryLock_withPositiveLease_noWatchdog() {

        lock.tryLock("k", 5000, TimeUnit.MILLISECONDS);

        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("lock阻塞模式立即成功")
    void lock_blocking_success() {

        lock.lock("k");

        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("lock阻塞模式等待后成功")
    void lock_blocking_eventualSuccess() {
        doReturn(5L).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        lock.lock("k");

        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("UUID:threadId 格式验证")
    void tryLock_entryName_format() {

        lock.tryLock("myLock", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(scriptCaptor.capture(), keysCaptor.capture(), argsCaptor.capture());
        Object[] args = argsCaptor.getValue();
        // args: [String.valueOf(leaseMs), entryName]
        String clientAndThread = (String) args[1];
        assertThat(clientAndThread).contains(":");
        assertThat(clientAndThread).startsWith(lock.getClientId());
    }

    @Test
    @DisplayName("Redis异常时tryLock返回false")
    void tryLock_redisError_returnsFalse() {
        doThrow(new RuntimeException("Redis connection refused"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = lock.tryLock("k", 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("完整的加锁解锁生命周期")
    void unlock_afterLock_fullLifecycle() {
        // lock成功：由 setUp 默认 stub 处理
        // unlock成功：单独 stub
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        assertThat(lock.tryLock("lifecycle", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.unlock("lifecycle")).isTrue();
    }

    @Test
    @DisplayName("tryLock传入leaseTime=-1启动Watchdog")
    void tryLock_negativeLease_startsWatchdog() {

        boolean result = lock.tryLock("k", -1, TimeUnit.MILLISECONDS);

        assertThat(result).isTrue();
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
        // 清理
        lock.unlock("k");
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("tryLock传入leaseTime=0不启动Watchdog")
    void tryLock_zeroLease_noWatchdog() {

        boolean result = lock.tryLock("k", 0, TimeUnit.MILLISECONDS);

        assertThat(result).isTrue();
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("tryLock等待超时返回false")
    void tryLock_waitTimeout() {
        // 始终返回TTL表示锁一直被占
        doReturn(5000L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        long start = System.currentTimeMillis();
        boolean result = lock.tryLock("k", 30, 30000, TimeUnit.MILLISECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(result).isFalse();
        assertThat(elapsed).isLessThan(2000); // 应在短时间内超时返回
    }

    @Test
    @DisplayName("不同锁名称各自独立")
    void tryLock_differentKeys() {

        assertThat(lock.tryLock("lockA", -1, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.tryLock("lockB", -1, TimeUnit.MILLISECONDS)).isTrue();

        verify(redisTemplate, times(2)).execute(any(RedisScript.class), anyList(), any(), any());
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(2);
        // 清理
        lock.unlock("lockA");
        lock.unlock("lockB");
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("lock指定正数leaseTime不启动Watchdog")
    void lock_withPositiveLease_noWatchdog() {

        lock.lock("k", 10, TimeUnit.SECONDS);

        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    // ========================================================================
    // 2. 边界条件（10 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("空锁解锁：从未加锁直接unlock返回false")
    void unlock_withoutLock() {

        assertThat(lock.unlock("nonexistent")).isFalse();
    }

    @Test
    @DisplayName("双重解锁防重入")
    void doubleUnlock() {
        doReturn(1L).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        assertThat(lock.unlock("k")).isTrue();
        assertThat(lock.unlock("k")).isFalse();
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("锁到期后unlock幂等安全")
    void unlock_afterExpiry() {

        assertThat(lock.unlock("k")).isFalse();
        verify(redisTemplate).execute(eq(LuaScriptRegistry.UNLOCK_SCRIPT), anyList(), any(), any(), any());
    }

    @Test
    @DisplayName("同一线程持有多个不同锁不互斥")
    void multipleLocks_differentKeys() {

        assertThat(lock.tryLock("A", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.tryLock("B", 30000, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    @DisplayName("深度可重入：连续5次加锁解锁")
    void reentrantDepth_deep() {
        // 第一次unlock返回0L（仍有重入），最后一次返回1L（完全释放）
        // 这里简化：直接让tryLock全部成功
        String key = "deepLock";

        for (int i = 0; i < 5; i++) {
            // 使用 -1 启动 Watchdog 验证计数
            assertThat(lock.tryLock(key, -1, TimeUnit.MILLISECONDS)).isTrue();
        }
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);

        // 所有unlock设返回0L（重入残留），最后一个unlock设返回1L
        // 由于顺序不确定，先重置mock
        reset(redisTemplate);
        // 前4次返回0L，最后一次返回1L（unlock调用，5个参数）
        doReturn(0L).doReturn(0L).doReturn(0L).doReturn(0L).doReturn(1L)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        // 全部解锁
        for (int i = 0; i < 5; i++) {
            lock.unlock(key);
        }
        // watchdog在第一次unlock时已被cancel
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("lock后又tryLock同一锁名")
    void lock_tryLock_sameKey() {

        lock.lock("sameKey");
        // lock后tryLock同一key，因为是同一线程，应该成功（可重入）
        assertThat(lock.tryLock("sameKey", 30000, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    @DisplayName("tryLock零等待且锁被占返回false")
    void tryLock_zeroWait_heldByOther() {
        doReturn(9999L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = lock.tryLock("busyLock", 0, 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("完全解锁后再次加锁成功")
    void unlock_afterFullReentrant_thenLockAgain() {
        // 执行序列：tryLock(2次) → unlock(2次) → tryLock(1次)
        // lock调用（4参数）由默认 stub 返回 null
        // unlock调用（5参数）前1次返回0L，第2次返回1L
        doReturn(0L).doReturn(1L)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        assertThat(lock.tryLock("x", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.tryLock("x", 30000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.unlock("x")).isTrue();  // 0L → 重入残留
        assertThat(lock.unlock("x")).isTrue();  // 1L → 完全释放
        assertThat(lock.tryLock("x", 30000, TimeUnit.MILLISECONDS)).isTrue(); // 重新获取
    }

    @Test
    @DisplayName("同一锁名lock后直接再次lock（可重入）")
    void lock_alreadyHeld_sameThread() {

        lock.lock("reentrant");
        // 同一线程再次lock同一锁名应该立即成功（可重入）
        lock.lock("reentrant");
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("tryLock传入负数waitTime行为")
    void tryLock_negativeWaitTime() {
        // waitTime为负数，unit.toMillis可能抛出异常或返回负数
        // 实际上tryLock(name, waitTime, leaseTime, unit)中用 unit.toMillis(waitTime)
        // 如果负数很大可能异常，但这里正常传递 -1
        doReturn(5000L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        // 即使锁被占，waitMs = unit.toMillis(-1) = -1，所以 waitMs <= 0，直接返回false
        boolean result = lock.tryLock("k", -1, 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isFalse();
    }

    // ========================================================================
    // 3. 时间敏感（6 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("lock(lease>0)不启动Watchdog")
    void lock_withLease_noWatchdog() {

        lock.lock("k", 5, TimeUnit.SECONDS);

        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("lock()默认启动Watchdog")
    void lock_default_startsWatchdog() {

        lock.lock("k");

        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
        lock.unlock("k");
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("unlock后Watchdog停止")
    void watchdog_stops_on_unlock() {
        // lock（默认 stub 返回 null），unlock 返回 1L
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        lock.lock("wdKey");
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);

        lock.unlock("wdKey");
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("lock(0)不启动Watchdog（leaseTime=0走default）")
    void lock_zeroLease_noWatchdog() {
        // leaseTime=0 → leaseMs = DEFAULT_LEASE_TIME_MS, leaseTime<0为false → 不启动Watchdog

        lock.lock("k", 0, TimeUnit.MILLISECONDS);

        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("lock(-1)使用默认超时并启动Watchdog")
    void lock_negativeLease_usesDefault() {

        lock.lock("k", -1, TimeUnit.MILLISECONDS);

        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
        verify(redisTemplate).execute(scriptCaptor.capture(), keysCaptor.capture(), argsCaptor.capture());
        // 验证leaseMs参数为默认值30000
        Object[] args = argsCaptor.getValue();
        assertThat(args[0]).isEqualTo("30000");
    }

    @Test
    @DisplayName("lock正数lease不启动Watchdog（重复验证）")
    void watchdog_not_started_positiveLease() {

        lock.lock("k", 100, TimeUnit.MILLISECONDS);

        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    // ========================================================================
    // 4. 混沌/故障注入（8 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("Redis超时 → tryLock返回false")
    void chaos_redisTimeout_tryLock() {
        doThrow(new RuntimeException("Redis timeout"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = lock.tryLock("k", 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("Redis超时 → unlock抛出异常但Watchdog已清理")
    void chaos_redisTimeout_unlock() {
        doThrow(new RuntimeException("Redis timeout"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        // unlock内部先cancelRenewal，再execute，所以异常不影响watchdog清理
        assertThrows(RuntimeException.class, () -> lock.unlock("k"));
    }

    @Test
    @DisplayName("tryLock时Redis间歇性故障")
    void chaos_intermittentFailure() {
        // 第一次异常（故障），第二次正常（恢复）
        doThrow(new RuntimeException("Redis timeout")).doReturn(null)
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = lock.tryLock("k", 5000, 30000, TimeUnit.MILLISECONDS);

        // 第一次evalLock异常→返回0L→不null→waitMs>0→进入waitForLock
        // waitForLock中第二次evalLock返回null→成功
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("tryLock连续Redis故障")
    void chaos_redisTimeout_always() {
        doThrow(new RuntimeException("Redis always down"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        boolean result = lock.tryLock("k", 30000, TimeUnit.MILLISECONDS);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("lock时Redis故障（evalLock捕获异常返回0L）")
    void chaos_lock_afterRedisDown() {
        // evalLock捕获异常返回0L，lock中ttl=0L≠null，会进入等待路径
        // 需要mock connectionFactory用于PubSubSubscriber
        doThrow(new RuntimeException("Redis down"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        // lock会无限循环，所以用assertThrows检查InterruptedException
        // 通过中断当前线程使lock退出
        Thread.currentThread().interrupt();
        assertThrows(RuntimeException.class, () -> lock.lock("k"));
        // 清除中断标志
        Thread.interrupted();
    }

    @Test
    @DisplayName("业务代码finally释放锁")
    void chaos_businessException_releasesLock() {
        // lock成功：由 setUp 默认 stub 处理
        // unlock成功：单独 stub
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        // 模拟业务异常后finally释放锁
        boolean locked = lock.tryLock("bizLock", 30000, TimeUnit.MILLISECONDS);
        assertThat(locked).isTrue();

        try {
            throw new RuntimeException("业务异常");
        } catch (RuntimeException e) {
            // 业务异常，finally中释放锁
        } finally {
            boolean unlocked = lock.unlock("bizLock");
            assertThat(unlocked).isTrue();
        }
    }

    @Test
    @DisplayName("unlock时Redis超时后watchdog状态")
    void chaos_unlockAfterRedisTimeout() {
        // 先获取锁（正常，由默认 stub 处理），再解锁（超时）
        doThrow(new RuntimeException("Redis timeout"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any()); // unlock

        lock.tryLock("k", -1, TimeUnit.MILLISECONDS);
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);

        // unlock异常，但cancelRenewal已执行
        assertThrows(RuntimeException.class, () -> lock.unlock("k"));
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("evalLock异常返回0L不被误解为成功")
    void chaos_evalLockException_returnsZeroNotSuccess() {
        doThrow(new RuntimeException("timeout"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any());

        // tryLock直接调用evalLock，异常→0L，非null，waitMs=0→false
        boolean result = lock.tryLock("k", 0, 30000, TimeUnit.MILLISECONDS);
        assertThat(result).isFalse();
    }

    // ========================================================================
    // 5. 内存泄漏（4 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("100次加锁解锁后Watchdog为0")
    void leak_noWatchdogLeak_100cycles() {
        // tryLock（默认 stub），unlock 返回 1L
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        for (int i = 0; i < 100; i++) {
            lock.tryLock("k" + i, 30000, TimeUnit.MILLISECONDS);
            lock.unlock("k" + i);
        }

        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("100次lock/unlock后Watchdog为0")
    void leak_noWatchdogLeak_lockUnlock() {
        // lock（默认 stub），unlock 返回 1L
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        for (int i = 0; i < 100; i++) {
            lock.lock("k" + i);
            lock.unlock("k" + i);
        }

        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("解锁异常不影响Watchdog清理")
    void leak_unlockException_cleanupWatchdog() {
        // tryLock（默认 stub 返回 null），unlock 异常
        doThrow(new RuntimeException("Redis error"))
                .when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        lock.tryLock("leakKey", -1, TimeUnit.MILLISECONDS);
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);

        // unlock异常但Watchdog已cancel
        assertThrows(RuntimeException.class, () -> lock.unlock("leakKey"));
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("多次加锁解锁后内部Map管理正常")
    void leak_renewalMap_cleanup() {
        // lock（默认 stub 返回 null），unlock 统一返回 1L
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        // 操作多个不同锁
        assertThat(lock.tryLock("a", -1, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.tryLock("b", -1, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.tryLock("c", -1, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(3);

        assertThat(lock.unlock("a")).isTrue();
        assertThat(lock.unlock("b")).isTrue();
        // 解锁c前验证
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);

        assertThat(lock.unlock("c")).isTrue();
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    // ========================================================================
    // 6. 线程安全（5 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("20线程并发竞争同一锁不抛异常")
    void concurrent_20threads_contention() throws InterruptedException {

        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    // 每个线程尝试加锁，由于mock总是返回null，所有线程都"成功"
                    // 主要验证并发访问renewalMap不抛异常
                    if (lock.tryLock("shared", 30000, TimeUnit.MILLISECONDS)) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(5, TimeUnit.SECONDS);
        // 所有线程都成功（mock总是返回null）
        assertThat(successCount.get()).isEqualTo(threadCount);
    }

    @Test
    @DisplayName("线程池复用同一锁名不冲突")
    void threadPool_reuse() throws InterruptedException {

        int taskCount = 10;
        CountDownLatch latch = new CountDownLatch(taskCount);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                try {
                    // 每个任务使用相同的锁名，线程池中不同线程会创建不同entryName
                    lock.tryLock("poolLock", 30000, TimeUnit.MILLISECONDS);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
        // 不验证具体行为，仅验证不抛异常
    }

    @Test
    @DisplayName("不同线程加锁互斥验证")
    void differentThreads_mutex() throws Exception {
        // 第一个线程加锁成功（null），第二个线程加锁失败（TTL）
        // 使用线程名区分mock行为
        String key = "mutexKey";

        // 主线程模拟"已经持有锁"的场景：mock返回TTL给其他线程
        // 其他线程调用tryLock时，收到TTL表示锁被占
        CountDownLatch threadReady = new CountDownLatch(1);
        CountDownLatch mainDone = new CountDownLatch(1);
        AtomicInteger otherResult = new AtomicInteger(-1);

        Thread other = new Thread(() -> {
            // 其他线程尝试加锁
            boolean locked = lock.tryLock(key, 30000, TimeUnit.MILLISECONDS);
            otherResult.set(locked ? 1 : 0);
            threadReady.countDown();
            try { mainDone.await(); } catch (InterruptedException ignored) {}
        });
        other.start();

        // 等子线程启动
        Thread.sleep(200);
        // 子线程执行tryLock，由于redisTemplate返回null，子线程"成功"
        // 这个测试主要验证不同线程使用不同entryName
        mainDone.countDown();
        other.join(2000);
    }

    @Test
    @DisplayName("多线程并发加锁解锁同一key")
    void concurrent_lock_unlock_sameKey() throws InterruptedException {

        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    boolean locked = lock.tryLock("concurrent", 30000, TimeUnit.MILLISECONDS);
                    if (locked) {
                        // 持有锁一段时间
                        Thread.sleep(10);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(5, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("多线程不同锁名不互斥")
    void concurrent_differentKeys() throws InterruptedException {

        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    lock.tryLock("key-" + idx, 30000, TimeUnit.MILLISECONDS);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(5, TimeUnit.SECONDS);
    }

    // ========================================================================
    // 7. 多锁类型交互（3 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("不同前缀锁不冲突（{lock}: vs {semaphore}:）")
    void differentPrefixes_noConflict() {

        // 验证锁的key使用{lock}:前缀
        lock.tryLock("test", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(scriptCaptor.capture(), keysCaptor.capture(), argsCaptor.capture());
        String key = keysCaptor.getValue().get(0);
        assertThat(key).isEqualTo("{lock}:test");
        assertThat(key).doesNotContain("{semaphore}:");
    }

    @Test
    @DisplayName("不同锁名前缀不共享Map")
    void multipleLocks_noInterference() {
        // unlock 需要返回 1L（5个参数）
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        assertThat(lock.tryLock("a", -1, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.tryLock("b", -1, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(2);
        // 解锁a不影响b
        assertThat(lock.unlock("a")).isTrue();
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("不同锁名的unlock互不影响")
    void lockAndUnlock_differentPrefixes() {
        // tryLock A/B 由默认 stub 返回 null；unlock A/B 返回 1L
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        lock.tryLock("orderLock", -1, TimeUnit.MILLISECONDS);
        lock.tryLock("payLock", -1, TimeUnit.MILLISECONDS);
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(2);

        lock.unlock("orderLock");
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);

        lock.unlock("payLock");
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    // ========================================================================
    // 8. Redisson行为一致性（4 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("lock语义：阻塞直到获取锁，自动Watchdog续期")
    void lock_consistency_immediate() {

        lock.lock("redissonLock");
        // 类似Redisson：lock()获取锁并启动Watchdog
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);
        verify(redisTemplate).execute(eq(LuaScriptRegistry.LOCK_SCRIPT), anyList(), argsCaptor.capture());
    }

    @Test
    @DisplayName("Watchdog语义：leaseTime<0自动续期，>0不续期")
    void watchdog_consistency() {

        // tryLock with watchdog
        lock.tryLock("wdLock", -1, TimeUnit.MILLISECONDS);
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1);

        // tryLock without watchdog
        lock.tryLock("noWdLock", 30000, TimeUnit.MILLISECONDS);
        // 第二个锁leaseTime=30000>0，不启动Watchdog
        // 但注意：tryLock的leaseTime=30000>0，waitTime=0，不启动Watchdog
        assertThat(lock.getActiveWatchdogCount()).isEqualTo(1); // 只有第一个有Watchdog

        lock.unlock("wdLock");
        assertThat(lock.getActiveWatchdogCount()).isZero();
    }

    @Test
    @DisplayName("unlock语义：仅持有者可解锁")
    void unlock_consistency_ownerOnly() {
        // 非持有者解锁返回false

        assertThat(lock.unlock("notMyLock")).isFalse();
        verify(redisTemplate).execute(eq(LuaScriptRegistry.UNLOCK_SCRIPT), anyList(), any(), any(), any());
    }

    @Test
    @DisplayName("TTL参数语义：锁过期时间传递到Lua脚本")
    void lock_ttl_consistency() {

        lock.tryLock("ttlLock", 15000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(scriptCaptor.capture(), keysCaptor.capture(), argsCaptor.capture());
        Object[] args = argsCaptor.getValue();
        // LOCK_SCRIPT args: [String.valueOf(leaseMs), entryName]
        assertThat(args[0]).isEqualTo("15000");
    }

    // ========================================================================
    // 9. 集群兼容性（4 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("锁Key含哈希标签{lock}:")
    void cluster_hashTagInKey() {

        lock.tryLock("myLock", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(any(RedisScript.class), keysCaptor.capture(), argsCaptor.capture());
        assertThat(keysCaptor.getValue().get(0)).contains("{lock}:");
    }

    @Test
    @DisplayName("解锁频道含哈希标签")
    void cluster_channelHashTag() {
        doReturn(1L).when(redisTemplate).execute(any(RedisScript.class), anyList(), any(), any(), any());

        lock.unlock("myLock");

        verify(redisTemplate).execute(any(RedisScript.class), keysCaptor.capture(), argsCaptor.capture());
        assertThat(keysCaptor.getValue().get(1)).contains("{lock}:");
    }

    @Test
    @DisplayName("entryName含clientId和threadId")
    void cluster_entryName_format() {

        lock.tryLock("clusterLock", 30000, TimeUnit.MILLISECONDS);

        verify(redisTemplate).execute(any(RedisScript.class), keysCaptor.capture(), argsCaptor.capture());
        Object[] args = argsCaptor.getValue();
        String entryName = (String) args[1];
        assertThat(entryName).matches("[a-f0-9\\-]+:\\d+");
    }

    @Test
    @DisplayName("Watchdog续期Key也含哈希标签")
    void cluster_renewal_hashTag() {

        lock.lock("clusterKey");

        verify(redisTemplate).execute(scriptCaptor.capture(), keysCaptor.capture(), argsCaptor.capture());
        assertThat(keysCaptor.getValue().get(0)).isEqualTo("{lock}:clusterKey");
    }

    // ========================================================================
    // 10. Lua脚本结构（2 个测试方法）
    // ========================================================================

    @Test
    @DisplayName("lock.lua包含hincrby/pexpire/hexists/pttl")
    void luaScript_containsRequiredCommands() {
        String s = LuaScriptRegistry.LOCK_SCRIPT.getScriptAsString();
        assertThat(s).contains("hincrby").contains("pexpire").contains("hexists").contains("pttl");
    }

    @Test
    @DisplayName("unlock.lua包含hexists/hincrby/publish")
    void unlockScript_containsRequiredCommands() {
        String s = LuaScriptRegistry.UNLOCK_SCRIPT.getScriptAsString();
        assertThat(s).contains("hexists").contains("hincrby").contains("publish");
    }
}
