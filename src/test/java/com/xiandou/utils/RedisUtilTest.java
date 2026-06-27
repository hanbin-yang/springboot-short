package com.xiandou.utils;

import com.xiandou.redis.core.DistributedLock;
import com.xiandou.utils.lock.RedisLockResult;
import com.xiandou.utils.lock.RedisLockUtil;
import com.xiandou.utils.lock.VoidSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * RedisUtil 外观层单元测试。
 * 使用 Mockito 模拟 DistributedLock，不依赖真实 Redis。
 */
@ExtendWith(MockitoExtension.class)
class RedisUtilTest {

    private static final String TEST_KEY = "test:lock:key";
    private static final String TEST_RESULT = "hello";

    @Mock
    private DistributedLock distributedLock;

    @BeforeEach
    void setUp() {
        RedisLockUtil.init(distributedLock);
    }

    // ==================== executeTryLock 测试 ====================

    @Test
    @DisplayName("executeTryLock 成功获取锁，Supplier 执行并返回结果")
    void executeTryLock_success_returnsResult() {
        doReturn(true).when(distributedLock).tryLock(anyString(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));

        RedisLockResult<String> result = RedisLockUtil.executeTryLock(TEST_KEY, 5, () -> TEST_RESULT);

        assertThat(result.isFailure()).isFalse();
        assertThat(result.getObj()).isEqualTo(TEST_RESULT);
        verify(distributedLock).unlock(TEST_KEY);
    }

    @Test
    @DisplayName("executeTryLock 锁被占用，返回 fail，Supplier 不执行")
    void executeTryLock_lockOccupied_returnsFail() {
        doReturn(false).when(distributedLock).tryLock(anyString(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));

        AtomicBoolean executed = new AtomicBoolean(false);
        RedisLockResult<String> result = RedisLockUtil.executeTryLock(TEST_KEY, 5, () -> {
            executed.set(true);
            return TEST_RESULT;
        });

        assertThat(result.isFailure()).isTrue();
        assertThat(result.getObj()).isNull();
        assertThat(executed.get()).isFalse();
        verify(distributedLock, never()).unlock(anyString());
    }

    @Test
    @DisplayName("executeTryLock Supplier 抛异常，锁仍被释放")
    void executeTryLock_supplierThrows_stillUnlocks() {
        doReturn(true).when(distributedLock).tryLock(anyString(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        RuntimeException expectedEx = new RuntimeException("业务异常");

        assertThatThrownBy(() ->
                RedisLockUtil.executeTryLock(TEST_KEY, 5, () -> {
                    throw expectedEx;
                })
        ).isSameAs(expectedEx);

        verify(distributedLock).unlock(TEST_KEY);
    }

    @Test
    @DisplayName("executeTryLock 不等待（0秒）")
    void executeTryLock_zeroWaitTime() {
        doReturn(true).when(distributedLock).tryLock(anyString(), eq(0L), anyLong(), eq(TimeUnit.MILLISECONDS));

        RedisLockResult<String> result = RedisLockUtil.executeTryLock(TEST_KEY, 0, () -> TEST_RESULT);

        assertThat(result.isFailure()).isFalse();
        assertThat(result.getObj()).isEqualTo(TEST_RESULT);
        verify(distributedLock).tryLock(TEST_KEY, 0L,
                DistributedLock.DEFAULT_LEASE_TIME_MS, TimeUnit.MILLISECONDS);
    }

    // ==================== executeLock 测试 ====================

    @Test
    @DisplayName("executeLock 正常执行并返回值")
    void executeLock_normal_returnsResult() {
        String result = RedisLockUtil.executeLock(TEST_KEY, 10, TimeUnit.SECONDS, () -> TEST_RESULT);

        assertThat(result).isEqualTo(TEST_RESULT);
        verify(distributedLock).lock(TEST_KEY, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS);
        verify(distributedLock).unlock(TEST_KEY);
    }

    @Test
    @DisplayName("executeLock VoidSupplier 正常执行")
    void executeLock_voidSupplier_normal() {
        AtomicBoolean executed = new AtomicBoolean(false);

        RedisLockUtil.executeLock(TEST_KEY, 10, TimeUnit.SECONDS, () -> executed.set(true));

        assertThat(executed.get()).isTrue();
        verify(distributedLock).lock(TEST_KEY, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS);
        verify(distributedLock).unlock(TEST_KEY);
    }

    // ==================== 重载路由测试 ====================

    @Test
    @DisplayName("所有 executeTryLock 重载最终调用到同一个核心方法")
    void executeTryLock_allOverloads_callCoreMethod() {
        doReturn(true).when(distributedLock).tryLock(anyString(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));

        // 2-参数：keyName, waitSeconds, Supplier
        RedisLockUtil.executeTryLock(TEST_KEY, 3, (Supplier<String>) () -> TEST_RESULT);

        // 2-参数：keyName, waitSeconds, VoidSupplier
        RedisLockUtil.executeTryLock(TEST_KEY, 3, (VoidSupplier) () -> {});

        // 3-参数：keyName, waitSeconds, expireSeconds, Supplier
        RedisLockUtil.executeTryLock(TEST_KEY, 3, 60, (Supplier<String>) () -> TEST_RESULT);

        // 3-参数：keyName, waitSeconds, expireSeconds, VoidSupplier
        RedisLockUtil.executeTryLock(TEST_KEY, 3, 60, (VoidSupplier) () -> {});

        // 4-参数：keyName, waitTime, expireTime, TimeUnit, Supplier
        RedisLockUtil.executeTryLock(TEST_KEY, 3, 60, TimeUnit.SECONDS, (Supplier<String>) () -> TEST_RESULT);

        // 4-参数：keyName, waitTime, expireTime, TimeUnit, VoidSupplier
        RedisLockUtil.executeTryLock(TEST_KEY, 3, 60, TimeUnit.SECONDS, (VoidSupplier) () -> {});

        // 验证 tryLock 被调用了 6 次（每个重载各 1 次）
        verify(distributedLock, times(6)).tryLock(anyString(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        // 验证 unlock 也被调用了 6 次
        verify(distributedLock, times(6)).unlock(anyString());
    }
}
