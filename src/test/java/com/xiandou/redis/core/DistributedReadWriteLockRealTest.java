package com.xiandou.redis.core;

import com.xiandou.redis.constant.RedisKeyConstant;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分布式读写锁 — 真实 Redis Cluster 深度集成测试。
 * <p>覆盖：写写互斥、读写互斥、读读共享、锁降级、可重入、等待超时、数据格式验证。</p>
 *
 * <p>前置条件：Redis Cluster 已启动（192.168.56.186:6379-6381）</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.MethodName.class)
class DistributedReadWriteLockRealTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private DistributedReadWriteLock rwLock;

    private String testPrefix(String suffix) {
        return "test:real:rw:" + suffix;
    }

    private void cleanup(String name) {
        redisTemplate.delete(RedisKeyConstant.rwlockKey(name));
    }

    @BeforeEach
    void setUp() {
        rwLock = new DistributedReadWriteLock(redisTemplate);
    }

    // ================================================================
    //  1. 写锁基础
    // ================================================================

    @Test
    @DisplayName("写锁-基本加锁与解锁")
    void writeLockBasic() {
        String name = testPrefix("wBasic");
        String key = RedisKeyConstant.rwlockKey(name);
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertEquals("write", redisTemplate.opsForHash().get(key, "mode"));
        assertTrue(rwLock.unlockWrite(name));
        assertFalse(redisTemplate.hasKey(key));
    }

    @Test
    @DisplayName("写锁-写写互斥（不同实例）")
    void writeLockExclusive() {
        String name = testPrefix("wExcl");
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertFalse(other.tryWriteLock(name, 0, 5, TimeUnit.SECONDS));
        rwLock.unlockWrite(name);
    }

    @Test
    @DisplayName("写锁-可重入（同实例多次加锁）")
    void writeLockReentrant() {
        String name = testPrefix("wReent");
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertTrue(rwLock.unlockWrite(name));
        assertTrue(rwLock.unlockWrite(name));
        assertTrue(rwLock.unlockWrite(name));
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.rwlockKey(name)));
    }

    @Test
    @DisplayName("写锁-不同实例无法解锁（非法解锁）")
    void writeLockUnlockByOtherFails() {
        String name = testPrefix("wUnlockOther");
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertFalse(other.unlockWrite(name));
        assertTrue(rwLock.unlockWrite(name));
    }

    @Test
    @DisplayName("写锁-重入后其他实例仍不可解锁")
    void writeLockReentrantOtherUnlockFails() {
        String name = testPrefix("wReentOther");
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertFalse(other.unlockWrite(name));
        rwLock.unlockWrite(name);
        rwLock.unlockWrite(name);
    }

    // ================================================================
    //  2. 读锁基础
    // ================================================================

    @Test
    @DisplayName("读锁-基本加锁与解锁")
    void readLockBasic() {
        String name = testPrefix("rBasic");
        String key = RedisKeyConstant.rwlockKey(name);
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        assertEquals("read", redisTemplate.opsForHash().get(key, "mode"));
        assertTrue(rwLock.unlockRead(name));
        assertFalse(redisTemplate.hasKey(key));
    }

    @Test
    @DisplayName("读锁-读读共享（多个实例同时加读锁）")
    void readLockShared() {
        String name = testPrefix("rShared");
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        assertTrue(other.readLock(name, 10, TimeUnit.SECONDS)); // 另一个实例也应成功
        assertTrue(rwLock.unlockRead(name));
        assertTrue(other.unlockRead(name));
    }

    @Test
    @DisplayName("读锁-可重入（同实例多次加读锁）")
    void readLockReentrant() {
        String name = testPrefix("rReent");
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        assertTrue(rwLock.unlockRead(name));
        assertTrue(rwLock.unlockRead(name));
        assertTrue(rwLock.unlockRead(name));
        assertFalse(redisTemplate.hasKey(RedisKeyConstant.rwlockKey(name)));
    }

    @Test
    @DisplayName("读锁-不同实例无法解锁读锁")
    void readLockUnlockByOtherFails() {
        String name = testPrefix("rUnlockOther");
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        assertFalse(other.unlockRead(name));
        assertTrue(rwLock.unlockRead(name));
    }

    // ================================================================
    //  3. 读写互斥
    // ================================================================

    @Test
    @DisplayName("读写互斥-写锁阻塞读锁")
    void writeLockBlocksRead() {
        String name = testPrefix("wrBlock");
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(rwLock.writeLock(name, 5, TimeUnit.SECONDS));
        assertFalse(other.tryReadLock(name, 0, 3, TimeUnit.SECONDS));
        rwLock.unlockWrite(name);
    }

    @Test
    @DisplayName("读写互斥-读锁阻塞写锁")
    void readLockBlocksWrite() {
        String name = testPrefix("rwBlock");
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        assertFalse(other.tryWriteLock(name, 0, 3, TimeUnit.SECONDS));
        rwLock.unlockRead(name);
    }

    @Test
    @DisplayName("读写互斥-写锁释放后读锁可获取（等待）")
    void writeLockThenReadLockWithWait() throws InterruptedException {
        String name = testPrefix("wrWait");
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));

        Thread t = new Thread(() -> {
            DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
            ready.countDown();
            got.set(other.tryReadLock(name, 5, 5, TimeUnit.SECONDS));
            if (got.get()) other.unlockRead(name);
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(300);
        rwLock.unlockWrite(name); // 释放写锁

        t.join(8000);
        assertTrue(got.get(), "写锁释放后读锁应能获取");
    }

    @Test
    @DisplayName("读写互斥-读锁释放后写锁可获取（等待）")
    void readLockThenWriteLockWithWait() throws InterruptedException {
        String name = testPrefix("rwWait");
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));

        Thread t = new Thread(() -> {
            DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
            ready.countDown();
            got.set(other.tryWriteLock(name, 5, 5, TimeUnit.SECONDS));
            if (got.get()) other.unlockWrite(name);
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(300);
        rwLock.unlockRead(name); // 释放读锁

        t.join(8000);
        assertTrue(got.get(), "读锁释放后写锁应能获取");
    }

    @Test
    @DisplayName("读写互斥-多读线程+写等待+读释放后写成功")
    void multipleReadersThenWriter() throws InterruptedException {
        String name = testPrefix("multiRw");
        int readerCount = 3;
        CountDownLatch readersReady = new CountDownLatch(readerCount);
        AtomicBoolean writerGot = new AtomicBoolean(false);
        CountDownLatch writerDone = new CountDownLatch(1);

        // 多线程加读锁
        for (int i = 0; i < readerCount; i++) {
            new Thread(() -> {
                DistributedReadWriteLock r = new DistributedReadWriteLock(redisTemplate);
                r.readLock(name, 10, TimeUnit.SECONDS);
                readersReady.countDown();
                // 保持读锁直到被通知
                try { TimeUnit.SECONDS.sleep(5); } catch (InterruptedException ignore) {}
                r.unlockRead(name);
            }).start();
        }

        readersReady.await();
        TimeUnit.MILLISECONDS.sleep(200);

        // 写锁应被读锁阻塞
        Thread writer = new Thread(() -> {
            DistributedReadWriteLock w = new DistributedReadWriteLock(redisTemplate);
            writerGot.set(w.tryWriteLock(name, 8, 3, TimeUnit.SECONDS));
            if (writerGot.get()) w.unlockWrite(name);
            writerDone.countDown();
        });
        writer.start();

        // 等待读锁过期（5秒）→ 写锁应能获取
        writerDone.await(12, TimeUnit.SECONDS);
        assertTrue(writerGot.get(), "所有读锁释放后写锁应能获取");
    }

    // ================================================================
    //  4. 锁降级（写→读）
    // ================================================================

    @Test
    @DisplayName("锁降级-写锁持有者可获取读锁")
    void lockDowngradeWriteToRead() {
        String name = testPrefix("downgrade");
        String key = RedisKeyConstant.rwlockKey(name);

        // 写锁
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        assertEquals("write", redisTemplate.opsForHash().get(key, "mode"));

        // 同线程获取读锁 → 降级
        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));

        // 释放写锁 → 模式变 read
        assertTrue(rwLock.unlockWrite(name));
        assertEquals("read", redisTemplate.opsForHash().get(key, "mode"));

        // 释放读锁 → 完全删除
        assertTrue(rwLock.unlockRead(name));
        assertFalse(redisTemplate.hasKey(key));
    }

    @Test
    @DisplayName("锁降级-其他写锁在降级期间仍阻塞")
    void lockDowngradeBlocksOtherWriter() {
        String name = testPrefix("downgradeBlock");
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);

        rwLock.writeLock(name, 10, TimeUnit.SECONDS);
        rwLock.readLock(name, 10, TimeUnit.SECONDS);   // 降级
        rwLock.unlockWrite(name);                       // mode→read

        // 其他线程在 read 模式下应无法加写锁
        assertFalse(other.tryWriteLock(name, 0, 3, TimeUnit.SECONDS));

        rwLock.unlockRead(name);
    }

    @Test
    @DisplayName("锁降级-可多次降级并恢复")
    void lockDowngradeMultipleTimes() {
        String name = testPrefix("downgradeMulti");
        String key = RedisKeyConstant.rwlockKey(name);

        rwLock.writeLock(name, 10, TimeUnit.SECONDS);
        rwLock.readLock(name, 10, TimeUnit.SECONDS);   // 降级1

        // 释放写锁后读锁仍在
        rwLock.unlockWrite(name);
        assertEquals("read", redisTemplate.opsForHash().get(key, "mode"));

        // 再升级（通过unlockRead后加写锁）
        rwLock.unlockRead(name);

        // 重新加写锁
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        rwLock.unlockWrite(name);
        assertFalse(redisTemplate.hasKey(key));
    }

    // ================================================================
    //  5. 超时与等待
    // ================================================================

    @Test
    @DisplayName("tryWriteLock带等待-超时返回false")
    void tryWriteLockWaitTimeout() throws InterruptedException {
        String name = testPrefix("wWaitTimeout");
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));

        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        long start = System.currentTimeMillis();
        boolean result = other.tryWriteLock(name, 2, 5, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(result, "写锁被占应超时");
        assertTrue(elapsed >= 1800, "应等待约2秒（实际=" + elapsed + "ms）");
        rwLock.unlockWrite(name);
    }

    @Test
    @DisplayName("tryReadLock带等待-超时返回false")
    void tryReadLockWaitTimeout() throws InterruptedException {
        String name = testPrefix("rWaitTimeout");
        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));

        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        long start = System.currentTimeMillis();
        boolean result = other.tryReadLock(name, 2, 5, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(result, "写锁阻塞读锁应超时");
        assertTrue(elapsed >= 1800, "应等待约2秒（实际=" + elapsed + "ms）");
        rwLock.unlockWrite(name);
    }

    @Test
    @DisplayName("tryWriteLock带等待-成功获取")
    void tryWriteLockWaitSuccess() throws InterruptedException {
        String name = testPrefix("wWaitOK");
        AtomicBoolean got = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(1);

        assertTrue(rwLock.writeLock(name, 5, TimeUnit.SECONDS));

        Thread t = new Thread(() -> {
            DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
            ready.countDown();
            got.set(other.tryWriteLock(name, 8, 5, TimeUnit.SECONDS));
            if (got.get()) other.unlockWrite(name);
        });
        t.start();

        ready.await();
        TimeUnit.MILLISECONDS.sleep(500);
        rwLock.unlockWrite(name); // 释放

        t.join(10000);
        assertTrue(got.get(), "写锁释放后其他线程应能获取");
    }

    // ================================================================
    //  6. 数据格式验证
    // ================================================================

    @Test
    @DisplayName("数据-写锁Redis结构验证")
    void dataWriteLockStructure() {
        String name = testPrefix("dataWrite");
        String key = RedisKeyConstant.rwlockKey(name);
        long tid = Thread.currentThread().getId();

        assertTrue(rwLock.writeLock(name, 10, TimeUnit.SECONDS));
        String entryName = "write_lock:" + rwLock.getClientId() + ":" + tid;

        assertEquals("write", redisTemplate.opsForHash().get(key, "mode"));
        assertEquals("1", redisTemplate.opsForHash().get(key, entryName).toString());

        assertTrue(redisTemplate.getExpire(key, TimeUnit.SECONDS) > 0);

        rwLock.unlockWrite(name);
        assertFalse(redisTemplate.hasKey(key));
    }

    @Test
    @DisplayName("数据-读锁Redis结构验证")
    void dataReadLockStructure() {
        String name = testPrefix("dataRead");
        String key = RedisKeyConstant.rwlockKey(name);
        long tid = Thread.currentThread().getId();

        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        String entryName = "read_lock:" + rwLock.getClientId() + ":" + tid;

        assertEquals("read", redisTemplate.opsForHash().get(key, "mode"));
        assertEquals("1", redisTemplate.opsForHash().get(key, entryName).toString());

        rwLock.unlockRead(name);
        assertFalse(redisTemplate.hasKey(key));
    }

    @Test
    @DisplayName("数据-读锁多实例共享计数")
    void dataReadLockMultipleReaders() {
        String name = testPrefix("dataMultiRead");
        String key = RedisKeyConstant.rwlockKey(name);
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        String entry1 = "read_lock:" + rwLock.getClientId() + ":" + Thread.currentThread().getId();

        assertTrue(rwLock.readLock(name, 10, TimeUnit.SECONDS));
        // 读锁 hash 包含 mode 和 reader 两个字段
        assertEquals(2L, redisTemplate.opsForHash().size(key), "应有 mode + reader 两个字段");
        assertEquals("1", redisTemplate.opsForHash().get(key, entry1));

        // 不同 DistributedReadWriteLock 实例（不同 clientId）获取读锁应创建新条目
        String entry2 = "read_lock:" + other.getClientId() + ":" + Thread.currentThread().getId();
        assertTrue(other.readLock(name, 10, TimeUnit.SECONDS));
        // 此时 hash 有 mode + reader1 + reader2 = 3 个字段
        assertEquals(3L, redisTemplate.opsForHash().size(key), "两个读锁应有 3 个字段");
        assertEquals("1", redisTemplate.opsForHash().get(key, entry2));

        rwLock.unlockRead(name);
        other.unlockRead(name);
        assertFalse(redisTemplate.hasKey(key));
    }

    @Test
    @DisplayName("数据-读写锁Key哈希标签")
    void rwLockKeyHashTag() {
        String name = testPrefix("hashTag");
        String key = RedisKeyConstant.rwlockKey(name);
        assertTrue(key.startsWith("{rwlock}:"), "rwlockKey 应以 {rwlock}: 开头");
    }

    // ================================================================
    //  7. 边界条件
    // ================================================================

    @Test
    @DisplayName("边界-不同锁名互不干扰")
    void differentNamesIndependent() {
        assertTrue(rwLock.writeLock(testPrefix("indepA"), 10, TimeUnit.SECONDS));
        assertTrue(rwLock.readLock(testPrefix("indepB"), 10, TimeUnit.SECONDS));
        rwLock.unlockWrite(testPrefix("indepA"));
        rwLock.unlockRead(testPrefix("indepB"));
    }

    @Test
    @DisplayName("边界-写锁TTL过期后自动释放")
    void writeLockTTLExpires() throws InterruptedException {
        String name = testPrefix("wTTL");
        assertTrue(rwLock.writeLock(name, 2, TimeUnit.SECONDS));
        TimeUnit.SECONDS.sleep(3);
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(other.writeLock(name, 0, TimeUnit.SECONDS), "写锁过期后应能重新获取");
        other.unlockWrite(name);
    }

    @Test
    @DisplayName("边界-读锁TTL过期后自动释放")
    void readLockTTLExpires() throws InterruptedException {
        String name = testPrefix("rTTL");
        assertTrue(rwLock.readLock(name, 2, TimeUnit.SECONDS));
        TimeUnit.SECONDS.sleep(3);
        DistributedReadWriteLock other = new DistributedReadWriteLock(redisTemplate);
        assertTrue(other.writeLock(name, 0, TimeUnit.SECONDS), "读锁过期后写锁应能获取");
        other.unlockWrite(name);
    }

    @Test
    @DisplayName("边界-unlockWrite对未锁名称调用返回false")
    void unlockWriteOnNonExistent() {
        assertFalse(rwLock.unlockWrite("nonexistent_rw"));
    }

    @Test
    @DisplayName("边界-unlockRead对未锁名称调用返回false")
    void unlockReadOnNonExistent() {
        assertFalse(rwLock.unlockRead("nonexistent_rw"));
    }
}
