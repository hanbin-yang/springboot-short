# RedisTemplate 仿写 Redisson 功能 — 改造记录

> 基于 Redis Cluster 3 主节点（192.168.56.186:186:6379-6381）的真实环境验证。

## 目录

1. [环境搭建](#1-环境搭建)
2. [Bug 修复](#2-bug-修复)
3. [TTL 过期策略改造](#3-ttl-过期策略改造)
4. [测试覆盖](#4-测试覆盖)
5. [运行命令](#5-运行命令)
6. [遗留问题](#6-遗留问题)

---

## 1. 环境搭建

### 1.1 K8s 部署 Redis Cluster

虚拟机 2 台：Master（192.168.56.186）+ Worker（192.168.56.187），K8s 1 主 1 从。

```yaml
# Pod 分配（hostNetwork 模式）
redis-0 → k8s-node01   (192.168.56.187:6379)
redis-1 → k8s-master01 (192.168.56.186:6380)
redis-2 → k8s-master01 (192.168.56.186:6381)
```

- hostNetwork + 每 Pod 不同端口（6379/6380/6381）避免同节点冲突
- `cluster-announce-ip` 广播虚拟机真实 IP，外部可直连
- 纯内存运行（`--save "" --appendonly no`），hostPath 持久化

**部署文件：** `k8s/01-redis-cluster.yaml`

### 1.2 Spring Boot 连接配置

```yaml
spring:
  data:
    redis:
      cluster:
        nodes:
          - 192.168.56.187:6379
          - 192.168.56.186:6380
          - 192.168.56.186:6381
        max-redirects: 3
      timeout: 3000ms
      lettuce:
        pool:
          max-active: 16
          max-idle: 8
          min-idle: 2
        cluster:
          refresh:
            period: 2000s
```

---

## 2. Bug 修复

### 2.1 `DistributedSemaphore.release()` 参数传错（生产代码 Bug）

**现象：** 运行 `SEMAPHORE_RELEASE_SCRIPT` 时报 `ERR value is not an integer or out of range`

**根因：** Lua 脚本要求 `KEYS[2]` 是频道名，但代码将频道名传到了 `ARGV[1]`：

```java
// ❌ 错误：频道名跑到了 ARGV[1]
redisTemplate.execute(
    LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT,
    List.of(RedisKeyConstant.semaphoreKey(name)),     // 只有 1 个 KEYS
    RedisKeyConstant.semaphoreChannel(name),           // → ARGV[1]
    String.valueOf(permits)                            // → ARGV[2]
);
```

```java
// ✅ 修复：频道名作为 KEYS[2] 传入
redisTemplate.execute(
    LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT,
    List.of(RedisKeyConstant.semaphoreKey(name), RedisKeyConstant.semaphoreChannel(name)),
    String.valueOf(permits)
);
```

### 2.2 测试代码错误：`tryLock` 方法重载混淆

**现象：** `tryLock(name, 8, TimeUnit.SECONDS)` 期望"等 8 秒"却"不等待"

**根因：** 3 参数 `tryLock(name, leaseTime, unit)` 是"不等待，指定过期时间"；4 参数 `tryLock(name, waitTime, leaseTime, unit)` 才是"等待指定时间"。测试代码误用了 3 参版本。

```java
// ❌ 错误：调成了「不等待，leaseTime=8」
got.set(l2.tryLock(name, 8, TimeUnit.SECONDS));

// ✅ 正确：等 8 秒，过期 5 秒
got.set(l2.tryLock(name, 8, 5, TimeUnit.SECONDS));
```

### 2.3 JVM 参数缺失：Mockito ByteBuddy 无法加载

**现象：** `Could not initialize inline Byte Buddy mock maker`

**修复：** 

```xml
<!-- pom.xml -->
<properties>
    <argLine>-Djdk.attach.allowAttachSelf=true -Xms128m -Xmx384m</argLine>
</properties>
```

运行命令（内存不足时）：

```bash
MAVEN_OPTS="-Djdk.attach.allowAttachSelf=true -Xms128m -Xmx256m" mvn test -Dtest=TestName -DforkCount=0
```

---

## 3. TTL 过期策略改造

### 3.1 问题背景

`DistributedSemaphore` 和 `DistributedCountDownLatch` 操作 Redis 时**没有设置过期时间**，可能导致内存泄漏。

**排查结果：** 4 个 Lua 脚本缺少 TTL：

| # | Lua 脚本 | 对应方法 | 问题 |
|---|----------|----------|------|
| 1 | `SET_IF_ABSENT_SCRIPT` | `trySetPermits` / `trySetCount` | `SET` 创建 Key 无过期 |
| 2 | `SEMAPHORE_ACQUIRE_SCRIPT` | `tryAcquire` | `DECRBY` 不刷新 TTL |
| 3 | `SEMAPHORE_RELEASE_SCRIPT` | `release` | `INCRBY` 创建 Key 无过期 |
| 4 | `LATCH_COUNTDOWN_SCRIPT` | `countDown` | `DECR` 创建 Key 无过期 |

**锁定/读写锁已有 TTL：** `LOCK_SCRIPT` / `UNLOCK_SCRIPT` / `RENEW_SCRIPT` / 读写锁全部脚本均已使用 `PEXPIRE` ✅

### 3.2 改造方案

1. TTL 通过 **ARGV 参数传入**，不硬编码在 Lua 脚本中
2. **构造器注入** TTL，默认 7 天（604800 秒）
3. 用 `@Value` 可配（通过 `RedisConfig` 或 `application.yml`）

```java
// 两个构造器
public DistributedSemaphore(StringRedisTemplate redisTemplate);        // 默认 7 天
public DistributedSemaphore(StringRedisTemplate redisTemplate, long ttlSeconds); // 自定义
```

### 3.3 Lua 脚本 ARGV 布局

| 脚本 | KEYS | ARGV |
|------|------|------|
| `SET_IF_ABSENT_SCRIPT` | [1]=key | [1]=value, [2]=ttlSeconds |
| `SEMAPHORE_ACQUIRE_SCRIPT` | [1]=key | [1]=permits, [2]=ttlSeconds |
| `SEMAPHORE_RELEASE_SCRIPT` | [1]=key, [2]=channel | [1]=permits, [2]=ttlSeconds |
| `LATCH_COUNTDOWN_SCRIPT` | [1]=key, [2]=channel | [1]=ttlSeconds |

### 3.4 安全性分析

#### 信号量 — 安全 ✅

```
trySetPermits("s", 5)    → Key=5, TTL=7天
tryAcquire("s", 5)       → Key=0（用光）
7天后 Key 过期
release("s", 3)          → INCRBY 创建 Key=3 ✅ 合理
tryAcquire("s", 2)       → 3≥2 → 成功 Key=1 ✅
```

`INCRBY` 在过期 Key 上创建**正数值**，语义就是"释放了几个许可"，无论之前状态如何都是合理操作。

#### Latch — 有坑，已修复 ✅

```
trySetCount("x", 5)      → Key=5, TTL=7天
countDown("x") ×3次       → Key=2
7天后 Key 过期
countDown("x")            → ❌ DECR 创建 Key=-1，PUBLISH 归零通知
await() 看到 -1 ≤ 0       → ❌ 误判为已归零！
```

**修复后的 `LATCH_COUNTDOWN_SCRIPT`：**

```lua
if redis.call('exists', KEYS[1]) == 1 then          -- ① Key 存在
    local val = redis.call('decr', KEYS[1]);         -- ② 递减
    redis.call('expire', KEYS[1], ARGV[1]);           -- ③ 刷新 TTL
    if val <= 0 then
        redis.call('publish', KEYS[2], 0);             -- ④ 归零通知
    end;
    return val;
end;
redis.call('publish', KEYS[2], 0);  -- Key 过期，仍通知唤醒等待线程
return nil;
```

**三层保障：**

| 层 | 机制 | 效果 |
|---|------|------|
| 1 | `exists` 检查 | Key 过期后 `DECR` 不会乱创建 -1 脏数据 |
| 2 | `expire` 刷新 TTL | 活跃的 latch 不会意外过期 |
| 3 | 过期后 PUBLISH | 等待线程不会永久阻塞 |

### 3.5 TTL 策略总览

| 脚本 | `exists` 保护 | `expire` | 原因 |
|------|:----------:|:--------:|------|
| `SET_IF_ABSENT` | ✅ 本身就是 | ✅ 7天 | 创建 Key 时设 TTL |
| `SEMAPHORE_ACQUIRE` | ❌ 不需要 | ✅ 7天 | 刷新 TTL，防活跃信号量过期 |
| `SEMAPHORE_RELEASE` | ❌ 不需要 | ✅ 7天 | `INCRBY` 创建正数安全 |
| `LATCH_COUNTDOWN` | ✅ **必须有** | ✅ 7天 | 防 `DECR` 创建 -1 误判归零 |

---

## 4. 测试覆盖

### 4.1 测试文件清单

| 文件 | 测试类 | 测试数 | 说明 |
|------|--------|:------:|------|
| `core/DistributedLockRealTest` | `DistributedLock` | 37 | 加解锁、重入、互斥、Watchdog、Pub/Sub、并发、Hash结构 |
| `core/DistributedSemaphoreRealTest` | `DistributedSemaphore` | 29 | 初始化、获取释放、阻塞超时、并发竞争、TTL过期 |
| `core/DistributedCountDownLatchRealTest` | `DistributedCountDownLatch` | 30 | 计数设置、await超时/归零、多线程Pub/Sub、TTL过期 |
| `core/DistributedReadWriteLockRealTest` | `DistributedReadWriteLock` | 29 | 写写互斥、读读共享、锁降级、读写互斥、TTL过期 |
| `listener/PubSubSubscriberRealTest` | `PubSubSubscriber` | 17 | 订阅接收、多订阅者、close清理、集成通知 |
| `utils/RedisUtilRealTest` | `RedisUtil` (外观层) | 23 | executeTryLock/executeLock 6个重载、异常解锁、重入 |
| **合计** | | **165** | |

### 4.2 关键测试场景

**锁：**
- 同一线程可重入 3 次解锁
- 不同 `DistributedLock` 实例互斥
- Watchdog 续期（默认 30s TTL / 10s 间隔）
- 解锁后 Watchdog 停止
- 高并发 10 线程竞争

**信号量：**
- `trySetPermits` → `tryAcquire` → `release` 完整生命周期
- 许可不足时阻塞等待 / 超时返回 false
- TTL 过期后 `tryAcquire` 返回 false
- TTL 过期后 `release` 通过 `INCRBY` 创建新许可

**CountDownLatch：**
- `trySetCount` → `countDown` → `await` 完整流程
- 多线程同时等待，归零后全部唤醒
- TTL 过期后 `await` **立即返回**（不会死等）
- TTL 过期后 `countDown` **不创建 -1 脏数据**

**读写锁：**
- 写写互斥、读写互斥、读读共享
- 写锁→读锁降级
- 锁降级后其他写锁仍阻塞

### 4.3 测试发现的 Bug

| # | Bug | 类型 | 修复 |
|---|-----|------|------|
| 1 | `DistributedSemaphore.release()` 频道名当 ARGV 传 | 生产代码 | `List.of(key, channel)` |
| 2 | 测试代码 `tryLock` 重载混淆（等待时间/过期时间） | 测试代码 | 改用 4 参数版本 |

---

## 5. 运行命令

```bash
# 编译
mvn compile
mvn test-compile

# 运行单个测试（内存不足时用 -DforkCount=0 + MAVEN_OPTS）
mvn test -Dtest=DistributedLockRealTest
MAVEN_OPTS="-Djdk.attach.allowAttachSelf=true -Xms128m -Xmx256m" mvn test -Dtest=DistributedLockRealTest -DforkCount=0

# 运行多个测试（逗号分隔）
mvn test -Dtest="DistributedSemaphoreRealTest,DistributedCountDownLatchRealTest"

# 运行所有已有 Mock 测试
mvn test

# 强制清理 + 测试
rm -rf target/surefire-reports/ && mvn test -Dtest=TestName
```

---

## 6. 遗留问题

### 6.1 Pub/Sub 集群兼容性

`PubSubSubscriber` 通过 `connection.subscribe()` 连接到**随机集群节点**。锁释放时 `UNLOCK_SCRIPT` 中的 `PUBLISH` 在哈希槽归属节点上执行。如果订阅者连接到不同的节点，通知丢失，`tryLock` 的等待机制退化为**轮询**（每 1 秒重试一次）。

Redisson 的处理方式：为每个集群节点建立独立的 Pub/Sub 连接。

### 6.2 锁部分解锁 TTL 刷新

`UNLOCK_SCRIPT` 在部分解锁（仍有重入）时使用 `DEFAULT_LEASE_TIME_MS`（30s）刷新 TTL，而不是使用原始 `leaseTime`。

```lua
-- UNLOCK_SCRIPT 中的逻辑
if (counter > 0) then
    redis.call('pexpire', KEYS[1], ARGV[2]); -- ARGV[2] 始终是 DEFAULT_LEASE_TIME_MS
```

### 6.3 `LATCH_COUNTDOWN_SCRIPT` 不删除归零 Key

`countDown` 归零后只 PUBLISH 通知，**不 DEL Key**。Key 由 **7 天 TTL** 自动清理。如果需要显式清理，调用方需要自行调用 `redisTemplate.delete()`。
