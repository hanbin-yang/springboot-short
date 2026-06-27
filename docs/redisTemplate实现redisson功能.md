# RedisTemplate 仿写 Redisson 功能 — 改造全记录

> 基于 Redis Cluster 3 主节点 + K8s 虚拟机环境，对 `DistributedLock` / `DistributedSemaphore` / `DistributedCountDownLatch` / `DistributedReadWriteLock` 等分布式原语进行深度测试和改造。
>
> 项目地址：`springboot-short`，Spring Boot 3.4.1 + Lettuce + Redis Cluster 7.2

---

## 目录

1. [背景](#1-背景)
2. [环境搭建与踩坑](#2-环境搭建与踩坑)
3. [Bug 修复记录](#3-bug-修复记录)
4. [TTL 过期策略改造](#4-ttl-过期策略改造)
5. [测试覆盖](#5-测试覆盖)
6. [运行命令](#6-运行命令)
7. [遗留问题](#7-遗留问题)

---

## 1. 背景

### 1.1 项目现状

项目中基于 `StringRedisTemplate` + Lua 脚本仿写了 Redisson 的分布式锁功能：

```
com.xiandou/
├── redis/
│   ├── config/
│   │   ├── RedisConfig.java            → 注册 RedisTemplate、DistributedLock Bean
│   │   ├── RedisUtilInitializer.java   → @PostConstruct 桥接 RedisLockUtil 注入
│   │   └── LuaScriptRegistry.java      → 7 个 Lua 脚本常量
│   ├── constant/
│   │   └── RedisKeyConstant.java       → Key/Channel 前缀（含 {tag} 哈希标签）
│   ├── core/
│   │   ├── DistributedLock.java              → 可重入锁 + Watchdog
│   │   ├── DistributedSemaphore.java         → 信号量
│   │   ├── DistributedCountDownLatch.java    → 倒计数门闩
│   │   └── DistributedReadWriteLock.java     → 读写锁 + 锁降级
│   └── listener/
│       └── PubSubSubscriber.java     → Pub/Sub 订阅助手
└── utils/
    └── lock/
        ├── RedisLockUtil.java        → 外观层（executeTryLock / executeLock）
        ├── RedisLockResult.java      → 锁操作结果封装
        └── VoidSupplier.java         → 无返回值函数式接口
```

所有分布式原语是**纯 Spring Data Redis + Lua 脚本**自行实现，不依赖 Redisson。

### 1.2 原有测试

已有 Mock 测试覆盖内部逻辑、异常路径、时间敏感场景。 使用虚拟机构建k8s部署了一套简单的3节点的redis集群模式，配合测试用例，已充分测试

目标：编写**真实集群测试**，验证仿写 Redisson 的功能在集群环境下是否正常工作，并修复发现的问题。

---

## 2. 环境搭建与踩坑

### 2.1 第一阶段：K8s 部署 Redis Cluster

首次用 Kubernetes 部署 Redis Cluster，踩了不少坑。

#### 踩坑 1：集群模式 ≠ 哨兵模式

一开始想装"1 主 2 从"，但 **Redis Cluster 模式最少需要 3 个主节点**（16384 个哈希槽平分）。"1 主 2 从"是 Sentinel 模式，不是 Cluster。最终用的是 Cluster 模式 3 主 0 从。

#### 踩坑 2：external access 的设计

K8s 集群只有 1 主（192.168.56.186）+ 1 从（192.168.56.187），但需要 3 个 Pod。Spring Boot 在宿主机（Windows）上跑，需要直连虚拟机 IP。

**方案演进：**

```
第一次（复杂版）→ StatefulSet + PVC + ConfigMap + InitContainer + Job
第二次（简化版）→ 3 个独立 Pod + hostNetwork + 不同端口 + 纯 CLI 参数
```

最简方案只用 1 个 YAML 文件：

```yaml
# Pod 分配
redis-0 → k8s-node01   (192.168.56.187:6379)   ← Worker 节点
redis-1 → k8s-master01 (192.168.56.186:6380)   ← Master 节点
redis-2 → k8s-master01 (192.168.56.186:6381)   ← Master 节点（不同端口）
```

关键点：
- `hostNetwork: true` → Pod 直接绑虚拟机 IP
- `cluster-announce-ip` → 广播虚拟机 IP，外部可直连
- 每 Pod 不同端口（6379/6380/6381）→ 同节点不冲突
- `nodeSelector` + `tolerations` → 允许调度到 Master 节点

#### 踩坑 3：管它什么 StorageClass，直接用 hostPath

```bash
kubectl get storageclasses.storage.k8s.io
No resources found.
```

没有默认 StorageClass，PVC 起不来。测试环境直接改 `hostPath`，省掉 StorageClass 的麻烦。

#### 踩坑 4：节点名不是想的那样

```
kubectl apply -f 01-redis-cluster.yaml
→ 0/2 nodes are available: 2 node(s) didn't match node selector.
```

YAML 里写的 `k8s-worker` / `k8s-master`，实际节点名是 `k8s-node01` / `k8s-master01`。必须先查再写。

#### 踩坑 5：集群创建没成功

部署了 3 个 Pod，跑测试时全报 `PartitionSelectorException: Cannot determine a partition for slot 9412`。

查集群状态发现：
```
cluster_state:fail
cluster_slots_assigned:0
cluster_known_nodes:1
```

**没创建集群！** 3 个节点各自独立，没有通过 `redis-cli --cluster create` 形成集群。手动执行创建命令后恢复正常。

#### 踩坑 6：国内 Docker Hub 拉不动

Redis 镜像 `redis:7.2-alpine` 拉不下来。最后改成国内镜像 `docker.m.daocloud.io/library/redis:7.2-alpine`。

### 2.2 第二阶段：运行第一个测试

#### 踩坑 7：`PartitionSelectorException` — 槽位找不到

集群没创建时，所有依赖 `{semaphore}:` / `{lock}:` 等哈希标签的操作都报 `Cannot determine a partition for slot`。创建集群后解决。

#### 踩坑 8：测试调用了错误的 `tryLock` 重载

现象：`tryLock(name, 8, TimeUnit.SECONDS)` 期望等 8 秒，却**立即返回 false**。

根因：存在两个方法签名：

```java
// 3 参版本：「不等待，指定过期时间」
public boolean tryLock(String name, long leaseTime, TimeUnit unit)

// 4 参版本：「等待指定时间，指定过期时间」
public boolean tryLock(String name, long waitTime, long leaseTime, TimeUnit unit)
```

测试代码调了 3 参版本，`8` 被当作 leaseTime 而非 waitTime。修复：全部改用 4 参版本。

```java
// ❌ 错误
got.set(l2.tryLock(name, 8, TimeUnit.SECONDS));

// ✅ 正确
got.set(l2.tryLock(name, 8, 5, TimeUnit.SECONDS));
```

**影响范围：6 处测试代码。**

#### 踩坑 9：`assertNull(hasKey())` — 空指针隐患

`redisTemplate.hasKey()` 返回 `Boolean`（包装类），可能为 true/false/null。Lettuce 在健康集群返回 true/false，但 `assertNull()` 挂上去就 NPE。

```java
// ❌ 错误
assertNull(redisTemplate.hasKey(key));

// ✅ 正确
assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(key)));
```

sed 批量替换时括号搞乱了，又手动修了一轮。

#### 踩坑 10：JVM 分叉崩溃 — 内存不足

Windows 宿主机剩余内存不足，Surefire 分叉 JVM 时直接 OOM：

```
There is insufficient memory for the Java Runtime Environment to continue.
Native memory allocation (mmap) failed to map 268435456 bytes for G1 virtual space
```

最终方案：

```xml
<!-- pom.xml -->
<properties>
    <argLine>-Djdk.attach.allowAttachSelf=true -Xms128m -Xmx384m</argLine>
</properties>
```

运行命令：

```bash
MAVEN_OPTS="-Djdk.attach.allowAttachSelf=true -Xms128m -Xmx256m" mvn test -Dtest=TestName -DforkCount=0
```

#### 踩坑 11：Mockito ByteBuddy 在 Java 21 下不能动态加载

```
Could not initialize inline Byte Buddy mock maker.
It appears as if your JDK does not supply a working agent attachment mechanism.
```

Java 21 默认禁止动态加载 Agent。修复：`-Djdk.attach.allowAttachSelf=true` 加到 Surefire argLine。

#### 踩坑 12：Surefire 测试类匹配问题

`mvn test -Dtest="*RealTest"` 显示 "Tests run: 0"。逗号分隔也偶尔不识别。最终用类全名一个个跑：

```bash
mvn test -Dtest="DistributedLockRealTest"
mvn test -Dtest="DistributedSemaphoreRealTest,DistributedCountDownLatchRealTest"
```

---

## 3. Bug 修复记录

### 3.1 `DistributedSemaphore.release()` 参数传错 — 生产代码 Bug

#### 发现过程

运行 `DistributedSemaphoreRealTest` 时，`releaseIncreasesPermits` 等 7 个测试报错：

```
ERR value is not an integer or out of range
```

#### 追查

查看 `SEMAPHORE_RELEASE_SCRIPT` 的参数布局：

```lua
-- Lua 脚本期望：
KEYS[1] = 信号量 Key    ({semaphore}:name)
KEYS[2] = 频道 Key      (redisson_sc:{semaphore}:name)
ARGV[1] = 释放数量       (permits)
```

但 Java 代码传参：

```java
redisTemplate.execute(
    LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT,
    List.of(RedisKeyConstant.semaphoreKey(name)),     // KEYS[1] 只有 1 个
    RedisKeyConstant.semaphoreChannel(name),           → ARGV[1] ← 频道名!
    String.valueOf(permits)                            → ARGV[2] ← 释放数量!
);
```

Lua 脚本执行 `incrby KEYS[1] ARGV[1]` 时，用**频道名字符串**（如 `redisson_sc:{semaphore}:xxx`）做增量操作，Redis 当然报 `ERR value is not an integer`。

#### 修复

```java
// KEYS 传入 2 个，频道名不再占用 ARGV
redisTemplate.execute(
    LuaScriptRegistry.SEMAPHORE_RELEASE_SCRIPT,
    List.of(RedisKeyConstant.semaphoreKey(name), RedisKeyConstant.semaphoreChannel(name)),
    String.valueOf(permits)
);
```

**教训**：`redisTemplate.execute(script, keys, args...)` 中，参数的顺序是：keys → ARGV。多 key 脚本一定要把**所有 KEYS 都放在 `List.of()` 里**，不要漏。

### 3.2 `assertNull(hasKey())` 批量搞乱括号

用 sed 批量替换 `assertNull(hasKey(...))` → `assertFalse(Boolean.TRUE.equals(hasKey(...)))` 时，正则没写好，导致：

```
assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(key));   ← 少一个 )
assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(key))))); ← 多一个 )
assertNotNull(redisTemplate.hasKey(key)));                      ← 被误伤
```

手动修了 5 轮 sed 才稳定，前后改了 20 多处。

**教训**：批量替换带嵌套括号的表达式，慎用 sed。安全的做法是逐文件手改或用 IDE 的 refactor 工具。

### 3.3 `concurrencyLockAndTryLock()` 竞态条件 — 偶发失败

#### 发现过程

`RedisUtilRealTest.concurrencyLockAndTryLock()` 偶尔失败，`assertTrue(r.isFailure())` 期望锁被占，但 `executeTryLock` 成功返回了。

#### 根因

测试代码用 `CountDownLatch` 协调线程时序，但信号发早了：

```java
// 问题代码：ready.countDown() 在获取锁之前
Thread holder = new Thread(() -> {
    ready.countDown();                              // ← 锁还没拿到就发信号
    RedisLockUtil.executeLock(key, 5, SECONDS, () -> {
        TimeUnit.SECONDS.sleep(3);                   // 持锁 3 秒
    });
});
holder.start();

ready.await();                                      // ← 主线程收到信号，以为锁已持有
RedisLockResult<String> r = RedisLockUtil.executeTryLock(key, 1, () -> "try");
assertTrue(r.isFailure());                          // 但锁可能还没拿到，tryLock 成功 → ❌
```

`executeLock` 是**阻塞方法**，内部通过 `lock()` 等待获取锁。在执行回调之前，锁还不一定被持有。而 `ready.countDown()` 放在 `executeLock` 调用之前，信号比锁的状态提前了。

#### 修复

将 `ready.countDown()` 移到 `executeLock` 的回调内部，确保信号在锁已获取后才发出：

```java
Thread holder = new Thread(() -> {
    RedisLockUtil.executeLock(key, 5, SECONDS, (VoidSupplier) () -> {
        ready.countDown();                          // ← 锁已获取才发信号
        TimeUnit.SECONDS.sleep(3);
    });
});
```

`executeLock` 的回调只在锁获取成功后执行，所以 `countDown` 在这里触发保证「信号 = 锁已持有」。

#### 教训

用 `CountDownLatch` 做线程协调时，一定要确认信号点是否在**关键操作完成之后**。对于锁测试来说，"锁已持有"的判定点应该是回调内部、而非 `lock()` 调用之前。

---

## 4. TTL 过期策略改造

### 4.1 发现问题

`DistributedSemaphore` 和 `DistributedCountDownLatch` 的 Lua 脚本**全都没有过期时间**。如果业务方创建了信号量或 latch 后忘记清理，Redis 内存只增不减。

### 4.2 排查结果

**带 TTL 的（安全）：**

| 脚本 | 过期机制 |
|------|----------|
| `LOCK_SCRIPT` | `PEXPIRE KEYS[1] ARGV[1]` |
| `UNLOCK_SCRIPT` | 部分解锁 PEXPIRE，完全释放 DEL |
| `RENEW_SCRIPT` | PEXPIRE 续期 |
| 读写锁所有脚本 | 均含 PEXPIRE |

**不带 TTL 的（问题）：**

| 脚本 | 问题 |
|------|------|
| `SET_IF_ABSENT_SCRIPT` | 创建 Key 无过期 → 内存泄漏 |
| `SEMAPHORE_ACQUIRE_SCRIPT` | DECRBY 不刷新 TTL → Key 可能过期 |
| `SEMAPHORE_RELEASE_SCRIPT` | INCRBY 创建 Key 无过期 |
| `LATCH_COUNTDOWN_SCRIPT` | DECR 创建 Key 无过期 |

### 4.3 改什么不改什么

原则：
1. **Lua 脚本里不硬编码 TTL** → TTL 通过 ARGV 参数传入
2. **构造器注入** TTL 而非 setter → `DistributedSemaphore(redisTemplate, ttlSeconds)`
3. 默认 7 天（604800 秒），和 Redisson 保持一致
4. **不修改 `RedisConfig.java`** → 不需要额外 Spring Bean

```java
public class DistributedSemaphore {
    public static final long DEFAULT_TTL_SECONDS = 604800L;
    private final long defaultTtlSeconds;

    public DistributedSemaphore(StringRedisTemplate redisTemplate) {
        this(redisTemplate, DEFAULT_TTL_SECONDS);
    }

    public DistributedSemaphore(StringRedisTemplate redisTemplate, long defaultTtlSeconds) {
        this.redisTemplate = redisTemplate;
        this.defaultTtlSeconds = defaultTtlSeconds > 0 ? defaultTtlSeconds : DEFAULT_TTL_SECONDS;
    }
}
```

### 4.4 改造后的 Lua 脚本

```lua
-- SET_IF_ABSENT_SCRIPT（信号量/latch 初始化）
if redis.call('exists', KEYS[1]) == 0 then
    redis.call('set', KEYS[1], ARGV[1]);
    redis.call('expire', KEYS[1], ARGV[2]);   -- TTL 从参数传入
    return 1;
end;
return 0;

-- SEMAPHORE_ACQUIRE_SCRIPT（获取许可）
local value = redis.call('get', KEYS[1]);
if (value ~= false and tonumber(value) >= tonumber(ARGV[1])) then
    redis.call('decrby', KEYS[1], ARGV[1]);
    redis.call('expire', KEYS[1], ARGV[2]);   -- 每次获取刷新 TTL
    return 1;
end;
return 0;

-- SEMAPHORE_RELEASE_SCRIPT（释放许可）
local val = redis.call('incrby', KEYS[1], ARGV[1]);
redis.call('expire', KEYS[1], ARGV[2]);       -- 每次释放刷新 TTL
redis.call('publish', KEYS[2], 'release');
return val;

-- LATCH_COUNTDOWN_SCRIPT（递减计数）⚠️ 最复杂
if redis.call('exists', KEYS[1]) == 1 then       -- ① 先检查 Key 是否存在
    local val = redis.call('decr', KEYS[1]);      -- ② 递减
    redis.call('expire', KEYS[1], ARGV[1]);       -- ③ 刷新 TTL
    if val <= 0 then
        redis.call('publish', KEYS[2], 0);         -- ④ 归零通知
    end;
    return val;
end;
redis.call('publish', KEYS[2], 0);  -- Key已过期，唤醒等待线程
return nil;
```

### 4.5 LATCH 坑的分析过程（重点）

#### 第一版：直接加 expire

```
trySetCount("x", 5)      → Key=5, TTL=7天
countDown("x") ×3次       → Key=2
7天后 Key 过期
countDown("x")            → DECR 创建 Key=-1 → 触发归零通知
await() 看到 -1 ≤ 0       → ❌ 误判为已归零
```

`DECR` 在过期 Key 上创建 -1，`await()` 看到 -1 ≤ 0 就认为 latch 已完成 → **逻辑错误**。

#### 第二版：去掉 expire（不让 countDown 刷新 TTL）

Key 由 `trySetCount` 创建时带 7 天 TTL。`countDown` 不设 expire，Key 就不会意外续期，7 天后自动过期。过期后 `DECR` 不管是否创建 -1，Key 已经不存在了。

但 `DECR` 在 Key 不存在时**会自动创建 Key 并设值为 -1**，没有区别。

```lua
-- 去掉 expire 也解决不了问题
local val = redis.call('decr', KEYS[1]);  -- Key 不存在 → 创建 key = -1
```

#### 第三版：加 exists 保护（但忘了 publish）

```lua
if redis.call('exists', KEYS[1]) == 1 then
    ...
end;
return nil;  -- Key 过期，什么都不做
```

Key 过期后 `countDown` 不做任何操作 → **等待线程永久阻塞！**

因为 `await()` 里 `subscriber.await(Long.MAX_VALUE)` 在等永远不来的通知。

#### 最终版：exists + publish

```lua
if redis.call('exists', KEYS[1]) == 1 then
    -- 正常递减流程 ...
end;
redis.call('publish', KEYS[2], 0);  -- 过期也要通知等待线程
return nil;
```

三层保障：

| 层 | 做什么 | 防止什么 |
|---|--------|----------|
| `exists` 检查 | Key 过期后不执行 DECR | 防止 DECR 创建 -1 脏数据 |
| `expire` 刷新 TTL | 每次 countDown 续期 | 活跃 latch 不会 7 天后过期 |
| 过期后 publish | 通知等待线程 | 等待线程不会永久阻塞 |

### 4.6 信号量为什么不需要 exists 保护

信号量的 `release` 在过期 Key 上执行 `INCRBY`：

```
trySetPermits("s", 5)    → Key=5
tryAcquire("s", 5)       → Key=0
7天后 Key 过期
release("s", 3)          → INCRBY 创建 Key=3 ✅ 释放许可，语义正确
```

`INCRBY` 产生**正数值**，语义上是"释放了几个许可"，无论之前状态如何都是合理操作。和 latch `DECR` 产生 -1 触发归零通知有本质区别。

### 4.7 最终 TTL 策略

| 脚本 | exists 保护 | expire | 原因 |
|------|:----------:|:------:|------|
| `SET_IF_ABSENT` | ✅ 本身就是 | ✅ 7天 | 初始化 Key |
| `SEMAPHORE_ACQUIRE` | ❌ 不需要 | ✅ 7天 | 刷新 TTL，防活跃信号量过期 |
| `SEMAPHORE_RELEASE` | ❌ 不需要 | ✅ 7天 | INCRBY 创建正数安全 |
| `LATCH_COUNTDOWN` | ✅ **必须有** | ✅ 7天 | 防 DECR -1 误判归零 + 过期 publish |

---

## 5. 测试覆盖

### 5.1 测试文件清单

| 文件 | 测试数 | 说明 |
|------|:------:|------|
| `DistributedLockRealTest.java` | 37 | 加解锁、可重入(3级)、互斥、非法解锁、Watchdog(启动/续期/停止/多锁计数)、Pub/Sub等待(提前释放/超时)、lock()阻塞、并发10线程、Hash数据结构验证、TTL验证、哈希标签 |
| `DistributedSemaphoreRealTest.java` | 29 | 初始化(存在/重复/负数/零)、获取释放(充足/不足/零)、完整生命周期、阻塞acquire、超时等待(成功/超时/立即)、并发竞争(10线程轮转/全部归还)、中断处理、**TTL过期(acquire/available/release/续期)** |
| `DistributedCountDownLatchRealTest.java` | 30 | 初始化(存在/重复/负数/零)、计数查询、递减(归零/超零/幂等)、await(阻塞/超时/归零/提前返回)、多线程Pub/Sub通知(5个全唤醒)、逐步递减3阶段、中断处理、**TTL过期(countDown不创建-1/await立即返回/续期)** |
| `DistributedReadWriteLockRealTest.java` | 29 | 写/读锁基础、写写互斥、读读共享(多实例)、可重入(3级)、非法解锁、读写互斥(双向)、等待获取(写释放后读/多读+写等待)、锁降级(单次/多次/阻塞其他写)、超时等待、数据结构验证、TTL过期 |
| `PubSubSubscriberRealTest.java` | 17 | 订阅接收消息、await超时、先发布后订阅不接收、多订阅者(5个全收)、独立频道隔离、close清理/幂等/关闭后不接收、try-with-resources、模拟lock通知流程、模拟latch通知流程、中断处理、**哈希标签频道** |
| `utils/RedisUtilRealTest.java` | 23 | `RedisLockUtil` 外观层测试：executeTryLock 6个重载全部验证、Supplier/VoidSupplier变体、锁占失败、异常解锁(exception→unlock→其他线程可重入)、executeLock阻塞(Supplier/VoidSupplier)、嵌套重入、并发竞争、executeLock+executeTryLock互斥、边界(wait=0/负数)、不启动Watchdog |
| **合计** | **165** | |

### 5.2 发现的问题清单

| # | 问题 | 类型 | 根因 | 修复位置 |
|---|------|------|------|----------|
| 1 | `release()` 报 `value not integer` | 生产代码 Bug | 频道名传给 ARGV 而非 KEYS[2] | `DistributedSemaphore.java:104` |
| 2 | `tryLock` 调用混淆 | 测试代码 Bug | 3 参 vs 4 参方法签名，等待时间误当过期时间 | `DistributedLockRealTest.java:6处` |
| 3 | JVM fork 崩溃 | 环境问题 | 内存不足，G1 GC 分配 256M 失败 | `pom.xml argLine` |
| 4 | Mockito 加载失败 | 环境问题 | Java 21 禁止动态 Agent 加载 | `pom.xml argLine` |
| 5 | Latch DECR 创建 -1 | 生产代码 Bug | 过期 Key 上 DECR 产生 -1 误判归零 | `LuaScriptRegistry.java LATCH_COUNTDOWN_SCRIPT` |
| 6 | Latch 过期不 publish | 生产代码 Bug | exists 保护后忘了通知等待线程 | `LuaScriptRegistry.java LATCH_COUNTDOWN_SCRIPT` |
| 7 | assertNull(hasKey) | 测试代码 Bug | hasKey 返回 Boolean 非 null | `*RealTest.java 14处` |
| 8 | concurrencyLockAndTryLock 偶发失败 | 测试代码 Bug | CountDownLatch 信号点在锁获取之前 | `RedisUtilRealTest.java:319` |

---

## 6. 运行命令

```bash
# 编译
mvn compile
mvn test-compile

# 运行单个测试
mvn test -Dtest=DistributedLockRealTest

# 运行多个测试（逗号分隔）
mvn test -Dtest="DistributedSemaphoreRealTest,DistributedCountDownLatchRealTest"

# 内存不足时的降级方案
rm -rf target/surefire-reports/
MAVEN_OPTS="-Djdk.attach.allowAttachSelf=true -Xms128m -Xmx256m" mvn test -Dtest=TestName -DforkCount=0

# 运行所有已有 Mock 测试
mvn test
```

---

## 7. 遗留问题

### 7.1 Pub/Sub 在 Cluster 模式下不可靠

`PubSubSubscriber` 通过 `connection.subscribe()` 连接到**随机集群节点**。锁释放时 `UNLOCK_SCRIPT` 中的 `PUBLISH` 在哈希槽归属节点上执行。订阅者与发布者在不同节点时，通知丢失。

`tryLock` 的等待机制退化为**轮询**（每 1 秒重试一次）：

```java
// waitForLock 中的退路
subscriber.await(sleepMs);  // 运气好 Pub/Sub 通知到，否则等超时
// 超时后 while 循环重试 evalLock → 最终总能拿到锁
```

对于 `lock()` 阻塞方法，每次 `await` 超时后重新订阅并检查锁状态。

**影响：** 锁等待时间可能从毫秒级退化到秒级。

### 7.2 锁部分解锁 TTL 刷新

`UNLOCK_SCRIPT` 在部分解锁（仍有重入）时使用 `DEFAULT_LEASE_TIME_MS`（30s）刷新 TTL，而不是使用原始 `leaseTime`。这是复刻 Redisson 的行为，但不是最精确的设计。

### 7.3 `LATCH_COUNTDOWN_SCRIPT` 不 DEL 归零 Key

`countDown` 归零后只 PUBLISH 通知，不主动 DEL Key。Key 由 **7 天 TTL** 自动清理。如果业务上需要立即清理，需自行调用 `redisTemplate.delete()`。
