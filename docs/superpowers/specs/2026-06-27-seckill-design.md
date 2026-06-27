# 秒杀功能设计文档

> 基于现有鲜豆商城（Spring Boot + Vue 3 + Redis Cluster）实现典型秒杀功能。
> H2 内存数据库 + MyBatis-Plus，真实 SQL 事务但不依赖外部数据库。

---

## 1. 架构概览

```
┌──────────────┐     ┌─────────────────────────┐     ┌──────────────┐
│   Vue 3 前端  │ ──> │   Spring Boot 后端       │ ──> │   H2 (内存)   │
│              │     │                        │     │   seckill    │
│ SeckillList  │     │  SeckillController      │     │ ─ 活动表      │
│ SeckillDetail │     │  SeckillService         │     │ ─ 订单表      │
└──────────────┘     │  SeckillStockService     │     └──────────────┘
                     │         │                │
                     │    ┌────▼────┐           │
                     │    │ Redis   │           │
                     │    │ Cluster │           │
                     │    │ 库存扣减 │           │
                     │    └─────────┘           │
                     └─────────────────────────┘
```

**核心原则：** Redis 做原子扣减 + 防重，H2 做订单持久化。

---

## 2. 后端设计

### 2.1 新增依赖

```xml
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>com.baomidou</groupId>
    <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
    <version>3.5.9</version>
</dependency>
```

### 2.2 application.yml 新增

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:seckill;DB_CLOSE_DELAY=-1
    driver-class-name: org.h2.Driver

mybatis-plus:
  configuration:
    map-underscore-to-camel-case: true
  global-config:
    db-config:
      id-type: auto
```

### 2.3 数据模型

#### seckill_activity（秒杀活动表）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | BIGINT (自增) | 主键 |
| product_id | BIGINT | 关联商品 ID（复用现有 Product） |
| seckill_price | DECIMAL(10,2) | 秒杀价 |
| total_stock | INT | 秒杀总库存 |
| remain_stock | INT | 剩余库存（DB 记录，实际扣减在 Redis） |
| start_time | DATETIME | 开始时间 |
| end_time | DATETIME | 结束时间 |
| status | INT | 0-待开始 1-进行中 2-已结束 |

#### seckill_order（秒杀订单表）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | BIGINT (自增) | 主键 |
| activity_id | BIGINT | 关联秒杀活动 |
| product_id | BIGINT | 关联商品 ID |
| user_id | BIGINT | 用户 ID |
| price | DECIMAL(10,2) | 实际成交价（秒杀价） |
| create_time | DATETIME | 下单时间 |
| status | VARCHAR(20) | CREATED / PAID / CANCELLED |

### 2.4 核心流程

```
POST /api/seckill/{activityId}
  1. 校验活动：存在 + 时间段内 + status=1
  2. 防重校验：同一个用户对同一个活动只能秒杀一次
  3. Redis Lua 原子扣减库存：DECRBY seckill:stock:{activityId} 1
  4. H2 插入订单 + UPDATE remainStock
  5. 返回秒杀结果
```

### 2.5 Lua 脚本：原子扣减

```lua
-- KEYS[1] = seckill:stock:{activityId}
-- ARGV[1] = 扣减数量
local stock = redis.call('decrby', KEYS[1], ARGV[1]);
if stock >= 0 then
    return stock;
end;
redis.call('incrby', KEYS[1], ARGV[1]);
return -1;
```

### 2.6 库存预热

- **启动时：** `ApplicationRunner` 查询所有"进行中"的活动，将 `remainStock` 写入 Redis，TTL 设为活动结束时间 + 1 小时
- **定时刷新：** `@Scheduled` 每 30 秒检查即将开始/已结束的活动，做预热和清理

### 2.7 异常回滚

```
Redis 扣减成功 → H2 写入失败 → INCRBY 回滚 Redis 库存
```

### 2.8 API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/seckill/list | 秒杀活动列表（当前可参与的） |
| GET | /api/seckill/{activityId} | 秒杀活动详情（含商品信息、倒计时） |
| POST | /api/seckill/{activityId} | 执行秒杀下单 |

---

## 3. 前端设计

### 3.1 路由

```js
{ path: '/seckill', name: 'SeckillList', component: () => import('../views/SeckillList.vue') },
{ path: '/seckill/:id', name: 'SeckillDetail', component: () => import('../views/SeckillDetail.vue') },
```

### 3.2 API 层

`src/api/seckill.js`

```js
import api from './index'

export const getSeckillList = () => api.get('/seckill/list')

export const getSeckillDetail = (id) => api.get(`/seckill/${id}`)

export const flashSale = (activityId) => api.post(`/seckill/${activityId}`)
```

### 3.3 SeckillList.vue — 秒杀列表页

**功能：** 展示当前可参与的秒杀活动列表

**页面结构：**
```
顶部标题栏 — "限时秒杀"
┌────────────────────────────────────────┐
│ 活动卡片1                              │
│ ┌──────┐  商品名          秒杀价 ¥29.9  │
│ │ 图片  │  ｜                    ｜     │
│ │      │  原价 ¥99.9  剩余 15/100    │
│ └──────┘  [ 立即抢购 ]    ⏰ 02:30:15 │
├────────────────────────────────────────┤
│ 活动卡片2（同上）                       │
├────────────────────────────────────────┤
│ 活动卡片3（同上）                       │
└────────────────────────────────────────┘
底部 TabBar（复用现有 TabBar.vue）
```

**状态处理：**
- **加载中：** 骨架屏或 loading 提示
- **空列表：** "暂无秒杀活动"
- **已结束：** 卡片显示"已结束"灰色状态
- **倒计时：** 使用现有 `composables/useTime.js` 的倒计时能力

### 3.4 SeckillDetail.vue — 秒杀详情页

**功能：** 商品详情 + 秒杀信息 + 立即抢购

**页面结构：**
```
┌────────────────────────────────────────┐
│ ← 返回                                │
│                                        │
│        ┌──────────────┐               │
│        │   商品大图     │              │
│        └──────────────┘               │
│                                        │
│  商品名称                              │
│  ｜                                    │
│  ¥ 秒杀价        ¥ 原价 (划线)         │
│                                        │
│  ⏰ 距结束 02:30:15                     │
│                                        │
│  已抢 85 件   剩余 15 件               │
│  ████████████░░░░  (进度条)            │
│                                        │
│  ┌────────────────────────────────┐    │
│  │        立即抢购                 │    │
│  └────────────────────────────────┘    │
└────────────────────────────────────────┘
```

**状态处理：**
- **活动未开始：** 按钮置灰，显示"活动尚未开始"+ 倒计时
- **活动进行中：** 按钮高亮，显示"立即抢购"
- **活动已结束：** 按钮置灰，显示"活动已结束"
- **库存为0：** 按钮置灰，显示"已售罄"
- **秒杀成功：** Toast 提示 + 跳转到订单页
- **秒杀失败：** Toast 提示错误原因
- **重复秒杀：** Toast 提示"您已参与该活动"

### 3.5 TabBar 添加秒杀入口

在 `constants.js` 中 tab 配置添加秒杀页面入口。

---

## 4. 组件清单

| 文件 | 说明 |
|------|------|
| `backend: SeckillController.java` | 秒杀接口控制器 |
| `backend: SeckillService.java` | 秒杀核心业务逻辑 |
| `backend: SeckillActivity.java` | 秒杀活动实体 |
| `backend: SeckillOrder.java` | 秒杀订单实体 |
| `backend: SeckillActivityMapper.java` | MyBatis-Plus Mapper |
| `backend: SeckillOrderMapper.java` | MyBatis-Plus Mapper |
| `backend: SeckillDataInitializer.java` | 启动初始化 + 定时任务 |
| `frontend: api/seckill.js` | 秒杀 API 封装 |
| `frontend: views/SeckillList.vue` | 秒杀列表页 |
| `frontend: views/SeckillDetail.vue` | 秒杀详情页 |

---

## 5. 测试策略

利用已有的 Redis Cluster + H2 内存数据库，写集成测试 `SeckillRealTest.java`：

| 测试场景 | 验证点 |
|----------|--------|
| 正常秒杀 | Redis 库存扣减 + H2 订单创建 + 库存一致 |
| 重复秒杀 | 同一用户 → 返回失败 |
| 并发秒杀 | 多线程同时抢购 → 不超卖 |
| 库存不足 | 超过库存的请求 → "已售罄" |
| 活动未开始 / 已结束 | 时间段外返回错误 |
