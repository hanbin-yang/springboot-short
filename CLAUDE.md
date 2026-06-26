# CLAUDE.md — 鲜豆商城后端

## 命令

```bash
mvn spring-boot:run    # 启动服务器（端口 8080）
mvn compile            # 编译
mvn package            # 打包为 JAR
```

## 技术栈

- **Spring Boot 2.7.18**（Java 8）
- **Maven** 构建
- 无数据库，使用 `MockDataService` 内存数据源
- 统一返回 `Result<T>` 标准结构

## 架构说明

```
src/main/java/com/xiandou/
├── XiandouApplication.java        # 应用入口
├── common/
│   ├── Result.java                # 统一响应体 { code, message, data }
│   └── CommonResponseUtil.java    # 快捷创建 Result 的工具类
├── config/
│   └── CorsConfig.java            # 全局 CORS 跨域配置
├── controller/                    # REST 控制器（共 8 个）
│   ├── HomeController.java        # 首页：分类、店铺列表、站点信息
│   ├── StoreController.java       # 店铺：详情、分类、商品列表
│   ├── CartController.java        # 购物车：增删改查
│   ├── OrderController.java       # 订单：列表、创建
│   ├── AuthController.java        # 登录、注册
│   ├── AddressController.java     # 地址：增删改查
│   ├── SearchController.java      # 搜索店铺
│   └── ProfileController.java     # 个人信息
├── model/                         # 数据模型（共 7 个）
│   ├── Address.java
│   ├── CartItem.java
│   ├── Category.java
│   ├── Order.java（内含 OrderItem 内部类）
│   ├── Product.java
│   ├── Store.java
│   └── User.java
└── service/
    └── MockDataService.java       # 内存数据源 + @PostConstruct 初始化

src/main/resources/
└── application.yml                # 端口 8080，Jackson 配置
```

## 接口规范

所有接口统一返回 `Result<T>` 格式：

```json
{
  "code": "0",
  "message": "操作成功",
  "data": { ... }
}
```

- `code = "0"` 表示成功，其他值为错误码
- `data` 字段携带业务数据
- 前端 Axios 拦截器根据 `code` 自动解包或抛错

## 数据说明

- `MockDataService` 在 `@PostConstruct` 阶段初始化演示数据
- 所有数据存储在内存中，服务重启后重置
- 支持购物车增删改、订单创建等写操作（内存级别，重启即失）
