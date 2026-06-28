package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.mapper.SeckillActivityMapper;
import com.xiandou.mapper.SeckillOrderMapper;
import com.xiandou.model.SeckillActivity;
import com.xiandou.model.SeckillOrder;
import com.xiandou.service.SeckillStockService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.MethodName.class)
class SeckillRealTest {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private SeckillActivityMapper activityMapper;

    @Autowired
    private SeckillOrderMapper orderMapper;

    @Autowired
    private SeckillStockService stockService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private Long testActivityId;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        orderMapper.delete(null);
        activityMapper.delete(null);
        redisTemplate.delete(redisTemplate.keys("seckill:*"));

        // 插入测试活动（有效期 1 小时）
        LocalDateTime now = LocalDateTime.now();
        SeckillActivity activity = new SeckillActivity(
                1L, BigDecimal.valueOf(9.9), 10, 10,
                now.minusMinutes(5), now.plusHours(1), 1
        );
        activityMapper.insert(activity);
        testActivityId = activity.getId();
        stockService.warmUpStock(testActivityId, 10, now.plusHours(1));
    }

    @Test
    @DisplayName("列表-返回当前进行的秒杀活动")
    void listReturnsActiveActivities() {
        ResponseEntity<Result> resp = rest.getForEntity("/api/seckill/list", Result.class);
        assertEquals("0", resp.getBody().getCode());
        assertNotNull(resp.getBody().getData());
    }

    @Test
    @DisplayName("详情-返回秒杀商品信息")
    void detailReturnsActivityInfo() {
        ResponseEntity<Result> resp = rest.getForEntity("/api/seckill/" + testActivityId, Result.class);
        assertEquals("0", resp.getBody().getCode());
    }

    @Test
    @DisplayName("秒杀-正常流程成功")
    void flashSaleSuccess() {
        ResponseEntity<Result> resp = rest.postForEntity("/api/seckill/" + testActivityId, null, Result.class);
        assertEquals("0", resp.getBody().getCode());

        // 验证订单已创建
        List<SeckillOrder> orders = orderMapper.selectList(null);
        assertEquals(1, orders.size());
        assertEquals(testActivityId, orders.get(0).getActivityId());

        // 验证 Redis 库存扣减
        assertEquals(9, stockService.getStock(testActivityId));
    }

    @Test
    @DisplayName("秒杀-重复秒杀返回失败")
    void flashSaleDuplicateReturnsFail() {
        rest.postForEntity("/api/seckill/" + testActivityId, null, Result.class);
        ResponseEntity<Result> resp = rest.postForEntity("/api/seckill/" + testActivityId, null, Result.class);
        assertNotEquals("0", resp.getBody().getCode());
    }

    @Test
    @DisplayName("秒杀-库存不足返回已售罄")
    void flashSaleStockOut() throws InterruptedException {
        // 快速消耗完库存
        stockService.deductStock(testActivityId, 10);
        // 清理订单数据
        orderMapper.delete(null);

        ResponseEntity<Result> resp = rest.postForEntity("/api/seckill/" + testActivityId, null, Result.class);
        assertNotEquals("0", resp.getBody().getCode());
    }

    @Test
    @DisplayName("秒杀-活动不存在返回失败")
    void flashSaleActivityNotFound() {
        ResponseEntity<Result> resp = rest.postForEntity("/api/seckill/99999", null, Result.class);
        assertNotEquals("0", resp.getBody().getCode());
    }

    @Test
    @DisplayName("并发-多线程同时秒杀不超卖")
    void concurrentFlashSaleNoOversell() throws InterruptedException {
        int threadCount = 20;
        java.util.concurrent.CountDownLatch startSignal = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch doneSignal = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final long userId = 100L + i;
            new Thread(() -> {
                try {
                    startSignal.await();
                    // 通过内部执行避免重复秒杀锁的限制
                    // 使用 RestTemplate 无法模拟不同用户，这里直接用 service
                    // 简化验证：用 StockService 直接验证 Redis 不超卖
                    long r = stockService.deductStock(testActivityId, 1);
                    if (r >= 0) successCount.incrementAndGet();
                } catch (Exception ignored) {}
                doneSignal.countDown();
            }).start();
        }

        startSignal.countDown();
        doneSignal.await(10, java.util.concurrent.TimeUnit.SECONDS);

        assertTrue(successCount.get() <= 10, "不应超卖，成功数=" + successCount.get());
        // 还原库存
        for (int i = 0; i < successCount.get(); i++) stockService.rollbackStock(testActivityId, 1);
    }
}
