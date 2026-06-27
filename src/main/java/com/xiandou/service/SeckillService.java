package com.xiandou.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.xiandou.common.Result;
import com.xiandou.mapper.SeckillActivityMapper;
import com.xiandou.mapper.SeckillOrderMapper;
import com.xiandou.model.Product;
import com.xiandou.model.SeckillActivity;
import com.xiandou.model.SeckillOrder;
import com.xiandou.model.Store;
import com.xiandou.redis.core.DistributedLock;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class SeckillService {

    private final SeckillActivityMapper activityMapper;
    private final SeckillOrderMapper orderMapper;
    private final SeckillStockService stockService;
    private final MockDataService mockData;
    private final DistributedLock distributedLock;

    public SeckillService(SeckillActivityMapper activityMapper, SeckillOrderMapper orderMapper,
                           SeckillStockService stockService, MockDataService mockData,
                           DistributedLock distributedLock) {
        this.activityMapper = activityMapper;
        this.orderMapper = orderMapper;
        this.stockService = stockService;
        this.mockData = mockData;
        this.distributedLock = distributedLock;
    }

    /** 秒杀活动列表（含商品信息） */
    public List<Map<String, Object>> getList() {
        List<SeckillActivity> activities = activityMapper.selectList(null);
        LocalDateTime now = LocalDateTime.now();
        return activities.stream()
                .filter(a -> a.getStartTime() != null && a.getEndTime() != null)
                .filter(a -> now.isAfter(a.getStartTime()) && now.isBefore(a.getEndTime()))
                .map(this::toVO)
                .collect(Collectors.toList());
    }

    /** 秒杀活动详情 */
    public Map<String, Object> getDetail(Long activityId) {
        SeckillActivity activity = activityMapper.selectById(activityId);
        if (activity == null) return null;
        return toVO(activity);
    }

    /** 执行秒杀 */
    @Transactional
    public Result<SeckillOrder> flashSale(Long userId, Long activityId) {
        // 1. 校验活动
        SeckillActivity activity = activityMapper.selectById(activityId);
        if (activity == null) {
            return Result.fail("活动不存在");
        }
        LocalDateTime now = LocalDateTime.now();
        if (now.isBefore(activity.getStartTime())) {
            return Result.fail("活动尚未开始");
        }
        if (now.isAfter(activity.getEndTime())) {
            return Result.fail("活动已结束");
        }

        // 2. 防重校验：同一用户对同一活动只能秒杀一次
        String lockKey = "seckill:lock:" + userId + ":" + activityId;
        boolean locked = distributedLock.tryLock(lockKey, 0, 3, TimeUnit.SECONDS);
        if (!locked) {
            return Result.fail("请勿重复提交");
        }
        try {
            long count = orderMapper.selectCount(
                    new LambdaQueryWrapper<SeckillOrder>()
                            .eq(SeckillOrder::getActivityId, activityId)
                            .eq(SeckillOrder::getUserId, userId)
            );
            if (count > 0) {
                return Result.fail("您已参与该活动");
            }

            // 3. Redis 原子扣减
            long result = stockService.deductStock(activityId, 1);
            if (result < 0) {
                return Result.fail("已售罄");
            }

            // 4. H2 插入订单 + 更新库存
            try {
                SeckillOrder order = new SeckillOrder(activityId, activity.getProductId(), userId, activity.getSeckillPrice());
                orderMapper.insert(order);
                activity.setRemainStock(activity.getRemainStock() - 1);
                activityMapper.updateById(activity);
                return Result.success(order);
            } catch (Exception e) {
                // 回滚 Redis 库存
                stockService.rollbackStock(activityId, 1);
                return Result.fail("下单失败，请重试");
            }
        } finally {
            distributedLock.unlock(lockKey);
        }
    }

    private Map<String, Object> toVO(SeckillActivity activity) {
        Map<String, Object> vo = new LinkedHashMap<>();
        vo.put("id", activity.getId());
        vo.put("productId", activity.getProductId());
        vo.put("seckillPrice", activity.getSeckillPrice());
        vo.put("totalStock", activity.getTotalStock());

        // 剩余库存优先从 Redis 获取，回退到 DB
        long redisStock = stockService.getStock(activity.getId());
        long remain = redisStock > 0 ? redisStock : activity.getRemainStock().longValue();
        vo.put("remainStock", Math.max(0, remain));

        vo.put("startTime", activity.getStartTime());
        vo.put("endTime", activity.getEndTime());
        vo.put("status", activity.getStatus());

        // 从 MockDataService 获取商品信息
        for (Store store : mockData.getStores()) {
            for (Product p : mockData.getProducts(store.getId().toString())) {
                if (p.getId().equals(activity.getProductId())) {
                    vo.put("productName", p.getName());
                    vo.put("productPrice", p.getPrice());
                    vo.put("productImage", p.getImage());
                    return vo;
                }
            }
        }
        return vo;
    }
}
