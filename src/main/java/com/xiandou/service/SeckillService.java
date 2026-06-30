package com.xiandou.service;

import com.xiandou.common.Result;
import com.xiandou.mapper.SeckillActivityMapper;
import com.xiandou.mapper.SeckillOrderMapper;
import com.xiandou.model.Product;
import com.xiandou.model.SeckillActivity;
import com.xiandou.model.SeckillOrder;
import com.xiandou.model.Store;
import static com.xiandou.service.SeckillStockService.STOCK_PREFIX;
import static com.xiandou.service.SeckillStockService.USER_SET_PREFIX;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class SeckillService {

    private final SeckillActivityMapper activityMapper;
    private final SeckillOrderMapper orderMapper;
    private final SeckillStockService stockService;
    private final MockDataService mockData;
    private final StringRedisTemplate redisTemplate;

    public SeckillService(SeckillActivityMapper activityMapper, SeckillOrderMapper orderMapper,
                           SeckillStockService stockService, MockDataService mockData,
                           StringRedisTemplate redisTemplate) {
        this.activityMapper = activityMapper;
        this.orderMapper = orderMapper;
        this.stockService = stockService;
        this.mockData = mockData;
        this.redisTemplate = redisTemplate;
    }

    /** 秒杀活动列表 */
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

    /** 执行秒杀（热路径：0 次 H2 读） */
    @Transactional
    public Result<SeckillOrder> flashSale(Long userId, Long activityId) {
        // 1. 校验活动是否有效 — Redis 中库存 Key 存在即说明活动进行中
        String stockKey = STOCK_PREFIX + activityId;
        Boolean keyExists = redisTemplate.hasKey(stockKey);
        if (keyExists == null || !keyExists) {
            return Result.fail("活动不存在或尚未开始");
        }

        // 2. 防重校验 — Redis Set 原子写入，不存在则加入
        String userSetKey = USER_SET_PREFIX + activityId;
        Long added = redisTemplate.opsForSet().add(userSetKey, String.valueOf(userId));
        if (added == null || added == 0) {
            return Result.fail("您已参与该活动");
        }

        // 3. Redis 原子扣减库存
        long stockResult = stockService.deductStock(activityId, 1);
        if (stockResult < 0) {
            // 扣减失败，回滚用户标记
            redisTemplate.opsForSet().remove(userSetKey, String.valueOf(userId));
            return Result.fail("已售罄");
        }

        // 4. H2 插入订单 + 更新库存（唯一落库操作）
        try {
            SeckillActivity activity = activityMapper.selectById(activityId);
            SeckillOrder order = new SeckillOrder(activityId, activity.getProductId(), userId, activity.getSeckillPrice());
            orderMapper.insert(order);
            activity.setRemainStock(activity.getRemainStock() - 1);
            activityMapper.updateById(activity);
            return Result.success(order);
        } catch (Exception e) {
            // 回滚 Redis 库存 + 用户标记
            stockService.rollbackStock(activityId, 1);
            redisTemplate.opsForSet().remove(userSetKey, String.valueOf(userId));
            return Result.fail("下单失败，请重试");
        }
    }

    private Map<String, Object> toVO(SeckillActivity activity) {
        Map<String, Object> vo = new LinkedHashMap<>();
        vo.put("id", activity.getId());
        vo.put("productId", activity.getProductId());
        vo.put("seckillPrice", activity.getSeckillPrice());
        vo.put("totalStock", activity.getTotalStock());

        long redisStock = stockService.getStock(activity.getId());
        long remain = redisStock > 0 ? redisStock : activity.getRemainStock().longValue();
        vo.put("remainStock", Math.max(0, remain));

        vo.put("startTime", activity.getStartTime().toString());
        vo.put("endTime", activity.getEndTime().toString());
        vo.put("status", activity.getStatus());

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
