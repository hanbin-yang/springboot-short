package com.xiandou.service;

import com.xiandou.redis.config.LuaScriptRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class SeckillStockService {

    static final String STOCK_PREFIX = "seckill:stock:";
    static final String USER_SET_PREFIX = "seckill:users:";

    private final StringRedisTemplate redisTemplate;

    public SeckillStockService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /** 预热库存到 Redis */
    public void warmUpStock(Long activityId, Integer remainStock, LocalDateTime endTime) {
        String key = stockKey(activityId);
        redisTemplate.opsForValue().set(key, String.valueOf(remainStock));
        // TTL = 活动结束时间 + 1 小时兜底
        long ttl = Duration.between(LocalDateTime.now(), endTime).getSeconds() + 3600;
        long ttlSec = Math.max(ttl, 3600);
        redisTemplate.expire(key, ttlSec, TimeUnit.SECONDS);
        // 同步设置用户去重 Set 的过期时间
        redisTemplate.expire(USER_SET_PREFIX + activityId, ttlSec, TimeUnit.SECONDS);
    }

    /** Redis 原子扣减库存，返回值 >= 0 成功，-1 失败 */
    public long deductStock(Long activityId, int count) {
        Long result = redisTemplate.execute(
                LuaScriptRegistry.SECKILL_DEDUCT_SCRIPT,
                List.of(stockKey(activityId)),
                String.valueOf(count)
        );
        return result != null ? result : -1;
    }

    /** 回滚 Redis 库存（H2 写入失败时调用） */
    public void rollbackStock(Long activityId, int count) {
        redisTemplate.opsForValue().increment(stockKey(activityId), count);
    }

    /** 获取 Redis 当前库存 */
    public long getStock(Long activityId) {
        String val = redisTemplate.opsForValue().get(stockKey(activityId));
        return val == null ? 0 : Long.parseLong(val);
    }

    /** 清理 Redis 库存 */
    public void clearStock(Long activityId) {
        redisTemplate.delete(stockKey(activityId));
    }

    private String stockKey(Long activityId) {
        return STOCK_PREFIX + activityId;
    }
}
