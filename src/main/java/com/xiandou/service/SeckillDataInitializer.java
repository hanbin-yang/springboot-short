package com.xiandou.service;

import com.xiandou.mapper.SeckillActivityMapper;
import com.xiandou.model.SeckillActivity;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Component
@EnableScheduling
public class SeckillDataInitializer implements ApplicationRunner {

    private final SeckillActivityMapper activityMapper;
    private final SeckillStockService stockService;

    public SeckillDataInitializer(SeckillActivityMapper activityMapper, SeckillStockService stockService) {
        this.activityMapper = activityMapper;
        this.stockService = stockService;
    }

    @Override
    public void run(ApplicationArguments args) {
        // 插入演示数据（仅在表为空时）
        if (activityMapper.selectCount(null) == 0) {
            LocalDateTime now = LocalDateTime.now();
            activityMapper.insert(new SeckillActivity(1L, BigDecimal.valueOf(9.9), 100, 100,
                    now.minusMinutes(30), now.plusHours(2), 1)); // 进行中
            activityMapper.insert(new SeckillActivity(5L, BigDecimal.valueOf(19.9), 50, 50,
                    now.minusHours(1), now.plusHours(1), 1));   // 进行中
            activityMapper.insert(new SeckillActivity(7L, BigDecimal.valueOf(29.9), 30, 30,
                    now.plusHours(1), now.plusHours(3), 0));    // 待开始
        }
        // 预热所有进行中活动的库存
        List<SeckillActivity> active = activityMapper.selectList(null);
        for (SeckillActivity a : active) {
            if (a.getStatus() == 1 && a.getEndTime() != null) {
                stockService.warmUpStock(a.getId(), a.getRemainStock(), a.getEndTime());
            }
        }
    }

    /** 每 30 秒刷新库存预热 */
    @Scheduled(fixedRate = 30000)
    public void refreshStock() {
        List<SeckillActivity> all = activityMapper.selectList(null);
        LocalDateTime now = LocalDateTime.now();
        for (SeckillActivity a : all) {
            if (a.getStatus() == 1 && a.getStartTime() != null && a.getEndTime() != null
                    && now.isAfter(a.getStartTime()) && now.isBefore(a.getEndTime())) {
                // 检查 Redis 是否已有库存
                if (stockService.getStock(a.getId()) <= 0) {
                    stockService.warmUpStock(a.getId(), a.getRemainStock(), a.getEndTime());
                }
            }
            // 已结束的活动清理
            if (a.getEndTime() != null && now.isAfter(a.getEndTime()) && a.getStatus() != 2) {
                a.setStatus(2);
                activityMapper.updateById(a);
                stockService.clearStock(a.getId());
            }
        }
    }
}
