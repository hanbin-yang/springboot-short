package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.model.SeckillOrder;
import com.xiandou.service.SeckillService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/seckill")
public class SeckillController {

    private final SeckillService seckillService;

    public SeckillController(SeckillService seckillService) {
        this.seckillService = seckillService;
    }

    @GetMapping("/list")
    public Result<List<Map<String, Object>>> list() {
        return Result.success(seckillService.getList());
    }

    @GetMapping("/{activityId}")
    public Result<Map<String, Object>> detail(@PathVariable Long activityId) {
        Map<String, Object> vo = seckillService.getDetail(activityId);
        if (vo == null) {
            return Result.fail("活动不存在");
        }
        return Result.success(vo);
    }

    @PostMapping("/{activityId}")
    public Result<SeckillOrder> flashSale(@PathVariable Long activityId,
                                          @RequestHeader(value = "X-User-Id", defaultValue = "1") Long userId) {
        return seckillService.flashSale(userId, activityId);
    }
}
