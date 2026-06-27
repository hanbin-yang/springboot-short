package com.xiandou.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@TableName("seckill_activity")
public class SeckillActivity {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long productId;
    private BigDecimal seckillPrice;
    private Integer totalStock;
    private Integer remainStock;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Integer status; // 0-待开始 1-进行中 2-已结束

    public SeckillActivity() {}

    public SeckillActivity(Long productId, BigDecimal seckillPrice, Integer totalStock,
                           Integer remainStock, LocalDateTime startTime, LocalDateTime endTime, Integer status) {
        this.productId = productId;
        this.seckillPrice = seckillPrice;
        this.totalStock = totalStock;
        this.remainStock = remainStock;
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
    }

    // getters / setters
    public Long getId() { return id; } public void setId(Long id) { this.id = id; }
    public Long getProductId() { return productId; } public void setProductId(Long productId) { this.productId = productId; }
    public BigDecimal getSeckillPrice() { return seckillPrice; } public void setSeckillPrice(BigDecimal seckillPrice) { this.seckillPrice = seckillPrice; }
    public Integer getTotalStock() { return totalStock; } public void setTotalStock(Integer totalStock) { this.totalStock = totalStock; }
    public Integer getRemainStock() { return remainStock; } public void setRemainStock(Integer remainStock) { this.remainStock = remainStock; }
    public LocalDateTime getStartTime() { return startTime; } public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }
    public LocalDateTime getEndTime() { return endTime; } public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }
    public Integer getStatus() { return status; } public void setStatus(Integer status) { this.status = status; }
}
