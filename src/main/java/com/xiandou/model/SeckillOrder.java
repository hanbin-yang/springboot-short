package com.xiandou.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@TableName("seckill_order")
public class SeckillOrder {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long activityId;
    private Long productId;
    private Long userId;
    private BigDecimal price;
    private LocalDateTime createTime;
    private String status; // CREATED / PAID / CANCELLED

    public SeckillOrder() {}

    public SeckillOrder(Long activityId, Long productId, Long userId, BigDecimal price) {
        this.activityId = activityId;
        this.productId = productId;
        this.userId = userId;
        this.price = price;
        this.createTime = LocalDateTime.now();
        this.status = "CREATED";
    }

    // getters / setters
    public Long getId() { return id; } public void setId(Long id) { this.id = id; }
    public Long getActivityId() { return activityId; } public void setActivityId(Long activityId) { this.activityId = activityId; }
    public Long getProductId() { return productId; } public void setProductId(Long productId) { this.productId = productId; }
    public Long getUserId() { return userId; } public void setUserId(Long userId) { this.userId = userId; }
    public BigDecimal getPrice() { return price; } public void setPrice(BigDecimal price) { this.price = price; }
    public LocalDateTime getCreateTime() { return createTime; } public void setCreateTime(LocalDateTime createTime) { this.createTime = createTime; }
    public String getStatus() { return status; } public void setStatus(String status) { this.status = status; }
}
