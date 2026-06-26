package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.model.Order;
import com.xiandou.service.MockDataService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final MockDataService data;

    public OrderController(MockDataService data) { this.data = data; }

    @GetMapping
    public Result<List<Order>> getOrders() {
        return Result.success(data.getOrders());
    }

    @PostMapping
    public Result<Order> createOrder(@RequestBody Order order) {
        return Result.success(data.createOrder(order));
    }
}
