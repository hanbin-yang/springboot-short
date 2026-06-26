package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.model.CartItem;
import com.xiandou.service.MockDataService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cart")
public class CartController {

    private final MockDataService data;

    public CartController(MockDataService data) { this.data = data; }

    @GetMapping
    public Result<List<CartItem>> getCart() {
        return Result.success(data.getCart());
    }

    @PostMapping
    public Result<CartItem> addToCart(@RequestBody CartItem item) {
        return Result.success(data.addToCart(item));
    }

    @PutMapping("/{productId}")
    public Result<Void> updateCartItem(@PathVariable Long productId, @RequestBody Map<String, Integer> body) {
        data.updateCartItem(productId, body.getOrDefault("count", 1));
        return Result.success();
    }

    @DeleteMapping("/{productId}")
    public Result<Void> removeItem(@PathVariable Long productId) {
        data.updateCartItem(productId, 0);
        return Result.success();
    }

    @DeleteMapping
    public Result<Void> clearCart() {
        data.clearCart();
        return Result.success();
    }
}
