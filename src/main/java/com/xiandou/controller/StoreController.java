package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.model.Product;
import com.xiandou.model.Store;
import com.xiandou.service.MockDataService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/stores")
public class StoreController {

    private final MockDataService data;

    public StoreController(MockDataService data) { this.data = data; }

    @GetMapping("/{id}")
    public Result<Store> getStore(@PathVariable Long id) {
        return data.getStore(id)
                .map(Result::success)
                .orElse(Result.error("404", "店铺不存在"));
    }

    @GetMapping("/{id}/categories")
    public Result<List<String>> getCategories(@PathVariable Long id) {
        return Result.success(data.getStoreCategories());
    }

    @GetMapping("/{id}/products")
    public Result<List<Product>> getProducts(@PathVariable String id,
                                              @RequestParam(required = false) String categoryId) {
        return Result.success(data.getStoreProducts(id, categoryId));
    }
}
