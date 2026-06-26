package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.model.Category;
import com.xiandou.model.Store;
import com.xiandou.service.MockDataService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class HomeController {

    private final MockDataService data;

    public HomeController(MockDataService data) { this.data = data; }

    @GetMapping("/categories")
    public Result<List<Category>> getCategories() {
        return Result.success(data.getCategories());
    }

    @GetMapping("/stores")
    public Result<List<Store>> getStores() {
        return Result.success(data.getStores());
    }

    @GetMapping("/site-info")
    public Result<Map<String, Object>> getSiteInfo() {
        return Result.success(data.getSiteInfo());
    }
}
