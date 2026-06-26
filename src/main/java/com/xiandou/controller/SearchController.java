package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.model.Store;
import com.xiandou.service.MockDataService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class SearchController {

    private final MockDataService data;

    public SearchController(MockDataService data) { this.data = data; }

    @GetMapping("/search")
    public Result<List<Store>> search(@RequestParam(defaultValue = "") String q) {
        return Result.success(data.searchStores(q));
    }
}
