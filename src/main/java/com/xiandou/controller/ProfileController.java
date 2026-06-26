package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.service.MockDataService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class ProfileController {

    private final MockDataService data;

    public ProfileController(MockDataService data) { this.data = data; }

    @GetMapping("/profile")
    public Result<Map<String, Object>> getProfile() {
        return Result.success(data.getProfile());
    }
}
