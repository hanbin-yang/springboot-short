package com.xiandou.controller;

import com.xiandou.common.Result;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    @PostMapping("/login")
    public Result<Map<String, Object>> login(@RequestBody Map<String, String> body) {
        String phone = body.get("phone");
        Map<String, Object> result = new HashMap<>();
        result.put("token", "mock-token-" + phone);
        result.put("userId", 1);
        result.put("name", "热心市民李先生");
        return Result.success(result);
    }

    @PostMapping("/register")
    public Result<Map<String, Object>> register(@RequestBody Map<String, String> body) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("message", "注册成功");
        return Result.success(result);
    }
}
