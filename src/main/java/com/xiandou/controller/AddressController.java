package com.xiandou.controller;

import com.xiandou.common.Result;
import com.xiandou.model.Address;
import com.xiandou.service.MockDataService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/addresses")
public class AddressController {

    private final MockDataService data;

    public AddressController(MockDataService data) { this.data = data; }

    @GetMapping
    public Result<List<Address>> getAddresses() {
        return Result.success(data.getAddresses());
    }

    @PostMapping
    public Result<Address> createAddress(@RequestBody Address a) {
        return Result.success(data.addAddress(a));
    }

    @PutMapping("/{id}")
    public Result<Address> updateAddress(@PathVariable Long id, @RequestBody Address a) {
        return data.updateAddress(id, a)
                .map(Result::success)
                .orElse(Result.error("404", "地址不存在"));
    }

    @DeleteMapping("/{id}")
    public Result<Void> deleteAddress(@PathVariable Long id) {
        data.deleteAddress(id);
        return Result.success();
    }
}
