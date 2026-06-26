package com.xiandou.model;

public class Address {
    private Long id;
    private String name;
    private String phone;
    private String detail;

    public Address() {}
    public Address(Long id, String name, String phone, String detail) {
        this.id = id; this.name = name; this.phone = phone; this.detail = detail;
    }

    public Long getId() { return id; } public void setId(Long id) { this.id = id; }
    public String getName() { return name; } public void setName(String name) { this.name = name; }
    public String getPhone() { return phone; } public void setPhone(String phone) { this.phone = phone; }
    public String getDetail() { return detail; } public void setDetail(String detail) { this.detail = detail; }
}
