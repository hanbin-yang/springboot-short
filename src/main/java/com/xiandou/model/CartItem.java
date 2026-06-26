package com.xiandou.model;

public class CartItem {
    private Long id;
    private Long productId;
    private String name;
    private Double price;
    private Integer count;
    private String image;

    public CartItem() {}
    public CartItem(Long id, Long productId, String name, Double price, Integer count, String image) {
        this.id = id; this.productId = productId; this.name = name;
        this.price = price; this.count = count; this.image = image;
    }

    public Long getId() { return id; } public void setId(Long id) { this.id = id; }
    public Long getProductId() { return productId; } public void setProductId(Long productId) { this.productId = productId; }
    public String getName() { return name; } public void setName(String name) { this.name = name; }
    public Double getPrice() { return price; } public void setPrice(Double price) { this.price = price; }
    public Integer getCount() { return count; } public void setCount(Integer count) { this.count = count; }
    public String getImage() { return image; } public void setImage(String image) { this.image = image; }
}
