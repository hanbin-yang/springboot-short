package com.xiandou.model;

public class Product {
    private Long id;
    private String name;
    private String sales;
    private Double price;
    private String image;
    private String categoryId;

    public Product() {}
    public Product(Long id, String name, String sales, Double price, String image, String categoryId) {
        this.id = id; this.name = name; this.sales = sales; this.price = price;
        this.image = image; this.categoryId = categoryId;
    }

    public Long getId() { return id; } public void setId(Long id) { this.id = id; }
    public String getName() { return name; } public void setName(String name) { this.name = name; }
    public String getSales() { return sales; } public void setSales(String sales) { this.sales = sales; }
    public Double getPrice() { return price; } public void setPrice(Double price) { this.price = price; }
    public String getImage() { return image; } public void setImage(String image) { this.image = image; }
    public String getCategoryId() { return categoryId; } public void setCategoryId(String categoryId) { this.categoryId = categoryId; }
}
