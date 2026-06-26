package com.xiandou.model;

import java.util.List;

public class Store {
    private Long id;
    private String name;
    private String iconLetter;
    private String bgClass;
    private String sales;
    private String minDelivery;
    private String shipping;
    private String tag;
    private String vipTag;
    private List<String> topImages;
    private List<String> storeCategories;
    private List<Product> products;

    public Store() {}
    public Store(Long id, String name, String iconLetter, String bgClass, String sales,
                 String minDelivery, String shipping, String tag) {
        this.id = id; this.name = name; this.iconLetter = iconLetter; this.bgClass = bgClass;
        this.sales = sales; this.minDelivery = minDelivery; this.shipping = shipping; this.tag = tag;
    }

    public Long getId() { return id; } public void setId(Long id) { this.id = id; }
    public String getName() { return name; } public void setName(String name) { this.name = name; }
    public String getIconLetter() { return iconLetter; } public void setIconLetter(String iconLetter) { this.iconLetter = iconLetter; }
    public String getBgClass() { return bgClass; } public void setBgClass(String bgClass) { this.bgClass = bgClass; }
    public String getSales() { return sales; } public void setSales(String sales) { this.sales = sales; }
    public String getMinDelivery() { return minDelivery; } public void setMinDelivery(String minDelivery) { this.minDelivery = minDelivery; }
    public String getShipping() { return shipping; } public void setShipping(String shipping) { this.shipping = shipping; }
    public String getTag() { return tag; } public void setTag(String tag) { this.tag = tag; }
    public String getVipTag() { return vipTag; } public void setVipTag(String vipTag) { this.vipTag = vipTag; }
    public List<String> getTopImages() { return topImages; } public void setTopImages(List<String> topImages) { this.topImages = topImages; }
    public List<String> getStoreCategories() { return storeCategories; } public void setStoreCategories(List<String> storeCategories) { this.storeCategories = storeCategories; }
    public List<Product> getProducts() { return products; } public void setProducts(List<Product> products) { this.products = products; }
}
