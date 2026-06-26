package com.xiandou.model;

import java.util.List;

public class Order {
    private Long id;
    private String store;
    private String status;
    private String statusColor;
    private List<OrderItem> items;
    private Integer itemCount;
    private Double total;
    private boolean showCancel;
    private boolean showPay;
    private boolean showConfirm;

    public static class OrderItem {
        private String name;
        private String spec;
        private Double price;
        private Integer count;
        private String image;

        public String getName() { return name; } public void setName(String name) { this.name = name; }
        public String getSpec() { return spec; } public void setSpec(String spec) { this.spec = spec; }
        public Double getPrice() { return price; } public void setPrice(Double price) { this.price = price; }
        public Integer getCount() { return count; } public void setCount(Integer count) { this.count = count; }
        public String getImage() { return image; } public void setImage(String image) { this.image = image; }
    }

    public Long getId() { return id; } public void setId(Long id) { this.id = id; }
    public String getStore() { return store; } public void setStore(String store) { this.store = store; }
    public String getStatus() { return status; } public void setStatus(String status) { this.status = status; }
    public String getStatusColor() { return statusColor; } public void setStatusColor(String statusColor) { this.statusColor = statusColor; }
    public List<OrderItem> getItems() { return items; } public void setItems(List<OrderItem> items) { this.items = items; }
    public Integer getItemCount() { return itemCount; } public void setItemCount(Integer itemCount) { this.itemCount = itemCount; }
    public Double getTotal() { return total; } public void setTotal(Double total) { this.total = total; }
    public boolean isShowCancel() { return showCancel; } public void setShowCancel(boolean showCancel) { this.showCancel = showCancel; }
    public boolean isShowPay() { return showPay; } public void setShowPay(boolean showPay) { this.showPay = showPay; }
    public boolean isShowConfirm() { return showConfirm; } public void setShowConfirm(boolean showConfirm) { this.showConfirm = showConfirm; }
}
