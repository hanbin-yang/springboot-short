package com.xiandou.service;

import com.xiandou.model.*;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class MockDataService {

    private final List<Category> categories = new ArrayList<>();
    private final List<Store> stores = new CopyOnWriteArrayList<>();
    private final Map<String, List<Product>> storeProducts = new LinkedHashMap<>();
    private final List<String> storeCatList = Arrays.asList("all", "seckill", "snacks", "meat", "fruit", "veggie");
    private final List<CartItem> cart = new CopyOnWriteArrayList<>();
    private final AtomicLong cartIdSeq = new AtomicLong(100);
    private final List<Order> orders = new CopyOnWriteArrayList<>();
    private final AtomicLong orderIdSeq = new AtomicLong(100);
    private final List<Address> addresses = new CopyOnWriteArrayList<>();
    private final AtomicLong addrIdSeq = new AtomicLong(10);

    @PostConstruct
    void init() {
        // Categories
        categories.add(new Category("market", "market", "\u8d85\u5e02\u4fbf\u5229"));
        categories.add(new Category("veg", "veg", "\u83dc\u5e02\u573a"));
        categories.add(new Category("fruit", "fruit", "\u6c34\u679c\u5e97"));
        categories.add(new Category("flower", "flower", "\u9c9c\u82b1\u7eff\u690d"));
        categories.add(new Category("health", "health", "\u533b\u836f\u5065\u5eb7"));
        categories.add(new Category("home", "home", "\u5bb6\u5c45\u65f6\u5c1a"));
        categories.add(new Category("cake", "cake", "\u70d8\u7119\u86cb\u7cd5"));
        categories.add(new Category("checkin", "checkin", "\u7b7e\u5230"));
        categories.add(new Category("delivery", "delivery", "\u5927\u724c\u514d\u8fd0"));
        categories.add(new Category("coupon", "coupon", "\u7ea2\u5305\u5957\u9910"));

        // Stores
        Store walmart = new Store(1L, "\u6c83\u5c14\u739b", "W", "blue-bg",
            "\u6708\u552e1\u4e07+", "\u8d77\u9001\uffe70", "\u57fa\u7840\u8fd0\u8d39\uffe55", "VIP\u5c0a\u4eab\u6ee1\u6ee19\u5143\u51cf4\u5143\u8fd0\u8d39\u5238\uff08\u6bcf\u67081\u5f20\uff09");
        walmart.setVipTag("VIP\u5c0a\u4eab\u6ee1\u6ee19\u5143\u51cf4\u5143\u8fd0\u8d39\u5238\uff08\u6bcf\u67081\u5f20\uff09");
        stores.add(walmart);

        Store sams = new Store(2L, "\u5c71\u59c6\u4f1a\u5458\u5546\u5e97", "S", "green-bg",
            "\u6708\u552e2\u4e07+", "\u8d77\u9001\uffe70", "\u57fa\u7840\u8fd0\u8d39\uffe55", "\u8054\u5408\u5229\u534e\u6d17\u62a4\u6ee1\u6ee10\u51cf");
        sams.setVipTag("\u8054\u5408\u5229\u534e\u6d17\u62a4\u6ee1\u6ee10\u51cf");
        stores.add(sams);

        // Products for store 1
        List<Product> wmProducts = new ArrayList<>();
        wmProducts.add(new Product(1L, "\u756a\u8304\u2502250g/\u4efd", "\u6708\u552e10\u4efd+", 33.6, "https://picsum.photos/seed/tomato/68/68", "all"));
        wmProducts.add(new Product(2L, "\u63d0\u5b50\u2502250g/\u4efd", "\u6708\u552e10\u4efd+", 33.6, "https://picsum.photos/seed/grape/68/68", "all"));
        wmProducts.add(new Product(3L, "\u63d0\u5b50\u2502250g/\u4efd", "\u6708\u552e10\u4efd+", 33.6, "https://picsum.photos/seed/grape2/68/68", "fruit"));
        wmProducts.add(new Product(4L, "\u756a\u8304\u2502250g/\u4efd", "\u6708\u552e10\u4efd+", 33.6, "https://picsum.photos/seed/tomato2/68/68", "fruit"));
        wmProducts.add(new Product(5L, "\u725b\u8089200g", "\u6708\u552e20\u4efd+", 58.0, "https://picsum.photos/seed/beef/68/68", "meat"));
        wmProducts.add(new Product(6L, "\u9e21\u80f8\u8089500g", "\u6708\u552e15\u4efd+", 28.0, "https://picsum.photos/seed/chicken/68/68", "meat"));
        wmProducts.add(new Product(7L, "\u6df7\u5408\u575a\u679c200g", "\u6708\u552e50\u4efd+", 45.0, "https://picsum.photos/seed/nuts/68/68", "snacks"));
        wmProducts.add(new Product(8L, "\u6709\u673a\u83e0\u83dc250g", "\u6708\u552e30\u4efd+", 12.8, "https://picsum.photos/seed/spinach/68/68", "veggie"));
        storeProducts.put("1", wmProducts);

        // Products for store 2
        List<Product> smProducts = new ArrayList<>();
        smProducts.add(new Product(9L, "\u6fb3\u6d32\u725b\u6392200g", "\u6708\u552e20\u4efd+", 88.0, "https://picsum.photos/seed/steak/68/68", "all"));
        smProducts.add(new Product(10L, "\u4e09\u6587\u9c7c\u5207\u7247200g", "\u6708\u552e12\u4efd+", 68.0, "https://picsum.photos/seed/salmon/68/68", "all"));
        smProducts.add(new Product(11L, "\u84dd\u8393125g", "\u6708\u552e40\u4efd+", 25.0, "https://picsum.photos/seed/blueberry/68/68", "fruit"));
        storeProducts.put("2", smProducts);

        // Cart
        cart.add(new CartItem(cartIdSeq.incrementAndGet(), 1L, "\u756a\u8304\u2502250g/\u4efd", 33.6, 1, "https://picsum.photos/seed/tomato/68/68"));
        cart.add(new CartItem(cartIdSeq.incrementAndGet(), 2L, "\u63d0\u5b50\u2502250g/\u4efd", 33.6, 2, "https://picsum.photos/seed/grape/68/68"));

        // Addresses
        addresses.add(new Address(addrIdSeq.incrementAndGet(), "\u7476\u59ae\uff08\u5148\u751f\uff09", "18911024266", "\u5317\u4eac\u7406\u5de5\u5927\u5b66\u56fd\u9632\u79d1\u6280\u56ed\u2465\u53f7\u697c10\u5c42"));
        addresses.add(new Address(addrIdSeq.incrementAndGet(), "\u5f20\u4e09\uff08\u5148\u751f\uff09", "13800138000", "\u5317\u4eac\u5e02\u6d77\u6dc0\u533a\u4e2d\u5173\u6751\u5927\u88571\u53f7"));

        // Orders
        Order o1 = new Order();
        o1.setId(orderIdSeq.incrementAndGet());
        o1.setStore("\u6c83\u5c14\u739b");
        o1.setStatus("\u5f85\u652f\u4ed8");
        o1.setStatusColor("#E93B3B");
        o1.setItemCount(4);
        o1.setTotal(62.0);
        o1.setShowCancel(true);
        o1.setShowPay(true);
        Order.OrderItem oi1 = new Order.OrderItem();
        oi1.setName("\u756a\u8304\u2502250g/\u4efd"); oi1.setSpec("1\u4efd"); oi1.setPrice(33.6); oi1.setCount(3);
        oi1.setImage("https://picsum.photos/seed/tomato/46/46");
        Order.OrderItem oi2 = new Order.OrderItem();
        oi2.setName("\u63d0\u5b50\u2502250g/\u4efd"); oi2.setSpec("1\u4efd"); oi2.setPrice(33.6); oi2.setCount(1);
        oi2.setImage("https://picsum.photos/seed/grape/46/46");
        o1.setItems(Arrays.asList(oi1, oi2));
        orders.add(o1);

        Order o2 = new Order();
        o2.setId(orderIdSeq.incrementAndGet());
        o2.setStore("\u5c71\u59c6\u4f1a\u5458\u5546\u5e97");
        o2.setStatus("\u914d\u9001\u4e2d");
        o2.setStatusColor("#1FA4FC");
        o2.setItemCount(2);
        o2.setTotal(176.0);
        o2.setShowConfirm(true);
        Order.OrderItem oi3 = new Order.OrderItem();
        oi3.setName("\u6fb3\u6d32\u725b\u6392200g"); oi3.setSpec("1\u4efd"); oi3.setPrice(88.0); oi3.setCount(2);
        oi3.setImage("https://picsum.photos/seed/steak/46/46");
        o2.setItems(Collections.singletonList(oi3));
        orders.add(o2);
    }

    public List<Category> getCategories() { return categories; }
    public List<Store> getStores() { return stores; }
    public List<String> getStoreCategories() { return storeCatList; }
    public List<Product> getProducts(String storeId) { return storeProducts.getOrDefault(storeId, List.of()); }

    public List<Product> getStoreProducts(String storeId, String categoryId) {
        List<Product> all = storeProducts.getOrDefault(storeId, new ArrayList<>());
        if (categoryId == null || "all".equals(categoryId)) return all;
        return all.stream().filter(p -> categoryId.equals(p.getCategoryId())).collect(Collectors.toList());
    }

    public Optional<Store> getStore(Long id) {
        return stores.stream().filter(s -> s.getId().equals(id)).findFirst();
    }

    public List<CartItem> getCart() { return cart; }

    public CartItem addToCart(CartItem item) {
        item.setId(cartIdSeq.incrementAndGet());
        cart.add(item);
        return item;
    }

    public void updateCartItem(Long productId, int count) {
        for (CartItem i : cart) {
            if (i.getProductId().equals(productId)) {
                i.setCount(count);
                return;
            }
        }
    }

    public void clearCart() { cart.clear(); }
    public List<Order> getOrders() { return orders; }

    public Order createOrder(Order o) {
        o.setId(orderIdSeq.incrementAndGet());
        orders.add(o);
        return o;
    }

    public List<Address> getAddresses() { return addresses; }

    public Address addAddress(Address a) {
        a.setId(addrIdSeq.incrementAndGet());
        addresses.add(a);
        return a;
    }

    public Optional<Address> updateAddress(Long id, Address a) {
        for (Address ad : addresses) {
            if (ad.getId().equals(id)) {
                ad.setName(a.getName());
                ad.setPhone(a.getPhone());
                ad.setDetail(a.getDetail());
                return Optional.of(ad);
            }
        }
        return Optional.empty();
    }

    public void deleteAddress(Long id) { addresses.removeIf(a -> a.getId().equals(id)); }

    public List<Store> searchStores(String q) {
        if (q == null || q.trim().isEmpty()) return stores;
        return stores.stream().filter(s -> s.getName().contains(q)).collect(Collectors.toList());
    }

    public Map<String, Object> getSiteInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("address", "\u5317\u4eac\u7406\u5de5\u5927\u5b66\u56fd\u9632\u79d1\u6280\u56ed\u2465\u53f7\u697c10\u5c42");
        info.put("carrier", "\u4e2d\u56fd\u79fb\u52a8");
        info.put("battery", "58%");
        info.put("searchPlaceholder", "\u5c71\u59c6\u4f1a\u5458\u5546\u5e97\u4f18\u60e0\u5546\u54c1");
        info.put("bannerText", "\u9650\u65f6\u7279\u60e0 \u00b7 \u7cbe\u9009\u597d\u7269");
        return info;
    }

    public Map<String, Object> getProfile() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("id", 1);
        p.put("name", "\u70ed\u5fc3\u5e02\u6c11\u674e\u5148\u751f");
        p.put("idNumber", "ID: 1069643013");
        p.put("level", 16);

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("\u7ea2\u5305", 218);
        stats.put("\u4f18\u60e0\u5238", "12\u5f20");
        stats.put("\u9c9c\u8c46", 88);
        stats.put("\u767d\u6761", 1000);
        p.put("stats", stats);
        return p;
    }
}