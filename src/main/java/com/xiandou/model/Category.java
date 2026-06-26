package com.xiandou.model;

public class Category {
    private String id;
    private String name;
    private String label;

    public Category() {}
    public Category(String id, String name, String label) {
        this.id = id; this.name = name; this.label = label;
    }

    public String getId() { return id; } public void setId(String id) { this.id = id; }
    public String getName() { return name; } public void setName(String name) { this.name = name; }
    public String getLabel() { return label; } public void setLabel(String label) { this.label = label; }
}
