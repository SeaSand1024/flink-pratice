package com.imooc.flink.domain;

public class ItemInfo {
    public int itemId;
    public String orderId;  // 作为join的条件
    public long time;
    public String sku;
    public double amount;
    public double money;

    @Override
    public String toString() {
        return "ItemInfo{" +
                "itemId=" + itemId +
                ", orderId='" + orderId + '\'' +
                ", time=" + time +
                ", sku='" + sku + '\'' +
                ", amount=" + amount +
                ", money=" + money +
                '}';
    }
}
