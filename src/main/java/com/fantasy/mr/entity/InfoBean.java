package com.fantasy.mr.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable {
    private int orderId;
    private String dateString;
    private String pId;
    private int amount;
    private String pName;
    private int categoryId;
    private float price;

    // flag=0   order表记录
    // flag=1   product表记录
    private String flag;

    public InfoBean() {
    }

    public InfoBean(int orderId, String dateString, String pId,
                    int amount, String pName, int categoryId, float price, String flag) {
        this.orderId = orderId;
        this.dateString = dateString;
        this.pId = pId;
        this.amount = amount;
        this.pName = pName;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeUTF(dateString);
        out.writeUTF(pId);
        out.writeInt(amount);
        out.writeUTF(pName);
        out.writeInt(categoryId);
        out.writeFloat(price);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.dateString = in.readUTF();
        this.pId = in.readUTF();
        this.amount = in.readInt();
        this.pName = in.readUTF();
        this.categoryId = in.readInt();
        this.price = in.readFloat();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return "InfoBean{" +
                "orderId=" + orderId +
                ", dateString='" + dateString + '\'' +
                ", pId='" + pId + '\'' +
                ", amount=" + amount +
                ", pName='" + pName + '\'' +
                ", categoryId=" + categoryId +
                ", price=" + price +
                ", flag='" + flag + '\'' +
                '}';
    }
}
