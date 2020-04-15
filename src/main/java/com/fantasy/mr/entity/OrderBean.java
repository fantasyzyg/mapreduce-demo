package com.fantasy.mr.entity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 *    一个实现了序列化并且可以比较的对象
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private Text itemId;
    private DoubleWritable amount;

    public OrderBean() {
    }

    public OrderBean(Text itemId, DoubleWritable amount) {
        this.itemId = itemId;
        this.amount = amount;
    }

    public Text getItemId() {
        return itemId;
    }

    public void setItemId(Text itemId) {
        this.itemId = itemId;
    }

    public DoubleWritable getAmount() {
        return amount;
    }

    public void setAmount(DoubleWritable amount) {
        this.amount = amount;
    }

    @Override
    public int compareTo(OrderBean o) {

        return itemId.compareTo(o.getItemId()) == 0 ? -amount.compareTo(o.getAmount()) : itemId.compareTo(o.getItemId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(itemId.toString());
        out.writeDouble(amount.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        itemId = new Text(in.readUTF());
        amount = new DoubleWritable(in.readDouble());
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "itemId=" + itemId +
                ", amount=" + amount +
                '}';
    }
}
