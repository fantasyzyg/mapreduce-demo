package com.fantasy.mr.demo3;

import com.fantasy.mr.entity.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *      为什么Partitioner是两个泛型参数呢?
 *     我想或许有些需求是需要 KEY/VALUE 同时参与到分区函数中
 */
public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getItemId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
