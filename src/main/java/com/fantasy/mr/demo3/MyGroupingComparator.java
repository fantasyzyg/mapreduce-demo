package com.fantasy.mr.demo3;

import com.fantasy.mr.entity.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  定义一个分组器,该分组器的作用是在Reduce端将收集好的数据进行排序合并分组
 *
 *      map端会有 sort 排序的比较是根据 key 对象的 compareTo 方法
 *      所以该key class必须是实现了 comparable 接口
 *
 *   并且 reduce 端的 group 分组操作，最后是默认取第一条记录的key作为最后整体的key
 */
public class MyGroupingComparator extends WritableComparator {

    public MyGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean ob1 = (OrderBean)a;
        OrderBean ob2 = (OrderBean)b;
        return ob1.getItemId().compareTo(ob2.getItemId());
    }
}
