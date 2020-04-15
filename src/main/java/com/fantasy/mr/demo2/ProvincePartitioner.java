package com.fantasy.mr.demo2;

import com.fantasy.mr.entity.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 *    Map端的分区器
 *      1. context.write(Text, FlowBean)   -- > ProvincePartitioner(Text, FlowBean, 5)
 *      2. 输出该记录到特定的文件中
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    private static Map<String, Integer> provinceDict = new HashMap<>();

    static {
        provinceDict.put("137", 0);
        provinceDict.put("133", 1);
        provinceDict.put("138", 2);
        provinceDict.put("135", 3);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        return provinceDict.getOrDefault(text.toString().substring(0, 3), 4);
    }
}
