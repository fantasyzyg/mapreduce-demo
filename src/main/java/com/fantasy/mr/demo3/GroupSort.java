package com.fantasy.mr.demo3;

import com.fantasy.mr.entity.OrderBean;
import com.fantasy.mr.util.MRUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GroupSort {

    static class SortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");

            OrderBean orderBean = new OrderBean(new Text(split[0]),
                    new DoubleWritable(Double.parseDouble(split[2])));
            context.write(orderBean, NullWritable.get());
        }
    }

    static class SortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        if (args.length != 2) {
            System.out.println("Usage:<in><out>");
            System.exit(2);
        }

        job.setJarByClass(GroupSort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);


        // 设置 partitioner and grouping
        job.setPartitionerClass(ItemIdPartitioner.class);
        job.setNumReduceTasks(2);
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        /**
         *   对于 MapOutputKeyClass ,如果没有设置的话，则会选择和 OutputKeyClass 一样的 class
         *   对于 apOutputValueClass 也是同样如此
         */

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        MRUtils.deleteHdfsPath(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(
                job.waitForCompletion(true)?0:1
        );
    }
}
