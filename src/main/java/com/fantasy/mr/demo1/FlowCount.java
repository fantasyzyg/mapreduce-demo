package com.fantasy.mr.demo1;

import com.fantasy.mr.entity.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.fantasy.mr.util.MRUtils;

import java.io.IOException;

/**
 *      demo1: 定义序列化对象
 */
public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().trim().split("\\s+");
            context.write(
                    new Text(split[0]),
                    new FlowBean(Integer.parseInt(split[1]),
                            Integer.parseInt(split[2])));
        }
    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sumUpFlow = 0;
            long sumDownFlow = 0;

            for (FlowBean bean : values) {
                sumDownFlow += bean.getDownFlow();
                sumUpFlow += bean.getUpFlow();
            }

            context.write(key, new FlowBean(sumUpFlow, sumDownFlow));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        if (args.length != 2) {
            System.out.println("Usage:<in><out>");
            System.exit(2);
        }

        job.setJarByClass(FlowCount.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        MRUtils.deleteHdfsPath(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(
                job.waitForCompletion(true)?0:1
        );
    }
}
