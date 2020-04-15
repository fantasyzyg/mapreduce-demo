package com.fantasy.mr.demo0;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *     按照country name来进行文件输出，挺有意思的，我想动态分区也是利用这个来实现的吧
 */
public class TestMultipleOutput2 {

    public static class IPCountryMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static int country_pos = 1;
        private final static Pattern pattern = Pattern.compile("\\s+");

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String country = pattern.split(value.toString().trim())[country_pos];
            context.write(new Text(country), new IntWritable(1));
        }
    }

    public static class IPCountryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> output;

        protected void setup(Context context) {
            output = new MultipleOutputs<>(context);
        }

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                // key, value, baseOutputPath
                output.write(key, new IntWritable(1), key.toString());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            output.close();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.out.println("Usage:<in><out>");
            System.exit(2);
        }

        // 判断文件是否存在，存在则删除
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if (hdfs.exists(path))
            hdfs.delete(path, true);

        Job job = Job.getInstance(conf, "IP count by country to named files");
        job.setJarByClass(TestMultipleOutput.class);
        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);


        // 设置reducer个数
        // job.setNumReduceTasks(2);
        /*
         *  1. 默认reducer个数为1，输出会有一个文件    part-r-00000
         *  2. 若设置reducer个数为2，输出会有两个文件    part-r-00000 and part-r-00001
         *  3. 但是上述文件都为空
         */

        // map端输入输出格式
        job.setMapperClass(IPCountryMapper.class);
        job.setMapOutputKeyClass(Text.class);//(1)
        job.setMapOutputValueClass(IntWritable.class);//(2)

        // reduce端输入输出格式
        job.setReducerClass(IPCountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入、输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


