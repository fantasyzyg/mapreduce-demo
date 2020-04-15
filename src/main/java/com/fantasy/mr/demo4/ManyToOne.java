package com.fantasy.mr.demo4;

import com.fantasy.mr.util.MRUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


/**
 *   合并多个小文件为一个大文件
 *      1. 自定义InputFormat
 *      2. 自定义RecordReader   -> 一次性读完一个文件
 *      3. 只有一个Reducer
 *      4. 顺利完成小文件合并    -> job outputFormat需要指定
 */
public class ManyToOne {

    /**
     *  Mapper 底层依赖于  RecordReader 的实现
     */
    static class FileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text fileNameKey;

        // 对于这个context，我的理解是应该是map task的context
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.fileNameKey = new Text(((FileSplit)context.getInputSplit()).getPath().toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(fileNameKey, value);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        if (args.length != 2) {
            System.out.println("Usage:<in><out>");
            System.exit(2);
        }

        job.setJarByClass(ManyToOne.class);

        // InputFormatClass and OutputFormatClass
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        job.setMapperClass(FileMapper.class);
        // Map输出和Reduce输出的 kv class 一样
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);


        //  MapReduce 文件输入输出路径设置 
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        MRUtils.deleteHdfsPath(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(
                job.waitForCompletion(true)?0:1
        );
    }
}
