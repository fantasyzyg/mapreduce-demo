package com.fantasy.mr.demo5;

import com.fantasy.mr.demo4.ManyToOne;
import com.fantasy.mr.entity.InfoBean;
import com.fantasy.mr.util.MRUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinMR {

    /**
     * 一个 Mapper 应该是针对一个 InputSplit 的
     */
    static class JoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().trim().split("\t");

            String pid;
            if (fileName.startsWith("order")) {
                pid = split[2];
                context.write(new Text(pid), new InfoBean(
                        Integer.parseInt(split[0]), split[1], pid, Integer.parseInt(split[3]), "", 0, 0, "0"
                ));
            } else {
                pid = split[0];
                context.write(new Text(pid), new InfoBean(
                        0, "", pid, 0, split[1], Integer.parseInt(split[2]), Float.parseFloat(split[3]), "1"
                ));
            }
        }
    }

    static class JoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            InfoBean pBean = new InfoBean();
            List<InfoBean> orderBeans = new ArrayList<>();

            try {
                for (InfoBean bean : values) {
                    if (bean.getFlag().equals("1")) {
                        BeanUtils.copyProperties(pBean, bean);
                    } else {
                        InfoBean infoBean = new InfoBean();
                        BeanUtils.copyProperties(infoBean, bean);
                        orderBeans.add(bean);
                    }
                }
            } catch (Exception e) {

            }

            for (InfoBean bean : orderBeans) {
                bean.setpName(pBean.getpName());
                bean.setCategoryId(pBean.getCategoryId());
                bean.setPrice(pBean.getPrice());

                context.write(bean, NullWritable.get());
            }
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

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        MRUtils.deleteHdfsPath(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(
                job.waitForCompletion(true)?0:1
        );
    }
}
