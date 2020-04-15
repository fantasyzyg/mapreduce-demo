package com.fantasy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;

public class HDFSTest {

    @Test
    public void testHdfsConnection() throws IOException {

        // 从core-site.xml读取配置

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(fs);
    }
}
