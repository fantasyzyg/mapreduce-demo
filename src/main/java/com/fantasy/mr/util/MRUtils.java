package com.fantasy.mr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MRUtils {

    public static FileSystem fs;
    static {
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteHdfsPath(String path) throws IOException {
        if (fs.exists(new Path(path))) {
            fs.delete(new Path(path), true);
        }
    }
}
