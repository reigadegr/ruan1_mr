package com.bcu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URISyntaxException;

public class HdfsApi {
    public FileSystem getHdfs() throws IOException, URISyntaxException {
        /*
          构建一个当前程序的配置对象，用于管理所有的配置：*-default。xml
          这个对象会加载所有的*-default。xml中的默认配置
          然后加载所有的*-size。xml中的自定义的配置，用这些自定义的配置替换默认配置
          如何让当前地址知道HDFS的地址？也就是fs.defaultFS
          方法1：创建一个resource资源目录，将core-site.xml复制粘贴到resources中
         */
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://node-1:9000");

        FileSystem hdfs = FileSystem.get(conf);

        System.out.println(hdfs);

        return hdfs;

    }
}
