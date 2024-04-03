package com.bcu.secondHouse_avg1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * 二手房统计驱动类
 */
public class SecondHouseDirver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Configuration(), new SecondHouseDirver(), args);
        System.exit(status);
    }

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

    @Override
    public int run(String[] strings) throws Exception {
        //构建Job对象
        Job job = Job.getInstance(super.getConf(), SecondHouseDirver.class.getSimpleName());
        job.setJarByClass(SecondHouseDirver.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://192.168.100.101:9000/secondhouse"));

        //map
        job.setMapperClass(SecondHouseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SecondHouseBean.class);

        //shuffle
        //设置分区器
        job.setPartitionerClass(SecondeHousePartition.class);
        //设置全局排序器
        job.setSortComparatorClass(SecondHouseSort.class);

        //reduce
        job.setReducerClass(SecondHouseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SecondHouseBean.class);
        //设置Reducer个数
        job.setNumReduceTasks(2);

        //output
        FileSystem hdfs = getHdfs();
        hdfs.delete(new Path("hdfs://192.168.100.101:9000/output/secondhouse_avg"));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.100.101:9000/output/secondhouse_avg"));

        //submit
        return job.waitForCompletion(true) ? 0 : -1;
    }
}
