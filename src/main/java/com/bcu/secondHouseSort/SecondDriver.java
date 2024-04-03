package com.bcu.secondHouseSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SecondDriver extends Configured implements Tool {
    public FileSystem getHdfs() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://node-1:9000");
        FileSystem hdfs = FileSystem.get(conf);
        System.out.println(hdfs);
        return hdfs;
    }

    @Override
    public int run(String[] strings) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(super.getConf(), SecondDriver.class.getSimpleName());
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://192.168.100.101:9000/datas/xxx.csv"));
        //mapper
        job.setMapperClass(SecondMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //shuffle
        job.setPartitionerClass(secondPartitioner.class);
        //reduce
        job.setReducerClass(SecondReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //output
        //优先删除要创建的目录，防止因为存在导致异常
        FileSystem hdfs = getHdfs();
        Path shP = new Path("hdfs://192.168.100.101:9000/datas/shP");
        hdfs.delete(shP, true);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, shP);
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Configuration(), new SecondDriver(), args);
        System.exit(status);
    }
}
