package com.bcu.wordCountCustom;

import com.bcu.JobMain;
import com.bcu.mapreduce.WordCountMapper;
import com.bcu.mapreduce.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordCountDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Configuration(), new wordCountDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
        job.setJarByClass(JobMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.100.101:9000/wordcount"));
        //第二步 设置我们的mapper类
        job.setMapperClass(WordCountMapper.class);
        //然后输出key,和mapper数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //第七部：设置reducer类
        job.setReducerClass(WordCountReducer.class);
        //然后输出key,和mapper数据类型
        job.setOutputKeyClass(WordCountBean.class);
        job.setOutputValueClass(LongWritable.class);

        //第八步：设置输出数据所用类
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.100.101:9000/wordcount_out"));
        boolean b = job.waitForCompletion(true);
        return b ? 0 : -1;
    }
}
