package com.bcu.wordCountGlobalSort;

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

public class WordCountDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Configuration(), new WordCountDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), WordCountDriver.class.getSimpleName());
        job.setJarByClass(WordCountDriver.class);

//input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://192.168.100.101:9000/wordcount/wordcount.txt"));

        //map
        job.setMapperClass(WordCountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //SHUFLE
        job.setSortComparatorClass(UserSort.class);

        //reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.100.101:9000/wordcount_sort"));
        return job.waitForCompletion(true) ? 0 : -1;
    }
}
