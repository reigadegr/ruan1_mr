package com.bcu.wordCountGlobalSort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountDriver extends Configured implements Tool {


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

        return 0;
    }
}
