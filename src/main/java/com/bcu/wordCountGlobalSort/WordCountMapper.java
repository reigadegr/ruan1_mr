package com.bcu.wordCountGlobalSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text outputKey = new Text();
    LongWritable outputValue = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String words[] = line.split(",");
        for (String tmp : words) {
            outputKey.set(tmp);
            context.write(outputKey, outputValue);
        }
    }
}
