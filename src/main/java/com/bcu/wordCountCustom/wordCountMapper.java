package com.bcu.wordCountCustom;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text outputKey = new Text();
    LongWritable outputValue = new LongWritable(1);


    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);


        String line = value.toString();
        String[] words = line.split(",");
        for (String word : words) {
            outputKey.set(word);

            context.write(outputKey, outputValue);
        }
    }
}
