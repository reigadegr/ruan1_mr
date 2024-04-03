package com.bcu.secondHouseSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text outputKey = new Text();

    LongWritable outputValue = new LongWritable(1);

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] infos = line.split(",");
        String address = infos[3];
        outputKey.set(address);
        context.write(outputKey, outputValue);
    }
}
