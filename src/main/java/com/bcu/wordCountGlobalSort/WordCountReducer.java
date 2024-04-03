package com.bcu.wordCountGlobalSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable outputValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        outputValue.set(count);
        context.write(key, outputValue);
    }
}
