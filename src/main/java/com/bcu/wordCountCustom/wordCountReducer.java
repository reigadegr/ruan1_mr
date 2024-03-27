package com.bcu.wordCountCustom;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class wordCountReducer extends Reducer<Text, LongWritable, WordCountBean, LongWritable> {
    WordCountBean outputKey=new WordCountBean();
    LongWritable outputVaue=new LongWritable();

    protected void reduce(WordCountBean key, Iterable<LongWritable> values, Reducer<Text, LongWritable, WordCountBean, LongWritable>.Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        String word=key.toString();
        outputKey.setAll(word,word.length());
        int count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        outputVaue.set(count);
        context.write(outputKey,outputVaue);
    }
}
