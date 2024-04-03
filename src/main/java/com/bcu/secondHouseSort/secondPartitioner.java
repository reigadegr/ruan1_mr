package com.bcu.secondHouseSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class secondPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        //key Text->String
        String address = text.toString();
        if ("浦东".equals(address)) {
            return 0;
        } else {
            return 1;
        }
    }
}
