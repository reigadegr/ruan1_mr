package com.bcu.secondHouse_avg2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器
 */
public class SecondeHousePartition extends Partitioner<Text, SecondHouseBean> {
    @Override
    public int getPartition(Text text, SecondHouseBean secondHouseBean, int i) {
        //获取地区字符串
        String address = text.toString();
        //判断地区是否是浦东
        if ("浦东".equals(address)) {
            //如果是分区0
            return 0;
        }else{
            //如果不是分区1
            return 1;
        }
    }
}
