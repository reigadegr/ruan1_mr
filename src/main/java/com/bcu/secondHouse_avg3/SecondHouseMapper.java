package com.bcu.secondHouse_avg3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 二手房统计Mapper类
 * 输入的数据: key: 偏移量(LongWritable); value: 一行数据(Text)
 * 输出的数据: key: 地区(Text); value: 1 + 一套房的总价 + 一套房的单价(SecondHouseBean)
 */
public class SecondHouseMapper extends Mapper<LongWritable, Text, Text, SecondHouseBean> {
    //准备输出的key
    Text outputKey = new Text();
    //准备输出的value
    SecondHouseBean outputValue = new SecondHouseBean();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, SecondHouseBean>.Context context) throws IOException, InterruptedException {
        //将一行数据转化成字符串
        String line = value.toString();
        //根据逗号切分字符串
        String[] infos = line.split(",");
        //获取地区信息
        String address = infos[3];
        //获取总价信息并转化成double类型
        Double totalPrice = Double.parseDouble(infos[6]);
        //获取单价信息并转化成double类型
        double unitPrice = Double.parseDouble(infos[7]);
        //封装输出的key
        outputKey.set(address);
        //封装输出的value
        outputValue.setAll(1, totalPrice, unitPrice);
        //使用上下文对象将key和value写出
        context.write(outputKey,outputValue);
    }
}
