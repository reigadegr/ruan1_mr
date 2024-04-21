package com.bcu.secondHouse_avg1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 二手房统计Reducer类
 * 输入的数据: key: 地区(Text); value: 迭代器<1+单个房的总价>(SecondHouseBean)
 * 输出的数据: key: 地区(Text); value: 地区二手房总数 + 地区二手房总价(SecondHouseBean)
 */
public class SecondHouseReducer extends Reducer<Text, SecondHouseBean, Text, SecondHouseBean> {

    //准备输出的value
    SecondHouseBean outputValue = new SecondHouseBean();

    @Override
    protected void reduce(Text key, Iterable<SecondHouseBean> values, Reducer<Text, SecondHouseBean, Text, SecondHouseBean>.Context context) throws IOException, InterruptedException {
        //创建变量count,用于计算总数量
        int count = 0;
        //创建变量addressTotalPrice,用于计算总价格
        int addressTotalPrice = 0;
        //遍历迭代器
        for(SecondHouseBean value: values){
            //计算总数量
            count += value.getCount();
            //计算总金额
            addressTotalPrice += value.getTotalPrice();
        }
        //封装输出的value
        outputValue.setAll(count, addressTotalPrice);
        //使用上下文写出key和value
        context.write(key, outputValue);
    }
}
