package com.bcu.secondHouse_avg3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 二手房统计Reducer类
 * 输入的数据: key: 地区(Text); value: 迭代器<1+单个房的总价+单个房单价总额>(SecondHouseBean)
 * 输出的数据: key: 地区(Text); value: 地区二手房总价平均值 + 单价平均值(SecondHouseBean2)
 */
public class SecondHouseReducer extends Reducer<Text, SecondHouseBean, Text, SecondHouseBean2> {

    //准备输出的value
    SecondHouseBean2 outputValue = new SecondHouseBean2();

    @Override
    protected void reduce(Text key, Iterable<SecondHouseBean> values, Reducer<Text, SecondHouseBean, Text, SecondHouseBean2>.Context context) throws IOException, InterruptedException {
        //创建变量count,用于计算总数量
        int count = 0;
        //创建变量addressTotalPrice,用于计算总价格
        int addressTotalPrice = 0;
        //创建变量addressUnitPrice,用于计算单价总金额
        int addressUnitPrice = 0;
        //遍历迭代器
        for (SecondHouseBean value : values) {
            //计算总数量
            count += value.getCount();
            //计算总房价总金额
            addressTotalPrice += value.getTotalPrice();
            //计算单价总金额
            addressUnitPrice += value.getUnitPrice();
        }
        //计算总房价平均值
        double totalPriceAvg = addressTotalPrice / count;
        //计算单价平均值
        double unitPriceAvg = addressUnitPrice / count;
        //封装输出的value
        outputValue.setAll(totalPriceAvg, unitPriceAvg);
        //使用上下文写出key和value
        context.write(key, outputValue);
    }
}
