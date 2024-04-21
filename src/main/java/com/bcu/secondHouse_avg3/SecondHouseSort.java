package com.bcu.secondHouse_avg3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义的排序比较器
 * 将key按照字典顺序倒序排列
 */
public class SecondHouseSort extends WritableComparator {

    //注册： 实现将Text类型进行构建实力，进行比较
    public SecondHouseSort(){
        super(Text.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        Text o1 = (Text) a;
        Text o2 = (Text) b;
        return o1.compareTo(o2);
    }
}
