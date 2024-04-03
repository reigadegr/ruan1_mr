package com.bcu.wordCountGlobalSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class UserSort extends WritableComparator {
    public UserSort() {
        super(Text.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        Text o1 = (Text) a;
        Text o2 = (Text) b;
        //降序排列
        return -o1.compareTo(o2);
    }
}
