package com.bcu.secondHouse_avg2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据类型
 */
public class SecondHouseBean implements Writable {
    private Integer count;//总数
    private Double totalPriceAvg;//总价平均值
    private Double unitPriceAvg;//单价平均值

    public SecondHouseBean(){}

    public void setAll(int count, double totalPriceAvg, double unitPriceAvg){
        this.setCount(count);
        this.setTotalPriceAvg(totalPriceAvg);
        this.setUnitPriceAvg(unitPriceAvg);
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getTotalPriceAvg() {
        return totalPriceAvg;
    }

    public void setTotalPriceAvg(Double totalPriceAvg) {
        this.totalPriceAvg = totalPriceAvg;
    }

    public Double getUnitPriceAvg() {
        return unitPriceAvg;
    }

    public void setUnitPriceAvg(Double unitPriceAvg) {
        this.unitPriceAvg = unitPriceAvg;
    }

    @Override
    public String toString() {
        return this.count + "\t" + this.totalPriceAvg + "\t" + this.unitPriceAvg;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeDouble(totalPriceAvg);
        dataOutput.writeDouble(unitPriceAvg);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count = dataInput.readInt();
        this.totalPriceAvg = dataInput.readDouble();
        this.unitPriceAvg = dataInput.readDouble();
    }
}
