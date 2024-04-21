package com.bcu.secondHouse_avg3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * 自定义数据类型2
 */
public class SecondHouseBean2 implements Writable {
    private Double totalPriceAvg;//总价平均值
    private Double unitPriceAvg;//单价平均值

    public SecondHouseBean2() {
    }

    public void setAll(double totalPriceAvg, double unitPriceAvg) {
        this.setTotalPriceAvg(totalPriceAvg);
        this.setUnitPriceAvg(unitPriceAvg);
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
        return this.totalPriceAvg + "\t" + this.unitPriceAvg;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(totalPriceAvg);
        dataOutput.writeDouble(unitPriceAvg);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.totalPriceAvg = dataInput.readDouble();
        this.unitPriceAvg = dataInput.readDouble();
    }
}
