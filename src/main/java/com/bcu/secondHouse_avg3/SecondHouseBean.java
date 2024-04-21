package com.bcu.secondHouse_avg3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据类型1
 */
public class SecondHouseBean implements Writable {
    private Integer count;//总数
    private Double totalPrice;//总价
    private Double unitPrice;//单价

    public SecondHouseBean(){}

    public void setAll(int count, double totalPrice, double unitPrice){
        this.setCount(count);
        this.setTotalPrice(totalPrice);
        this.setUnitPrice(unitPrice);
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(Double totalPriceAvg) {
        this.totalPrice = totalPriceAvg;
    }

    public Double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(Double unitPriceAvg) {
        this.unitPrice = unitPriceAvg;
    }

    @Override
    public String toString() {
        return this.count + "\t" + this.totalPrice + "\t" + this.unitPrice;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeDouble(totalPrice);
        dataOutput.writeDouble(unitPrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count = dataInput.readInt();
        this.totalPrice = dataInput.readDouble();
        this.unitPrice = dataInput.readDouble();
    }
}
