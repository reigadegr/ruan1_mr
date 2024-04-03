package com.bcu.secondHouse_avg2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据类型
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SecondHouseBean implements Writable {
    private Integer count;//总数
    private Double totalPrice;//总价

    public void setAll(Integer count, double totalPrice) {
        this.setCount(count);
        this.setTotalPrice(totalPrice);
    }

    @Override
    public String toString() {
        return this.count + "\t" + this.totalPrice;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeDouble(totalPrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count = dataInput.readInt();
        this.totalPrice = dataInput.readDouble();
    }
}
