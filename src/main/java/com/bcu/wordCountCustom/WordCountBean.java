package com.bcu.wordCountCustom;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class WordCountBean implements Writable {
    private String word;
    private int len;

    public WordCountBean() {
    }

    @Override
    public String toString() {
        return this.word + '\t' + this.len;
    }

    public void setAll(String word, int len) {
        this.setWord(word);
        this.setLen(len);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(len);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.len = dataInput.readInt();
    }
}
