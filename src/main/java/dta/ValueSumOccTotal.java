package dta;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ValueSumOccTotal implements Writable {

    private IntWritable occurence;
    private IntWritable totalWords;

    public ValueSumOccTotal() {
        occurence = new IntWritable(1);
        totalWords = new IntWritable(0);
    }

    public ValueSumOccTotal(IntWritable occurence, IntWritable totalWords) {
        this.occurence = occurence;
        this.totalWords = totalWords;
    }

    public String toString() {
        return occurence.toString() + " " + totalWords.toString();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        occurence.write(dataOutput);
        totalWords.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        occurence.readFields(dataInput);
        totalWords.readFields(dataInput);
    }

    public IntWritable getOccurence() {
        return occurence;
    }

    public IntWritable getTotalWords() {
        return totalWords;
    }

    public void set(int occurence, int totalWords) {
        this.totalWords.set(totalWords);
        this.occurence.set(occurence);
    }
}
