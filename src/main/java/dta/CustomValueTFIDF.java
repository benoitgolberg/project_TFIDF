package dta;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValueTFIDF implements Writable {

    IntWritable nbOccurences;
    IntWritable nbWords;

    public CustomValueTFIDF() {
        nbOccurences = new IntWritable();
        nbWords.set(0);
    }

    public String toString() {
        return nbOccurences.toString() + " " + nbWords.toString();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        nbOccurences.write(dataOutput);
        nbWords.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        nbOccurences.readFields(dataInput);
        nbWords.readFields(dataInput);
    }

    public IntWritable getWord() {
        return nbOccurences;
    }

    public IntWritable getNbWords() {
        return nbWords;
    }

    public void set(IntWritable nbOccurences, IntWritable nbWords) {
        this.nbOccurences = nbOccurences;
        this.nbWords = nbWords;
    }
}
