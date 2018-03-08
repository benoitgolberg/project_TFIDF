package dta;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CustomValueOpt implements Writable {

    private Text word;
    private IntWritable occurence;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomValueOpt that = (CustomValueOpt) o;
        return Objects.equals(word, that.word) &&
                Objects.equals(occurence, that.occurence);
    }

    @Override
    public int hashCode() {

        return Objects.hash(word, occurence);
    }

    public CustomValueOpt() {
        occurence = new IntWritable(1);
        word = new Text();
    }

    public CustomValueOpt(String word, int occurence) {
        this.occurence = new IntWritable(occurence);
        this.word = new Text(word);
    }

    public String toString() {
        return word.toString() + " " + occurence.toString();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        occurence.write(dataOutput);
        word.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        occurence.readFields(dataInput);
        word.readFields(dataInput);
    }

    public IntWritable getOccurence() {
        return occurence;
    }

    public Text getWord() {
        return word;
    }

    public void set(String word, int occurence) {
        this.word.set(word);
        this.occurence.set(occurence);
    }
}
