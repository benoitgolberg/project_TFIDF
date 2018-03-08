package dta;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CustomKey implements WritableComparable<CustomKey> {

    private Text fileName;
    private Text word;

    public CustomKey() {
        fileName = new Text();
        word = new Text();
    }

    public CustomKey(Text fileName, Text word) {
        this.fileName = fileName;
        this.word = word;
    }

    public String toString() {
        return fileName.toString() + " " + word.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomKey customKey = (CustomKey) o;
        return Objects.equals(fileName, customKey.fileName) &&
                Objects.equals(word, customKey.word);
    }

    @Override
    public int hashCode() {

        return Objects.hash(fileName, word);
    }

    @Override
    public int compareTo(CustomKey o) {
        int comp = this.word.compareTo(o.word);
        if (comp != 0) {
            return comp;
        } else {
            return this.fileName.compareTo(o.fileName);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        fileName.write(dataOutput);
        word.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fileName.readFields(dataInput);
        word.readFields(dataInput);
    }

    public Text getFileName() {
        return fileName;
    }

    public Text getWord() {
        return word;
    }

    public void set(String fileName, String word) {
        this.word.set(word);
        this.fileName.set(fileName);
    }
}
