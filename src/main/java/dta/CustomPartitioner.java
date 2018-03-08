package dta;

import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomKey, CustomValueTFIDF> {

    @Override
    public int getPartition(CustomKey customKey, CustomValueTFIDF customValue, int i) {

        return Math.abs(customKey.getWord().hashCode()) % i;
    }
}
