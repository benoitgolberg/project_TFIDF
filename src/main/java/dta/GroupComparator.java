package dta;

import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{

    public GroupComparator() {
        super(CustomKey.class, true);
    }

    public int compare(CustomKey key1, CustomKey key2) {
        String keyWord1 = key1.getWord().toString();
        String keyWord2 = key2.getWord().toString();
        return keyWord1.compareTo(keyWord2);
    }
}
