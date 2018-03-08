package dta;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReduceOpt extends Reducer<Text, CustomValueOpt, CustomKey, ValueSumOccTotal> {

    public void reduce(Text key, Iterable<CustomValueOpt> values,
                       Context context) throws IOException, InterruptedException {

        Map<CustomKey, Integer> result = new HashMap<>();
        int totalWords = 0;

        for (CustomValueOpt val : values) {

            CustomKey currentKey = new CustomKey(key, new Text(val.getWord()));

            // On traite une autre occurence du mot actuel
            if (result.containsKey(currentKey)) {
                result.put(currentKey, result.get(currentKey) + 1);
            }
            // Sinon on ajoute la clé dans le hashMap
            else {
                result.put(currentKey, 1);
            }

            totalWords++;
        }

        for (CustomKey keyToWrite : result.keySet()) {
            context.write(keyToWrite, new ValueSumOccTotal(new IntWritable(result.get(keyToWrite)), new IntWritable(totalWords)));
            context.getCounter(COUNTER.JOB1).increment(1L);
        }

    }
}
