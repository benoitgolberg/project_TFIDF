package dta;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Reducer;

        import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ReduceTFIDF extends Reducer<CustomKey, IntWritable, CustomKey, DoubleWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(CustomKey key, Iterable<CustomValueTFIDF> valeurs, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        Map<Double, Double> toto = new LinkedHashMap<Double, Double>();

        for (CustomValueTFIDF customValue : valeurs) {
            Double wc = Double.parseDouble(customValue.getWord().toString());
            Double wpd = Double.parseDouble(customValue.getNbWords().toString());
            toto.put(wc, wpd);
            sum += 1;
        }

        for (Map.Entry<Double, Double> entry : toto.entrySet()) {
            Double wc = entry.getKey();
            Double wpd = entry.getValue();
            Double tfidf = (wc / wpd) * Math.log(2 / sum);
            context.write(key, new DoubleWritable(tfidf));
        }

    }
}
