package dta;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class MapTFIDF extends Mapper<Object, Text, CustomKey, CustomValueTFIDF> {

    private CustomKey text = new CustomKey();
    private CustomValueTFIDF value = new CustomValueTFIDF();

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {

        StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(), " \t");

        String fileName = st.nextToken().toString();
        String mot = st.nextToken();
        Integer number = Integer.parseInt(st.nextToken().toString());
        Integer numberTotal = Integer.parseInt(st.nextToken().toString());

        text.set(mot, fileName);
        value.set(new IntWritable(numberTotal), new IntWritable(number));

        context.write(text, value);

    }
}
