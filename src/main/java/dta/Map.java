package dta;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<Object, Text, Text, IntWritable> {

    public IntWritable intWritable = new IntWritable(1);
    public Text text;

    //@Override
    public void map(Object key, Text line, Context context){
        text = new Text("");
        StringTokenizer st = new StringTokenizer(line.toString()," ");
        while(st.hasMoreElements()) {
            text.set(st.nextToken());
            try {
                context.write(text,intWritable);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
