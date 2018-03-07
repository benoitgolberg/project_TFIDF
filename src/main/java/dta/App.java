package dta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class App 
{
    public static void main( String[] args )
    {
        Configuration config = new Configuration();
        Job job = null;
        try {
            job = new Job(config, "word_count");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputFilePath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        try {
            FileInputFormat.addInputPath(job, new Path(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileSystem fs = null;
        try {
            fs = FileSystem.newInstance(config);
            if(fs.exists(outputFilePath)) {
                fs.delete(outputFilePath,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        job.setJarByClass(App.class);

        try {
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
