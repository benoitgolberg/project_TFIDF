package dta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
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
        job.addCacheFile(new Path("/stopwords/stopwords_en.txt").toUri());

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

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomValueOpt.class);

        job.setOutputKeyClass(CustomKey.class);
        job.setOutputValueClass(ValueSumOccTotal.class);

        job.setMapperClass(MapOpt.class);
        job.setReducerClass(ReduceOpt.class);

        job.setJarByClass(App.class);



        /*Job jobTFIDF = null;
        try {
            jobTFIDF = new Job(config, "word_count");
        } catch (IOException e) {
            e.printStackTrace();
        }
        jobTFIDF.setInputFormatClass(TextInputFormat.class);
        jobTFIDF.setOutputFormatClass(TextOutputFormat.class);

        Path outputFilePathTFIDF = new Path(args[2]);
        FileOutputFormat.setOutputPath(jobTFIDF, outputFilePathTFIDF);

        try {
            FileInputFormat.addInputPath(jobTFIDF, outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            fs = FileSystem.newInstance(config);
            if(fs.exists(outputFilePathTFIDF)) {
                fs.delete(outputFilePathTFIDF,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        jobTFIDF.setOutputKeyClass(CustomKey.class);
        jobTFIDF.setOutputValueClass(CustomValueTFIDF.class);

        jobTFIDF.setMapperClass(MapTFIDF.class);
        jobTFIDF.setReducerClass(ReduceTFIDF.class);

        jobTFIDF.setJarByClass(App.class);*/



        try {
            job.waitForCompletion(true);
            //jobTFIDF.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Counters counter = new Counters();
        try {
            counter = job.getCounters();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Counter :"+ counter.findCounter(COUNTER.JOB1).getValue());
    }
}
