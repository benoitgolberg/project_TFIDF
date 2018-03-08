package dta;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class WordCountMapReduceTest {

    MapDriver<Object, Text, Text, CustomValueOpt> mapDriver;
    ReduceDriver<Text, CustomValueOpt, CustomKey, ValueSumOccTotal> reduceDriver;
    private static final Path placeholderFilePath = new Path("fileName");

    @Before
    public void setUp() {
        MapOpt mapper = new MapOpt();
        ReduceOpt reducer = new ReduceOpt();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.setMapInputPath(placeholderFilePath);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("horse horse zebra"));

        mapDriver.withOutput(new Text(placeholderFilePath.toString()), new CustomValueOpt("horse", 1));
        mapDriver.withOutput(new Text(placeholderFilePath.toString()), new CustomValueOpt("horse", 1));
        mapDriver.withOutput(new Text(placeholderFilePath.toString()), new CustomValueOpt("zebra", 1));

        mapDriver.runTest();
    }


}
