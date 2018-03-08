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

public class MapOpt extends Mapper<Object, Text, Text, CustomValueOpt> {

    private CustomValueOpt customValue = new CustomValueOpt();
    private Text docID = new Text();
    private Path pathFile;
    private String word;
    private Set<String> stopWordList = new HashSet<String>();
    private BufferedReader fis;

    protected void setup(
            Mapper<Object, Text, Text, CustomValueOpt>.Context context) throws IOException, InterruptedException {
        Path[] pathCache = context.getLocalCacheFiles();
        if (pathCache != null && pathCache.length > 0) {
            //fis = new BufferedReader(new FileReader(context.getLocalCacheFiles()[0].toString()));
            fis = new BufferedReader(new FileReader(pathCache[0].toString()));
            String stopword = null;
            while ((stopword = fis.readLine()) != null) {
                stopWordList.add(stopword);
            }
        }

        super.setup(context);
    }

    //@Override
    public void map(Object key, Text line, Context context){

        pathFile = ((FileSplit) context.getInputSplit()).getPath();

        StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(), " \"\\-;\':!?+éè&()[]àç%\t,.0123456789");
        while(st.hasMoreElements()) {
            word = st.nextToken();
            try {
                if (word.length() > 2 && !stopWordList.contains(word)) {
                    docID.set(pathFile.getName());
                    customValue.set(word,1);
                    context.write(docID, customValue);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
