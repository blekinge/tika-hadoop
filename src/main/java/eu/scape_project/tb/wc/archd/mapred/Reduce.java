package eu.scape_project.tb.wc.archd.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * The reducer class of WordCount
 */
public class Reduce
        implements Reducer<Text, LongWritable, Text, LongWritable> {

    long sum;


    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        sum = 0;
        while (values.hasNext()){
            LongWritable value = values.next();
            sum += value.get();
        }
        output.collect(key,new LongWritable(sum));
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configure(JobConf job) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
