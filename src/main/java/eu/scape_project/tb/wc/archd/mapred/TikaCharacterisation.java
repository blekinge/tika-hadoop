package eu.scape_project.tb.wc.archd.mapred;

import dk.statsbiblioteket.scape.arcunpacker.mapred.ArcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 9/26/12
 * Time: 5:17 PM
 * To change this template use File | Settings | File Templates.
 */

public class TikaCharacterisation extends Configured implements Tool{

    public static final String name="HADOOP tika characterise arc files application.";

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new TikaCharacterisation(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(name);
        long startTime = System.currentTimeMillis();


        Configuration config = new Configuration(true);
        config.addResource("jobConfig.xml");

        JobConf job = new JobConf(config);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(TikaCharacterisation.class);
        job.setJobName(name);

        //*** Set interface data types
        // We are using LONG because this value can become very large on huge archives.
        // In order to use the combiner function, also the map output needs to be a LONG.
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        //*** Set up the mapper, combiner and reducer
        job.setMapperClass(TikaMap.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);


        //*** Set the MAP output compression
        //job.getConfiguration().set("mapred.compress.map.output", "true");


        //*** Set input / output format

        job.setInputFormat(ArcInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(job);
        return 0;
    }
}
