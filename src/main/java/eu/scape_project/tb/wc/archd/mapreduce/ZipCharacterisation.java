package eu.scape_project.tb.wc.archd.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 9/5/12
 * Time: 2:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class ZipCharacterisation extends Configured implements Tool {


    public static final String name="HADOOP tika characterise arc files application.";

    public static void main(String[] args) throws Exception {
        System.out.println(name);
        long startTime = System.currentTimeMillis();

        Tool tool = new TikaCharacterisation();
        tool.setConf(new Configuration(true));
        tool.getConf().set("mapreduce.job.user.classpath.first","true");
        tool.getConf().set("mapreduce.user.classpath.first","true");

        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg" + i + ": " + args[i]);
        }


        int res = ToolRunner.run(tool.getConf(), tool, args);

        long elapsedTime = System.currentTimeMillis()-startTime;
        System.out.println("Processing time (sec): " + elapsedTime/1000F);


        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Job job = null;//Job.getInstance(getConf());
        System.out.println(getConf().get("mapreduce.job.user.classpath.first"));


        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg" + i + ": " + args[i]);
        }





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
        job.setMapperClass(ZipMap.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);


        //*** Set the MAP output compression
        //job.getConfiguration().set("mapred.compress.map.output", "true");


        //*** Set input / output format
        job.setInputFormatClass(FileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        //*** Start the job and wait for it
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


}
