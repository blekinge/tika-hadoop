/*
 *  Copyright 2012 The SCAPE Project Consortium.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package eu.scape_project.tb.wc.archd;

import dk.statsbiblioteket.scape.arcunpacker.mapreduce.ArcInputFormat;
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
 *
 * @author onbram
 */
public class FileCharacterisation extends Configured implements Tool {

    public static final String name="HADOOP file characterise arc files application.";

    public static void main(String[] args) throws Exception {
        System.out.println(name);
        long startTime = System.currentTimeMillis();

        Tool tool = new FileCharacterisation();
        tool.setConf(new Configuration(true));
        tool.getConf().set("mapreduce.job.user.classpath.first","true");
        tool.getConf().set("mapreduce.user.classpath.first","true");

        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg" + i + ": " + args[i]);
        }


        int res = ToolRunner.run(tool.getConf(),tool, args);
        
        long elapsedTime = System.currentTimeMillis()-startTime;
        System.out.println("Processing time (sec): " + elapsedTime/1000F);


        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        System.out.println(getConf().get("mapreduce.job.user.classpath.first"));


        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg" + i + ": " + args[i]);
        }




        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(FileCharacterisation.class);
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
        job.setInputFormatClass(ArcInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        //*** Start the job and wait for it
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

}
