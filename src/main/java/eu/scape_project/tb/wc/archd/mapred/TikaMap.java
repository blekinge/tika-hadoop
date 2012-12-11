package eu.scape_project.tb.wc.archd.mapred;

import dk.statsbiblioteket.scape.arcunpacker.HadoopArcRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 7/4/12
 * Time: 11:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class TikaMap implements Mapper<Text, HadoopArcRecord, Text, LongWritable> {

    String recMimeType;
    String recType;
    String recURL;
    Date recDate;
    int recLength;
    int recHTTPret;
    InputStream recContent;
    String myTIKAout = "";
    Metadata met = new Metadata();
    LongWritable one = new LongWritable(1);
    DefaultDetector detector = new DefaultDetector();


    @Override
    public void map(Text key, HadoopArcRecord value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

        //            recMimeType = value.getMimeType();
        //            recType = value.getType();
        //            recURL = value.getUrl();
        //            recDate = value.getDate();
        recLength = value.getLength();
        // recHTTPret = value.getHttpReturnCode();

        //excude 0 size files. Otherwise they will falsify statistics.
        // 0byte files with jpg extension => jpg detect
        // 0byte files with jpg extension => txt as http-get stored type

        if (recLength > 0) {
            recContent = value.getContents();
            met.set(Metadata.RESOURCE_NAME_KEY, key.toString());
            MediaType mediaType = detector.detect(recContent, met);
            myTIKAout = mediaType.toString().intern();
        } else {
            myTIKAout = "SIZE=0";
        }
         output.collect(new Text(myTIKAout), one);


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

