package eu.scape_project.tb.wc.archd.mapreduce;

import dk.statsbiblioteket.scape.arcunpacker.HadoopArcRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
public class TikaMap extends Mapper<Text, HadoopArcRecord, Text, LongWritable> {

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
    public void map(Text key, HadoopArcRecord value, Context context) throws IOException, InterruptedException {

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

        context.write(new Text(myTIKAout), one);

    }
}

