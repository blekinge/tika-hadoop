package eu.scape_project.tb.wc.archd.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 9/5/12
 * Time: 2:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class ZipMap extends Mapper<Text, Path, Text, LongWritable> {

    @Override
    protected void map(Text key, Path value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);    //To change body of overridden methods use File | Settings | File Templates.


        ZipInputStream stream = new ZipInputStream(value.getFileSystem(context.getConfiguration()).open(value));
        ZipEntry entry;
        while ((entry = stream.getNextEntry()) != null){
            String name = entry.getName();
            String ext = name.substring(name.lastIndexOf(".") + 1).intern();
            long size = entry.getSize();
            context.write(new Text(ext),new LongWritable(size));
        }
    }
}
