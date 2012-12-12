package eu.scape_project.tb.wc.archd;

import eu.scape_project.tb.wc.archd.mapred.TikaCharacterisation;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 9/26/12
 * Time: 3:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class TikaCharacterisationTest {
    @Before
    public void setUp() throws Exception {
        new File("target").mkdir();

    }



    @Test
    public void testRun() throws Exception {

        TikaCharacterisation.main(
                new String[]{
                        "src/test/resources/*.arc",
                        "target/" + new Date().getTime()});
    }
}
