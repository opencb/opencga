package org.opencb.opencga.storage.alignment.json;

import org.junit.Test;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.commons.run.Runner;
import org.opencb.commons.test.GenericTest;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: jacobo
 * Date: 18/06/14
 * Time: 17:48
 *
 */
public class AlignmentJsonDataTest extends GenericTest {



    @Test
    public void jsonWrite(){
        AlignmentDataReader samReader = new AlignmentSamDataReader("/home/jacobo/Documentos/bioinfo/small.sam");
        AlignmentJsonDataWriter jsonWriter = new AlignmentJsonDataWriter(samReader, "small", Paths.get("/tmp/"));

        Runner runner = new Runner(samReader, Arrays.asList(jsonWriter), Arrays.asList(), 1);

        try {
            runner.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
