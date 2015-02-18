package org.opencb.opencga.storage.alignment.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentDataWriter;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataWriter;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataWriter;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.commons.run.Runner;
import org.opencb.commons.test.GenericTest;

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
        AlignmentJsonDataWriter jsonWriter = new AlignmentJsonDataWriter(samReader, "/tmp/small" , true);
        
        Runner runner = new Runner(samReader, Arrays.asList(jsonWriter), Arrays.asList(), 1);

        try {
            runner.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    @Test
    public void jsonRead(){
        AlignmentJsonDataReader jsonReader = new AlignmentJsonDataReader("/tmp/small", true);
//        AlignmentDataWriter writer = new AlignmentBamDataWriter("/tmp/small.bam", jsonReader);
        AlignmentDataWriter writer = new AlignmentSamDataWriter("/tmp/small.sam", jsonReader);

        
        Runner runner = new Runner(jsonReader, Arrays.asList(writer), Arrays.asList(), 1);

        
        try {
            runner.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    static class MyClass{
        private final int a;
        public final int b;
        private final String c;

        public MyClass() { this(1,2,"hello"); }

        public MyClass(int a, int b, String c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
        @Override
        public String toString() { return "MyClass{" + "a=" + a + ", b=" + b + ", c=" + c + '}'; }
        public String getC() { return c;}
    }
    @Test
    public void json() throws FileNotFoundException, IOException{
        {
            OutputStream os = new FileOutputStream("/tmp/aux.json");
            JsonFactory factory = new JsonFactory();
            ObjectMapper jsonObjectMapper = new ObjectMapper(factory);
            JsonGenerator generator = factory.createGenerator(os);

            generator.writeObject(new MyClass(5, 7, "world"));
            generator.writeObject(new MyClass(8, 9, "foo"));
            
            generator.flush();
            generator.close();
            
        }


        {
            InputStream is = new FileInputStream("/tmp/aux.json");
            JsonFactory factory = new JsonFactory();
            ObjectMapper jsonObjectMapper = new ObjectMapper(factory);
            JsonParser parser = factory.createParser(is);

            MyClass objectTest;
            
            while(parser.nextToken() != null){
                objectTest = parser.readValueAs(MyClass.class);
                System.out.println("objectTest = " + objectTest);
            }
            parser.close();

        } 
    }
    
    
}
