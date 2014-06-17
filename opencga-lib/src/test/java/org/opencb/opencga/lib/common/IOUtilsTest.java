package org.opencb.opencga.lib.common;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class IOUtilsTest {

    @Test
    public void testHeadOffset() throws Exception {

        InputStream is = IOUtils.headOffset(Paths.get("/home/fsalavert/file.txt"), 0, 1);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String inputLine;
        while ((inputLine = in.readLine()) != null){
            System.out.println(inputLine);
        }
        in.close();

    }

    @Test
    public void testGrepFile() throws Exception {

//        InputStream is = IOUtils.grepFile(Paths.get("/home/fsalavert/file.txt"), "^#a.*", false, false);
        InputStream is = IOUtils.grepFile(Paths.get("/home/fsalavert/file.txt"), ".*", false, true);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String inputLine;
        while ((inputLine = in.readLine()) != null){
            System.out.println(inputLine);
        }
        in.close();

    }
}