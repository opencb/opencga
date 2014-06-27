package org.opencb.opencga.lib.common;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;

public class IOUtilsTest {

    private static String inputFile = IOUtilsTest.class.getResource("/file.txt").getFile();

    @Test
    public void testHeadOffset() throws Exception {

        InputStream is = IOUtils.headOffset(Paths.get(inputFile), 0, 1);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println(inputLine);
        }
        in.close();

    }

    @Test
    public void testGrepFile() throws Exception {

//        InputStream is = IOUtils.grepFile(Paths.get(inputFile), "^#a.*", false, false);
        InputStream is = IOUtils.grepFile(Paths.get(inputFile), ".*", false, true);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println(inputLine);
        }
        in.close();

    }
}