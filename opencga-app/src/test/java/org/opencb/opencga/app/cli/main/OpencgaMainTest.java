package org.opencb.opencga.app.cli.main;

import org.junit.Test;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;

import java.util.Collections;

/**
 * Created by wasim on 20/09/17.
 */
public class OpencgaMainTest {

    @Test
    public void generateBashAutoCompleteTest() throws Exception {
        OpencgaCliOptionsParser opencgaCliOptionsParser = new OpencgaCliOptionsParser();
        System.out.println(System.getProperty("user.dir"));
        System.out.println(System.getProperties());
//        opencgaCliOptionsParser.generateBashAutoComplete(System.getProperty("user.dir") + "/../build/conf/auto-complete", "opencga");
        opencgaCliOptionsParser.generateBashAutoComplete(System.getProperty("user.dir") + "/target/opencga", "opencga", Collections.singletonList("-D"));
    }

//    @Test
//    public void generateBashAutoCompleteTest2() throws Exception {
//        OpencgaCliOptionsParser opencgaCliOptionsParser = new OpencgaCliOptionsParser();
//        System.out.println(System.getProperty("user.dir"));
//        System.out.println(System.getProperties());
//        opencgaCliOptionsParser.generateBashAutoComplete(System.getProperty("user.dir") + "/target/auto-complete", "opencga-admin");
//    }
}
