package org.opencb.opencga.app.cli.main;

import org.junit.Test;

/**
 * Created by wasim on 20/09/17.
 */
public class OpencgaMainTest {

    @Test
    public void generateBashAutoCompleteTest() throws Exception {
        OpencgaCliOptionsParser opencgaCliOptionsParser = new OpencgaCliOptionsParser();
        opencgaCliOptionsParser.generateBashAutoComplete("../build/conf/auto-complete1");
    }
}
