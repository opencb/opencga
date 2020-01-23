package org.opencb.opencga.analysis.variant.samples;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.core.exceptions.ToolException;

public class SampleEligibilityAnalysisTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Test
    public void checkValidQueryFilters1() throws ToolException {
        thrown.expectMessage("includeSample");
        thrown.expectMessage("includeFormat");
        SampleEligibilityAnalysis.checkValidQueryFilters(new TreeQuery("includeSample=s1,s2,s3 AND includeFormat=DP"));
    }

    @Test
    public void checkValidQueryFilters2() throws ToolException {
        thrown.expectMessage("genotype");
        SampleEligibilityAnalysis.checkValidQueryFilters(new TreeQuery("genotype=s1:1/2"));
    }

    @Test
    public void checkValidQueryFilters3() throws ToolException {
        thrown.expectMessage("unknownFilter");
        TreeQuery treeQuery = new TreeQuery("(biotype=pritein_coding) OR ( NOT ( (gene = BRCA2) AND (ct=missense AND unknownFilter=anything) )) ");
//        treeQuery.log();
        SampleEligibilityAnalysis.checkValidQueryFilters(treeQuery);
    }
}