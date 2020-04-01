/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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