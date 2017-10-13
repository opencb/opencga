/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli;

import org.junit.Test;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.analysis.AnalysisCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;

import java.util.Collections;

/**
 * Created by wasim and imedina on 20/09/17.
 */
public class OpencgaCliCompletionTest {

    @Test
    public void generateBashAutoCompleteTest() throws Exception {
        OpencgaCliOptionsParser opencgaCliOptionsParser = new OpencgaCliOptionsParser();
        CommandLineUtils.generateBashAutoComplete(opencgaCliOptionsParser.getJCommander(),
                System.getProperty("user.dir") + "/target/opencga", "opencga", Collections.singletonList("-D"));
    }

    @Test
    public void generateBashAutoCompleteTest2() throws Exception {
        AdminCliOptionsParser adminCliOptionsParser = new AdminCliOptionsParser();
        CommandLineUtils.generateBashAutoComplete(adminCliOptionsParser.getJCommander(),
                System.getProperty("user.dir") + "/target/opencga-admin", "opencga-admin", Collections.singletonList("-D"));
    }
    @Test
    public void generateBashAutoCompleteTest3() throws Exception {
        AnalysisCliOptionsParser analysisCliOptionsParser = new AnalysisCliOptionsParser();
        CommandLineUtils.generateBashAutoComplete(analysisCliOptionsParser.getJCommander(),
                System.getProperty("user.dir") + "/target/opencga-analysis", "opencga-analysis", Collections.singletonList("-D"));
    }

}
