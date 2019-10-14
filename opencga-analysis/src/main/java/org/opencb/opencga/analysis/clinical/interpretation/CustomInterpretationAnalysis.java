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

package org.opencb.opencga.analysis.clinical.interpretation;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.nio.file.Path;

public class CustomInterpretationAnalysis extends FamilyInterpretationAnalysis {

    private Query query;

    private final static String CUSTOM_ANALYSIS_NAME = "Custom";

    private ObjectMap executorParams;
    private Path outDir;

    public CustomInterpretationAnalysis(String clinicalAnalysisId, String studyStr, Query query, ObjectMap options, String opencgaHome,
                                        String sessionId) {
        super(clinicalAnalysisId, studyStr,null, options, opencgaHome, sessionId);
        this.query = query;
    }

    @Override
    protected void exec() throws AnalysisException {
        // Get executor
        CustomInterpretationAnalysisExecutor executor = new CustomInterpretationAnalysisExecutor();

        // Set executor parameters
        executor.setup(executorParams, outDir);
//        throw new AnalysisException("Invalid input parameters for custom interpretation analysis");

        this.arm.startStep("custom-interpretation");
        executor.exec();
        this.arm.endStep(100.0F);
    }
}
