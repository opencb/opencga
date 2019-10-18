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

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.opencb.opencga.storage.core.manager.clinical.ClinicalUtils.readReportedVariants;

public abstract class FamilyInterpretationAnalysis extends InterpretationAnalysis {

    @Deprecated
    protected final static String SEPARATOR = "__";

//    public FamilyInterpretationAnalysis(ObjectMap executorParams, Path outDir) {
//        super(executorParams, outDir);
//    }
//
//    public FamilyInterpretationAnalysis(String clinicalAnalysisId, String studyId, Path outDir, ObjectMap executorParams, Path opencgaHome,
//                                        String sessionId) {
//        super(clinicalAnalysisId, studyId, outDir, executorParams, opencgaHome, sessionId);
//    }

    public FamilyInterpretationAnalysis(String clinicalAnalysisId, String studyId, Path outDir, Path openCgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, outDir, openCgaHome, sessionId);
    }
}
