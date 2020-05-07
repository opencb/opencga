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

package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.HashMap;
import java.util.Map;

public abstract class MendelianErrorAnalysisExecutor extends OpenCgaToolExecutor {

    private String studyId;
    private String familyId;

    private MendelianErrorReport mendelianErrorReport;

    public MendelianErrorAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public MendelianErrorAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public MendelianErrorAnalysisExecutor setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public MendelianErrorReport getMendelianErrorReport() {
        return mendelianErrorReport;
    }

    public MendelianErrorAnalysisExecutor setMendelianErrorReport(MendelianErrorReport mendelianErrorReport) {
        this.mendelianErrorReport = mendelianErrorReport;
        return this;
    }
}
