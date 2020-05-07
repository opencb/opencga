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
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class IBDRelatednessAnalysisExecutor extends OpenCgaToolExecutor {

    private String studyId;
    private List<String> sampleIds;
    private String minorAlleleFreq;

    public IBDRelatednessAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public IBDRelatednessAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public IBDRelatednessAnalysisExecutor setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public IBDRelatednessAnalysisExecutor setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }
}
