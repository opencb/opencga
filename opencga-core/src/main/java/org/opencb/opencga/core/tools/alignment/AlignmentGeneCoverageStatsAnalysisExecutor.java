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

package org.opencb.opencga.core.tools.alignment;

import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.ArrayList;
import java.util.List;

public abstract class AlignmentGeneCoverageStatsAnalysisExecutor extends OpenCgaToolExecutor {

    protected String studyId;
    protected String bamFileId;
    protected List<String> genes;

    protected List<GeneCoverageStats> geneCoverageStats;

    public AlignmentGeneCoverageStatsAnalysisExecutor() {
    }

    public AlignmentGeneCoverageStatsAnalysisExecutor(String studyId, String bamFileId, List<String> genes) {
        this.studyId = studyId;
        this.bamFileId = bamFileId;
        this.genes = genes;
        this.geneCoverageStats = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentGeneCoverageStatsAnalysisExecutor{");
        sb.append("studyId='").append(studyId).append('\'');
        sb.append(", bamFileId='").append(bamFileId).append('\'');
        sb.append(", genes=").append(genes);
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append('}');
        return sb.toString();
    }

    public String getStudyId() {
        return studyId;
    }

    public AlignmentGeneCoverageStatsAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getBamFileId() {
        return bamFileId;
    }

    public AlignmentGeneCoverageStatsAnalysisExecutor setBamFileId(String bamFileId) {
        this.bamFileId = bamFileId;
        return this;
    }

    public List<String> getGenes() {
        return genes;
    }

    public AlignmentGeneCoverageStatsAnalysisExecutor setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public AlignmentGeneCoverageStatsAnalysisExecutor setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }
}
