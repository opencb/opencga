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

package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CohortVariantStatsAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Cohort variant stats params";
    private String cohort;
    private List<String> samples;
    private boolean index;
    private String sampleAnnotation;
    private String outdir;

    public CohortVariantStatsAnalysisParams() {
    }

    public CohortVariantStatsAnalysisParams(String cohort, List<String> samples, boolean index, String sampleAnnotation,
                                            String outdir) {
        this.cohort = cohort;
        this.samples = samples;
        this.index = index;
        this.sampleAnnotation = sampleAnnotation;
        this.outdir = outdir;
    }

    public String getCohort() {
        return cohort;
    }

    public CohortVariantStatsAnalysisParams setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public CohortVariantStatsAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public boolean isIndex() {
        return index;
    }

    public CohortVariantStatsAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }

    public String getSampleAnnotation() {
        return sampleAnnotation;
    }

    public CohortVariantStatsAnalysisParams setSampleAnnotation(String sampleAnnotation) {
        this.sampleAnnotation = sampleAnnotation;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public CohortVariantStatsAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
