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

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class SampleVariantStatsAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Sample variant stats params";
    private List<String> sample;
    private List<String> individual;
    private AnnotationVariantQueryParams variantQuery;
    private String outdir;

    public SampleVariantStatsAnalysisParams() {
    }

    public SampleVariantStatsAnalysisParams(List<String> sample, List<String> individual,
                                            Query variantQuery, String outdir) {
        this(sample, individual, new AnnotationVariantQueryParams(variantQuery), outdir);
    }

    public SampleVariantStatsAnalysisParams(List<String> sample, List<String> individual,
                                            AnnotationVariantQueryParams variantQuery, String outdir) {
        this.sample = sample;
        this.individual = individual;
        this.variantQuery = variantQuery;
        this.outdir = outdir;
    }

    public List<String> getSample() {
        return sample;
    }

    public SampleVariantStatsAnalysisParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public List<String> getIndividual() {
        return individual;
    }

    public SampleVariantStatsAnalysisParams setIndividual(List<String> individual) {
        this.individual = individual;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SampleVariantStatsAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public AnnotationVariantQueryParams getVariantQuery() {
        return variantQuery;
    }

    public SampleVariantStatsAnalysisParams setVariantQuery(AnnotationVariantQueryParams variantQuery) {
        this.variantQuery = variantQuery;
        return this;
    }
}
