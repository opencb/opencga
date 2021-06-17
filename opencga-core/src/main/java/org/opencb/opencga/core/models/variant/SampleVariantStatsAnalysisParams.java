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
    public static final String DESCRIPTION = "Sample variant stats params. "
            + "Use index=true and indexId='' to store the result in catalog sample QC. indexId=ALL requires an empty query. "
            + "Use sample=all to compute sample stats of all samples in the variant storage.";
    private List<String> sample;
    private List<String> individual;
    private VariantQueryParams variantQuery = new VariantQueryParams();
    private String outdir;
    private boolean index;
    private boolean indexOverwrite;
    private String indexId;
    private String indexDescription;
    private Integer batchSize;

    public static class VariantQueryParams extends AnnotationVariantQueryParams {
        public String sampleData;
        public String fileData;

        public VariantQueryParams() {
        }

        public VariantQueryParams(Query query) {
            super(query);
        }

        public String getSampleData() {
            return sampleData;
        }

        public VariantQueryParams setSampleData(String sampleData) {
            this.sampleData = sampleData;
            return this;
        }

        public String getFileData() {
            return fileData;
        }

        public VariantQueryParams setFileData(String fileData) {
            this.fileData = fileData;
            return this;
        }
    }

    public SampleVariantStatsAnalysisParams() {
    }

    public SampleVariantStatsAnalysisParams(List<String> sample, List<String> individual, String outdir, boolean index,
                                            boolean indexOverwrite, String indexId, String indexDescription, Integer batchSize,
                                            AnnotationVariantQueryParams variantQuery) {
        this(sample, individual, outdir, index, indexOverwrite, indexId, indexDescription, batchSize,
                variantQuery == null ? null : new VariantQueryParams(variantQuery.toQuery()));
    }

    public SampleVariantStatsAnalysisParams(List<String> sample, List<String> individual, String outdir, boolean index,
                                            boolean indexOverwrite, String indexId, String indexDescription, Integer batchSize,
                                            Query variantQuery) {
        this(sample, individual, outdir, index, indexOverwrite, indexId, indexDescription, batchSize,
                variantQuery == null ? null : new VariantQueryParams(variantQuery));
    }

    public SampleVariantStatsAnalysisParams(List<String> sample, List<String> individual, String outdir, boolean index,
                                            boolean indexOverwrite, String indexId, String indexDescription, Integer batchSize,
                                            VariantQueryParams variantQuery) {
        this.sample = sample;
        this.individual = individual;
        this.variantQuery = variantQuery;
        this.outdir = outdir;
        this.index = index;
        this.indexOverwrite = indexOverwrite;
        this.indexId = indexId;
        this.indexDescription = indexDescription;
        this.batchSize = batchSize;
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

    public VariantQueryParams getVariantQuery() {
        return variantQuery;
    }

    public SampleVariantStatsAnalysisParams setVariantQuery(VariantQueryParams variantQuery) {
        this.variantQuery = variantQuery;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SampleVariantStatsAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public boolean isIndex() {
        return index;
    }

    public SampleVariantStatsAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }

    public boolean isIndexOverwrite() {
        return indexOverwrite;
    }

    public SampleVariantStatsAnalysisParams setIndexOverwrite(boolean indexOverwrite) {
        this.indexOverwrite = indexOverwrite;
        return this;
    }

    public String getIndexId() {
        return indexId;
    }

    public SampleVariantStatsAnalysisParams setIndexId(String indexId) {
        this.indexId = indexId;
        return this;
    }

    public String getIndexDescription() {
        return indexDescription;
    }

    public SampleVariantStatsAnalysisParams setIndexDescription(String indexDescription) {
        this.indexDescription = indexDescription;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public SampleVariantStatsAnalysisParams setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }
}
