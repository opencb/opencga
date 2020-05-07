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

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class VariantIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Variant index params";

    public VariantIndexParams() {
    }

    public VariantIndexParams(String file,
                              boolean resume, String outdir,
                              boolean transform,
                              boolean gvcf,
                              boolean normalizationSkip, String referenceGenome,
                              boolean family,
                              boolean load, String loadSplitData, boolean loadMultiFileData,
                              String loadSampleIndex,
                              String loadArchive,
                              String loadHomRef,
                              String postLoadCheck,
                              boolean excludeGenotype, String includeSampleData, String merge,
                              String deduplicationPolicy,
                              boolean calculateStats, Aggregation aggregated, String aggregationMappingFile, boolean annotate,
                              String annotator, boolean overwriteAnnotations, boolean indexSearch) {
        this.file = file;
        this.resume = resume;
        this.outdir = outdir;
        this.transform = transform;
        this.gvcf = gvcf;
        this.normalizationSkip = normalizationSkip;
        this.referenceGenome = referenceGenome;
        this.family = family;
        this.load = load;
        this.loadSplitData = loadSplitData;
        this.loadMultiFileData = loadMultiFileData;
        this.loadSampleIndex = loadSampleIndex;
        this.loadArchive = loadArchive;
        this.loadHomRef = loadHomRef;
        this.postLoadCheck = postLoadCheck;
        this.excludeGenotype = excludeGenotype;
        this.includeSampleData = includeSampleData;
        this.merge = merge;
        this.deduplicationPolicy = deduplicationPolicy;
        this.calculateStats = calculateStats;
        this.aggregated = aggregated;
        this.aggregationMappingFile = aggregationMappingFile;
        this.annotate = annotate;
        this.annotator = annotator;
        this.overwriteAnnotations = overwriteAnnotations;
        this.indexSearch = indexSearch;
    }

    private String file;
    private boolean resume;
    private String outdir;

    private boolean transform;
    private boolean gvcf;

    private boolean normalizationSkip;
    private String referenceGenome;

    private boolean family;

    private boolean load;
    private String loadSplitData;
    private boolean loadMultiFileData;
    private String loadSampleIndex;
    private String loadArchive;
    private String loadHomRef;
    private String postLoadCheck;
    private boolean excludeGenotype;
    private String includeSampleData = ParamConstants.ALL;
    private String merge;
    private String deduplicationPolicy;

    private boolean calculateStats;
    private Aggregation aggregated = Aggregation.NONE;
    private String aggregationMappingFile;

    private boolean annotate;
    private String annotator;
    private boolean overwriteAnnotations;

    private boolean indexSearch;

    public String getFile() {
        return file;
    }

    public VariantIndexParams setFile(String file) {
        this.file = file;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantIndexParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public VariantIndexParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public boolean isTransform() {
        return transform;
    }

    public VariantIndexParams setTransform(boolean transform) {
        this.transform = transform;
        return this;
    }

    public boolean isGvcf() {
        return gvcf;
    }

    public VariantIndexParams setGvcf(boolean gvcf) {
        this.gvcf = gvcf;
        return this;
    }

    public boolean getNormalizationSkip() {
        return normalizationSkip;
    }

    public VariantIndexParams setNormalizationSkip(boolean normalizationSkip) {
        this.normalizationSkip = normalizationSkip;
        return this;
    }

    public String getReferenceGenome() {
        return referenceGenome;
    }

    public VariantIndexParams setReferenceGenome(String referenceGenome) {
        this.referenceGenome = referenceGenome;
        return this;
    }

    public boolean isFamily() {
        return family;
    }

    public VariantIndexParams setFamily(boolean family) {
        this.family = family;
        return this;
    }

    public boolean isLoad() {
        return load;
    }

    public VariantIndexParams setLoad(boolean load) {
        this.load = load;
        return this;
    }

    public String getLoadSplitData() {
        return loadSplitData;
    }

    public VariantIndexParams setLoadSplitData(String loadSplitData) {
        this.loadSplitData = loadSplitData;
        return this;
    }

    public boolean isLoadMultiFileData() {
        return loadMultiFileData;
    }

    public VariantIndexParams setLoadMultiFileData(boolean loadMultiFileData) {
        this.loadMultiFileData = loadMultiFileData;
        return this;
    }

    public String getLoadSampleIndex() {
        return loadSampleIndex;
    }

    public VariantIndexParams setLoadSampleIndex(String loadSampleIndex) {
        this.loadSampleIndex = loadSampleIndex;
        return this;
    }

    public String getLoadArchive() {
        return loadArchive;
    }

    public VariantIndexParams setLoadArchive(String loadArchive) {
        this.loadArchive = loadArchive;
        return this;
    }

    public String getLoadHomRef() {
        return loadHomRef;
    }

    public VariantIndexParams setLoadHomRef(String loadHomRef) {
        this.loadHomRef = loadHomRef;
        return this;
    }

    public String getPostLoadCheck() {
        return postLoadCheck;
    }

    public VariantIndexParams setPostLoadCheck(String postLoadCheck) {
        this.postLoadCheck = postLoadCheck;
        return this;
    }

    public boolean isExcludeGenotype() {
        return excludeGenotype;
    }

    public VariantIndexParams setExcludeGenotype(boolean excludeGenotype) {
        this.excludeGenotype = excludeGenotype;
        return this;
    }

    public String getIncludeSampleData() {
        return includeSampleData;
    }

    public VariantIndexParams setIncludeSampleData(String includeSampleData) {
        this.includeSampleData = includeSampleData;
        return this;
    }

    public String getMerge() {
        return merge;
    }

    public VariantIndexParams setMerge(String merge) {
        this.merge = merge;
        return this;
    }

    public String getDeduplicationPolicy() {
        return deduplicationPolicy;
    }

    public VariantIndexParams setDeduplicationPolicy(String deduplicationPolicy) {
        this.deduplicationPolicy = deduplicationPolicy;
        return this;
    }

    public boolean isCalculateStats() {
        return calculateStats;
    }

    public VariantIndexParams setCalculateStats(boolean calculateStats) {
        this.calculateStats = calculateStats;
        return this;
    }

    public Aggregation getAggregated() {
        return aggregated;
    }

    public VariantIndexParams setAggregated(Aggregation aggregated) {
        this.aggregated = aggregated;
        return this;
    }

    public String getAggregationMappingFile() {
        return aggregationMappingFile;
    }

    public VariantIndexParams setAggregationMappingFile(String aggregationMappingFile) {
        this.aggregationMappingFile = aggregationMappingFile;
        return this;
    }

    public boolean isAnnotate() {
        return annotate;
    }

    public VariantIndexParams setAnnotate(boolean annotate) {
        this.annotate = annotate;
        return this;
    }

    public String getAnnotator() {
        return annotator;
    }

    public VariantIndexParams setAnnotator(String annotator) {
        this.annotator = annotator;
        return this;
    }

    public boolean isOverwriteAnnotations() {
        return overwriteAnnotations;
    }

    public VariantIndexParams setOverwriteAnnotations(boolean overwriteAnnotations) {
        this.overwriteAnnotations = overwriteAnnotations;
        return this;
    }

    public boolean isIndexSearch() {
        return indexSearch;
    }

    public VariantIndexParams setIndexSearch(boolean indexSearch) {
        this.indexSearch = indexSearch;
        return this;
    }

}
