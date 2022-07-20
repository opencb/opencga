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
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;

public class VariantIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Variant index params";

    public VariantIndexParams() {
    }

    public VariantIndexParams(String file,
                              boolean resume, String outdir,
                              boolean transform,
                              boolean gvcf,
                              boolean normalizationSkip, String referenceGenome,
                              String failOnMalformedLines,
                              boolean family,
                              boolean somatic,
                              boolean load, String loadSplitData, boolean loadMultiFileData,
                              String loadSampleIndex,
                              String loadArchive,
                              String loadHomRef,
                              String postLoadCheck,
                              String includeGenotypes, String includeSampleData, String merge,
                              String deduplicationPolicy,
                              boolean calculateStats, Aggregation aggregated, String aggregationMappingFile, boolean annotate,
                              String annotator, boolean overwriteAnnotations, boolean indexSearch, boolean skipIndexedFiles) {
        this.file = file;
        this.resume = resume;
        this.outdir = outdir;
        this.transform = transform;
        this.gvcf = gvcf;
        this.normalizationSkip = normalizationSkip;
        this.referenceGenome = referenceGenome;
        this.failOnMalformedLines = failOnMalformedLines;
        this.family = family;
        this.somatic = somatic;
        this.load = load;
        this.loadSplitData = loadSplitData;
        this.loadMultiFileData = loadMultiFileData;
        this.loadSampleIndex = loadSampleIndex;
        this.loadArchive = loadArchive;
        this.loadHomRef = loadHomRef;
        this.postLoadCheck = postLoadCheck;
        this.includeGenotypes = includeGenotypes;
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
        this.skipIndexedFiles = skipIndexedFiles;
    }

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_FILE_DESCRIPTION)
    private String file;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_RESUME_DESCRIPTION)
    private boolean resume;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_TRANSFORM_DESCRIPTION)
    private boolean transform;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_GVCF_DESCRIPTION)
    private boolean gvcf;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_NORMALIZATION_SKIP_DESCRIPTION)
    private boolean normalizationSkip;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_REFERENCE_GENOME_DESCRIPTION)
    private String referenceGenome;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_FAIL_ON_MALFORMED_LINES_DESCRIPTION)
    private String failOnMalformedLines;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_FAMILY_DESCRIPTION)
    private boolean family;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_SOMATIC_DESCRIPTION)
    private boolean somatic;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_LOAD_DESCRIPTION)
    private boolean load;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_LOAD_SPLIT_DATA_DESCRIPTION)
    private String loadSplitData;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_LOAD_MULTI_FILE_DATA_DESCRIPTION)
    private boolean loadMultiFileData;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_LOAD_SAMPLE_INDEX_DESCRIPTION)
    private String loadSampleIndex;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_LOAD_ARCHIVE_DESCRIPTION)
    private String loadArchive;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_LOAD_HOM_REF_DESCRIPTION)
    private String loadHomRef;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_POST_LOAD_CHECK_DESCRIPTION)
    private String postLoadCheck;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_INCLUDE_GENOTYPES_DESCRIPTION)
    private String includeGenotypes;
    private String includeSampleData = ParamConstants.ALL;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_MERGE_DESCRIPTION)
    private String merge;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_DEDUPLICATION_POLICY_DESCRIPTION)
    private String deduplicationPolicy;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_CALCULATE_STATS_DESCRIPTION)
    private boolean calculateStats;
    private Aggregation aggregated = Aggregation.NONE;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_AGGREGATION_MAPPING_FILE_DESCRIPTION)
    private String aggregationMappingFile;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_ANNOTATE_DESCRIPTION)
    private boolean annotate;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_ANNOTATOR_DESCRIPTION)
    private String annotator;
    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_OVERWRITE_ANNOTATIONS_DESCRIPTION)
    private boolean overwriteAnnotations;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_INDEX_SEARCH_DESCRIPTION)
    private boolean indexSearch;

    @DataField(description = ParamConstants.VARIANT_INDEX_PARAMS_SKIP_INDEXED_FILES_DESCRIPTION)
    private boolean skipIndexedFiles;

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

    public String getFailOnMalformedLines() {
        return failOnMalformedLines;
    }

    public VariantIndexParams setFailOnMalformedLines(String failOnMalformedLines) {
        this.failOnMalformedLines = failOnMalformedLines;
        return this;
    }

    public boolean isFamily() {
        return family;
    }

    public VariantIndexParams setFamily(boolean family) {
        this.family = family;
        return this;
    }

    public boolean isSomatic() {
        return somatic;
    }

    public VariantIndexParams setSomatic(boolean somatic) {
        this.somatic = somatic;
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

    public String getIncludeGenotypes() {
        return includeGenotypes;
    }

    public VariantIndexParams setIncludeGenotypes(String includeGenotypes) {
        this.includeGenotypes = includeGenotypes;
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

    public boolean isSkipIndexedFiles() {
        return skipIndexedFiles;
    }

    public VariantIndexParams setSkipIndexedFiles(boolean skipIndexedFiles) {
        this.skipIndexedFiles = skipIndexedFiles;
        return this;
    }
}
