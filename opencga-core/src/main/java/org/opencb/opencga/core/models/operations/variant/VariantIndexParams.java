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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.annotations.DataField;
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
                              boolean normalizationSkip,
                              String referenceGenome,
                              String failOnMalformedLines,
                              boolean family,
                              boolean somatic,
                              boolean load,
                              boolean forceReload,
                              String loadSplitData, boolean loadMultiFileData,
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
        this.forceReload = forceReload;
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

    @DataField(description = "List of files to be indexed.")
    private String file;
    @DataField(description = ParamConstants.RESUME_DESCRIPTION)
    private boolean resume;
    @DataField(description = "Output directory")
    private String outdir;

    @DataField(description = "If present it only runs the transform stage, no load is executed")
    private boolean transform;
    @DataField(description = "Hint to indicate that the input file is in gVCF format.")
    private boolean gvcf;

    @DataField(description = "Do not execute the normalization process. WARN: INDELs will be stored with the context base")
    private boolean normalizationSkip;
    @DataField(description = "Reference genome in FASTA format used during the normalization step "
            + "for a complete left alignment")
    private String referenceGenome;

    @DataField(description = "Fail when encountering malformed lines. (yes, no, auto) [auto]")
    private String failOnMalformedLines;

    @DataField(description = "Indicate that the files to be loaded are part of a family. "
            + "This will set 'load-hom-ref' to YES if it was in AUTO")
    private boolean family;
    @DataField(description = "Indicate that the files to be loaded contain somatic samples. "
            + "This will set 'load-hom-ref' to YES if it was in AUTO.")
    private boolean somatic;

    @DataField(description = "If present only the load stage is executed, transformation is skipped")
    private boolean load;
    @DataField(description = "If the file is already loaded, force a file reload")
    private boolean forceReload;
    @DataField(description = "Indicate that the variants from a group of samples is split in multiple files, either by CHROMOSOME or by REGION. In either case, variants from different files must not overlap.")
    private String loadSplitData;
    @DataField(description = "Indicate the presence of multiple files for the same sample. Each file could be the result of a different vcf-caller or experiment over the same sample.")
    private boolean loadMultiFileData;
    @DataField(description = "Build sample index while loading. (yes, no, auto) [auto]")
    private String loadSampleIndex;
    @DataField(description = "Load archive data. (yes, no, auto) [auto]")
    private String loadArchive;
    @DataField(description = "Load HOM_REF genotypes. (yes, no, auto) [auto]")
    private String loadHomRef;
    @DataField(description = "Execute post load checks over the database. (yes, no, auto) [auto]")
    private String postLoadCheck;
    @DataField(description = "Load the genotype data for the current file. "
            + "This only applies to the GT field from the FORMAT. All the rest of fields from the INFO and FORMAT will be loaded. "
            + "Use this parameter skip load data when the GT field is not reliable, or its only value across the file is \"./.\". "
            + "If \"auto\", genotypes will be automatically excluded if all genotypes are either missing, ./. or 0/0. "
            + "(yes, no, auto) [auto]")
    private String includeGenotypes;
    @DataField(description = "Index including other sample data fields (i.e. FORMAT fields)."
            + " Use \"" + ParamConstants.ALL + "\", \"" + ParamConstants.NONE + "\", or CSV with the fields to load.")
    private String includeSampleData;
    @DataField(deprecated = true, description = "Currently two levels of merge are supported: \"basic\" mode merge genotypes of the same variants while \"advanced\" merge multiallelic and overlapping variants.")
    private String merge;
    @DataField(description = "Specify how duplicated variants should be handled. Available policies: \"discard\", \"maxQual\"")
    private String deduplicationPolicy;

    @DataField(description = "Calculate indexed variants statistics after the load step")
    private boolean calculateStats;
    @DataField(description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC")
    private Aggregation aggregated;
    @DataField(description = "File containing population names mapping in an aggregated VCF file")
    private String aggregationMappingFile;

    @DataField(description = "Annotate indexed variants after the load step")
    private boolean annotate;
    @DataField(description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
    private String annotator;
    @DataField(description = "Overwrite annotations in variants already present")
    private boolean overwriteAnnotations;

    @DataField(description = "Add files to the secondary search index")
    private boolean indexSearch;

    @DataField(description = "Do not fail if any of the input files was already indexed.")
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

    public boolean isForceReload() {
        return forceReload;
    }

    public VariantIndexParams setForceReload(boolean forceReload) {
        this.forceReload = forceReload;
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
