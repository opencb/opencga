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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class SampleQcAnalysisParams extends ToolParams {
    public static final String VARIANT_STATS_SKIP_VALUE = "variant-stats";
    public static final String SIGNATURE_SKIP_VALUE = "signature";
    public static final String SIGNATURE_CATALOGUE_SKIP_VALUE = "signature-catalogue";
    public static final String SIGNATURE_FITTING_SKIP_VALUE = "signature-fitting";
    public static final String GENOME_PLOT_SKIP_VALUE = "genome-plot";

    public static final String DESCRIPTION = "Sample QC analysis params. Mutational signature and genome plot are calculated for somatic"
            + " samples only. In order to skip some metrics, use the following keywords (separated by commas): " + VARIANT_STATS_SKIP_VALUE
            + ", " + SIGNATURE_SKIP_VALUE  + ", " + SIGNATURE_CATALOGUE_SKIP_VALUE + ", " + SIGNATURE_FITTING_SKIP_VALUE + ", "
            + GENOME_PLOT_SKIP_VALUE;

    @DataField(id = "sample", description = FieldConstants.SAMPLE_ID_DESCRIPTION)
    private String sample;

    // Variant stats params
    @DataField(id = "vsId", description = FieldConstants.VARIANT_STATS_ID_DESCRIPTION)
    private String vsId;

    @DataField(id = "vsDescription", description = FieldConstants.VARIANT_STATS_DESCRIPTION_DESCRIPTION)
    private String vsDescription;

    @DataField(id = "vsQuery", description = FieldConstants.VARIANT_STATS_QUERY_DESCRIPTION)
    private AnnotationVariantQueryParams vsQuery;

    // Mutationsl signature params

    @DataField(id = "msId", description = FieldConstants.MUTATIONAL_SIGNATURE_ID_DESCRIPTION)
    private String msId;

    @DataField(id = "msDescription", description = FieldConstants.MUTATIONAL_SIGNATURE_DESCRIPTION_DESCRIPTION)
    private String msDescription;

    @DataField(id = "msQuery", description = FieldConstants.MUTATIONAL_SIGNATURE_QUERY_DESCRIPTION)
    private String msQuery;

    @DataField(id = "msFitId", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_METHOD_DESCRIPTION)
    private String msFitId;

    @DataField(id = "msFitMethod", defaultValue = "FitMS", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_METHOD_DESCRIPTION)
    private String msFitMethod;

    @DataField(id = "msFitNBoot", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_N_BOOT_DESCRIPTION)
    private Integer msFitNBoot;

    @DataField(id = "msFitSigVersion", defaultValue = "RefSigv2",
            description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_SIG_VERSION_DESCRIPTION)
    private String msFitSigVersion;

    @DataField(id = "msFitOrgan", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_ORGAN_DESCRIPTION)
    private String msFitOrgan;

    @DataField(id = "msFitThresholdPerc", defaultValue = "5f",
            description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_THRESHOLD_PERC_DESCRIPTION)
    private Float msFitThresholdPerc;

    @DataField(id = "msFitThresholdPval", defaultValue = "0.05f",
            description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_THRESHOLD_PVAL_DESCRIPTION)
    private Float msFitThresholdPval;

    @DataField(id = "msFitMaxRareSigs", defaultValue = "1", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_MAX_RARE_SIGS_DESCRIPTION)
    private Integer msFitMaxRareSigs;

    @DataField(id = "msFitSignaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_SIGNATURES_FILE_DESCRIPTION)
    private String msFitSignaturesFile;

    @DataField(id = "msFitRareSignaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_RARE_SIGNATURES_FILE_DESCRIPTION)
    private String msFitRareSignaturesFile;

    // Genome plot

    @DataField(id = "gpId", description = FieldConstants.GENOME_PLOT_ID_DESCRIPTION)
    private String gpId;

    @DataField(id = "gpDescription", description = FieldConstants.GENOME_PLOT_DESCRIPTION_DESCRIPTION)
    private String gpDescription;

    @DataField(id = "gpConfigFile", description = FieldConstants.GENOME_PLOT_CONFIGURATION_FILE_DESCRIPTION)
    private String gpConfigFile;

    // Other
    @DataField(id = "skip", description = FieldConstants.SAMPLE_QUALITY_CONTROL_SKIP_DESCRIPTION)
    private String skip;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public SampleQcAnalysisParams() {
    }

    public SampleQcAnalysisParams(String sample, String vsId, String vsDescription, AnnotationVariantQueryParams vsQuery, String msId,
                                  String msDescription, String msQuery, String msFitId, String msFitMethod, Integer msFitNBoot,
                                  String msFitSigVersion, String msFitOrgan, Float msFitThresholdPerc, Float msFitThresholdPval,
                                  Integer msFitMaxRareSigs, String msFitSignaturesFile, String msFitRareSignaturesFile, String gpId,
                                  String gpDescription, String gpConfigFile, String skip, String outdir) {
        this.sample = sample;
        this.vsId = vsId;
        this.vsDescription = vsDescription;
        this.vsQuery = vsQuery;
        this.msId = msId;
        this.msDescription = msDescription;
        this.msQuery = msQuery;
        this.msFitId = msFitId;
        this.msFitMethod = msFitMethod;
        this.msFitNBoot = msFitNBoot;
        this.msFitSigVersion = msFitSigVersion;
        this.msFitOrgan = msFitOrgan;
        this.msFitThresholdPerc = msFitThresholdPerc;
        this.msFitThresholdPval = msFitThresholdPval;
        this.msFitMaxRareSigs = msFitMaxRareSigs;
        this.msFitSignaturesFile = msFitSignaturesFile;
        this.msFitRareSignaturesFile = msFitRareSignaturesFile;
        this.gpId = gpId;
        this.gpDescription = gpDescription;
        this.gpConfigFile = gpConfigFile;
        this.skip = skip;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQcAnalysisParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", vsId='").append(vsId).append('\'');
        sb.append(", vsDescription='").append(vsDescription).append('\'');
        sb.append(", vsQuery=").append(vsQuery);
        sb.append(", msId='").append(msId).append('\'');
        sb.append(", msDescription='").append(msDescription).append('\'');
        sb.append(", msQuery='").append(msQuery).append('\'');
        sb.append(", msFitId='").append(msFitId).append('\'');
        sb.append(", msFitMethod='").append(msFitMethod).append('\'');
        sb.append(", msFitNBoot=").append(msFitNBoot);
        sb.append(", msFitSigVersion='").append(msFitSigVersion).append('\'');
        sb.append(", msFitOrgan='").append(msFitOrgan).append('\'');
        sb.append(", msFitThresholdPerc=").append(msFitThresholdPerc);
        sb.append(", msFitThresholdPval=").append(msFitThresholdPval);
        sb.append(", msFitMaxRareSigs=").append(msFitMaxRareSigs);
        sb.append(", msFitSignaturesFile='").append(msFitSignaturesFile).append('\'');
        sb.append(", msFitRareSignaturesFile='").append(msFitRareSignaturesFile).append('\'');
        sb.append(", gpId='").append(gpId).append('\'');
        sb.append(", gpDescription='").append(gpDescription).append('\'');
        sb.append(", gpConfigFile='").append(gpConfigFile).append('\'');
        sb.append(", skip='").append(skip).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public SampleQcAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getVsId() {
        return vsId;
    }

    public SampleQcAnalysisParams setVsId(String vsId) {
        this.vsId = vsId;
        return this;
    }

    public String getVsDescription() {
        return vsDescription;
    }

    public SampleQcAnalysisParams setVsDescription(String vsDescription) {
        this.vsDescription = vsDescription;
        return this;
    }

    public AnnotationVariantQueryParams getVsQuery() {
        return vsQuery;
    }

    public SampleQcAnalysisParams setVsQuery(AnnotationVariantQueryParams vsQuery) {
        this.vsQuery = vsQuery;
        return this;
    }

    public String getMsId() {
        return msId;
    }

    public SampleQcAnalysisParams setMsId(String msId) {
        this.msId = msId;
        return this;
    }

    public String getMsDescription() {
        return msDescription;
    }

    public SampleQcAnalysisParams setMsDescription(String msDescription) {
        this.msDescription = msDescription;
        return this;
    }

    public String getMsQuery() {
        return msQuery;
    }

    public SampleQcAnalysisParams setMsQuery(String msQuery) {
        this.msQuery = msQuery;
        return this;
    }

    public String getMsFitId() {
        return msFitId;
    }

    public SampleQcAnalysisParams setMsFitId(String msFitId) {
        this.msFitId = msFitId;
        return this;
    }

    public String getMsFitMethod() {
        return msFitMethod;
    }

    public SampleQcAnalysisParams setMsFitMethod(String msFitMethod) {
        this.msFitMethod = msFitMethod;
        return this;
    }

    public Integer getMsFitNBoot() {
        return msFitNBoot;
    }

    public SampleQcAnalysisParams setMsFitNBoot(Integer msFitNBoot) {
        this.msFitNBoot = msFitNBoot;
        return this;
    }

    public String getMsFitSigVersion() {
        return msFitSigVersion;
    }

    public SampleQcAnalysisParams setMsFitSigVersion(String msFitSigVersion) {
        this.msFitSigVersion = msFitSigVersion;
        return this;
    }

    public String getMsFitOrgan() {
        return msFitOrgan;
    }

    public SampleQcAnalysisParams setMsFitOrgan(String msFitOrgan) {
        this.msFitOrgan = msFitOrgan;
        return this;
    }

    public Float getMsFitThresholdPerc() {
        return msFitThresholdPerc;
    }

    public SampleQcAnalysisParams setMsFitThresholdPerc(Float msFitThresholdPerc) {
        this.msFitThresholdPerc = msFitThresholdPerc;
        return this;
    }

    public Float getMsFitThresholdPval() {
        return msFitThresholdPval;
    }

    public SampleQcAnalysisParams setMsFitThresholdPval(Float msFitThresholdPval) {
        this.msFitThresholdPval = msFitThresholdPval;
        return this;
    }

    public Integer getMsFitMaxRareSigs() {
        return msFitMaxRareSigs;
    }

    public SampleQcAnalysisParams setMsFitMaxRareSigs(Integer msFitMaxRareSigs) {
        this.msFitMaxRareSigs = msFitMaxRareSigs;
        return this;
    }

    public String getMsFitSignaturesFile() {
        return msFitSignaturesFile;
    }

    public SampleQcAnalysisParams setMsFitSignaturesFile(String msFitSignaturesFile) {
        this.msFitSignaturesFile = msFitSignaturesFile;
        return this;
    }

    public String getMsFitRareSignaturesFile() {
        return msFitRareSignaturesFile;
    }

    public SampleQcAnalysisParams setMsFitRareSignaturesFile(String msFitRareSignaturesFile) {
        this.msFitRareSignaturesFile = msFitRareSignaturesFile;
        return this;
    }

    public String getGpId() {
        return gpId;
    }

    public SampleQcAnalysisParams setGpId(String gpId) {
        this.gpId = gpId;
        return this;
    }

    public String getGpDescription() {
        return gpDescription;
    }

    public SampleQcAnalysisParams setGpDescription(String gpDescription) {
        this.gpDescription = gpDescription;
        return this;
    }

    public String getGpConfigFile() {
        return gpConfigFile;
    }

    public SampleQcAnalysisParams setGpConfigFile(String gpConfigFile) {
        this.gpConfigFile = gpConfigFile;
        return this;
    }

    public String getSkip() {
        return skip;
    }

    public SampleQcAnalysisParams setSkip(String skip) {
        this.skip = skip;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SampleQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
