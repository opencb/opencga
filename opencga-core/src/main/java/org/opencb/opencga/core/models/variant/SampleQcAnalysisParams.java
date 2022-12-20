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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class SampleQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Sample QC analysis params. Mutational signature and genome plot are calculated for somatic"
    + " samples only";
    @DataField(description = ParamConstants.SAMPLE_QC_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION)
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

    @DataField(id = "msFitMethod", defaultValue = "FitMS", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_METHOD_DESCRIPTION)
    private String msFitMethod;

    @DataField(id = "msNBoot", description = FieldConstants.MUTATIONAL_SIGNATURE_N_BOOT_DESCRIPTION)
    private Integer msNBoot;

    @DataField(id = "msSigVersion", defaultValue = "RefSigv2", description = FieldConstants.MUTATIONAL_SIGNATURE_SIG_VERSION_DESCRIPTION)
    private String msSigVersion;

    @DataField(id = "msOrgan", description = FieldConstants.MUTATIONAL_SIGNATURE_ORGAN_DESCRIPTION)
    private String msOrgan;

    @DataField(id = "msThresholdPerc", defaultValue = "5f", description = FieldConstants.MUTATIONAL_SIGNATURE_THRESHOLD_PERC_DESCRIPTION)
    private Float msThresholdPerc;

    @DataField(id = "msThresholdPval", defaultValue = "0.05f", description = FieldConstants.MUTATIONAL_SIGNATURE_THRESHOLD_PVAL_DESCRIPTION)
    private Float msThresholdPval;

    @DataField(id = "msMaxRareSigs", defaultValue = "1", description = FieldConstants.MUTATIONAL_SIGNATURE_MAX_RARE_SIGS_DESCRIPTION)
    private Integer msMaxRareSigs;

    @DataField(id = "msSignaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_SIGNATURES_FILE_DESCRIPTION)
    private String msSignaturesFile;

    @DataField(id = "msRareSignaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_RARE_SIGNATURES_FILE_DESCRIPTION)
    private String msRareSignaturesFile;

    // Genome plot

    @DataField(id = "gpId", description = FieldConstants.GENOME_PLOT_ID_DESCRIPTION)
    private String gpId;

    @DataField(id = "gpDescription", description = FieldConstants.GENOME_PLOT_DESCRIPTION_DESCRIPTION)
    private String gpDescription;

    @DataField(id = "gpConfigFile", description = FieldConstants.GENOME_PLOT_CONFIGURATION_FILE_DESCRIPTION)
    private String gpConfigFile;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public SampleQcAnalysisParams() {
    }

    public SampleQcAnalysisParams(String sample, String vsId, String vsDescription, AnnotationVariantQueryParams vsQuery, String msId,
                                  String msDescription, String msQuery, String msFitMethod, Integer msNBoot, String msSigVersion,
                                  String msOrgan, Float msThresholdPerc, Float msThresholdPval, Integer msMaxRareSigs,
                                  String msSignaturesFile, String msRareSignaturesFile, String gpId, String gpDescription,
                                  String gpConfigFile, String outdir) {
        this.sample = sample;
        this.vsId = vsId;
        this.vsDescription = vsDescription;
        this.vsQuery = vsQuery;
        this.msId = msId;
        this.msDescription = msDescription;
        this.msQuery = msQuery;
        this.msFitMethod = msFitMethod;
        this.msNBoot = msNBoot;
        this.msSigVersion = msSigVersion;
        this.msOrgan = msOrgan;
        this.msThresholdPerc = msThresholdPerc;
        this.msThresholdPval = msThresholdPval;
        this.msMaxRareSigs = msMaxRareSigs;
        this.msSignaturesFile = msSignaturesFile;
        this.msRareSignaturesFile = msRareSignaturesFile;
        this.gpId = gpId;
        this.gpDescription = gpDescription;
        this.gpConfigFile = gpConfigFile;
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
        sb.append(", msFitMethod='").append(msFitMethod).append('\'');
        sb.append(", msNBoot=").append(msNBoot);
        sb.append(", msSigVersion='").append(msSigVersion).append('\'');
        sb.append(", msOrgan='").append(msOrgan).append('\'');
        sb.append(", msThresholdPerc=").append(msThresholdPerc);
        sb.append(", msThresholdPval=").append(msThresholdPval);
        sb.append(", msMaxRareSigs=").append(msMaxRareSigs);
        sb.append(", msSignaturesFile='").append(msSignaturesFile).append('\'');
        sb.append(", msRareSignaturesFile='").append(msRareSignaturesFile).append('\'');
        sb.append(", gpId='").append(gpId).append('\'');
        sb.append(", gpDescription='").append(gpDescription).append('\'');
        sb.append(", gpConfigFile='").append(gpConfigFile).append('\'');
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

    public String getMsFitMethod() {
        return msFitMethod;
    }

    public SampleQcAnalysisParams setMsFitMethod(String msFitMethod) {
        this.msFitMethod = msFitMethod;
        return this;
    }

    public Integer getMsNBoot() {
        return msNBoot;
    }

    public SampleQcAnalysisParams setMsNBoot(Integer msNBoot) {
        this.msNBoot = msNBoot;
        return this;
    }

    public String getMsSigVersion() {
        return msSigVersion;
    }

    public SampleQcAnalysisParams setMsSigVersion(String msSigVersion) {
        this.msSigVersion = msSigVersion;
        return this;
    }

    public String getMsOrgan() {
        return msOrgan;
    }

    public SampleQcAnalysisParams setMsOrgan(String msOrgan) {
        this.msOrgan = msOrgan;
        return this;
    }

    public Float getMsThresholdPerc() {
        return msThresholdPerc;
    }

    public SampleQcAnalysisParams setMsThresholdPerc(Float msThresholdPerc) {
        this.msThresholdPerc = msThresholdPerc;
        return this;
    }

    public Float getMsThresholdPval() {
        return msThresholdPval;
    }

    public SampleQcAnalysisParams setMsThresholdPval(Float msThresholdPval) {
        this.msThresholdPval = msThresholdPval;
        return this;
    }

    public Integer getMsMaxRareSigs() {
        return msMaxRareSigs;
    }

    public SampleQcAnalysisParams setMsMaxRareSigs(Integer msMaxRareSigs) {
        this.msMaxRareSigs = msMaxRareSigs;
        return this;
    }

    public String getMsSignaturesFile() {
        return msSignaturesFile;
    }

    public SampleQcAnalysisParams setMsSignaturesFile(String msSignaturesFile) {
        this.msSignaturesFile = msSignaturesFile;
        return this;
    }

    public String getMsRareSignaturesFile() {
        return msRareSignaturesFile;
    }

    public SampleQcAnalysisParams setMsRareSignaturesFile(String msRareSignaturesFile) {
        this.msRareSignaturesFile = msRareSignaturesFile;
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

    public String getOutdir() {
        return outdir;
    }

    public SampleQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
