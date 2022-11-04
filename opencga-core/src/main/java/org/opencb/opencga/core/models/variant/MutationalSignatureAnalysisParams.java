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

public class MutationalSignatureAnalysisParams extends ToolParams {

    public static final String SIGNATURE_CATALOGUE_SKIP_VALUE = "catalogue";
    public static final String SIGNATURE_FITTING_SKIP_VALUE = "fitting";

    public static final String DESCRIPTION = "Mutational signature analysis parameters to index the genome context for that sample, and"
            + " to compute both catalogue counts and signature fitting. In order to skip one of them, , use the following keywords: "
            + ", " + SIGNATURE_CATALOGUE_SKIP_VALUE + ", " + SIGNATURE_FITTING_SKIP_VALUE + ".";

    // For counts (i.e., catalogue file)
    @DataField(id = "id", description = FieldConstants.MUTATIONAL_SIGNATURE_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.MUTATIONAL_SIGNATURE_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "sample", description = FieldConstants.MUTATIONAL_SIGNATURE_SAMPLE_DESCRIPTION)
    private String sample;

    @DataField(id = "query", description = FieldConstants.MUTATIONAL_SIGNATURE_QUERY_DESCRIPTION)
    private String query;

    // For fitting method
    @DataField(id = "fitId", defaultValue = "FitMS", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_ID_DESCRIPTION)
    private String fitId;

    @DataField(id = "fitMethod", defaultValue = "FitMS", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_METHOD_DESCRIPTION)
    private String fitMethod;

    @DataField(id = "fitNBoot", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_N_BOOT_DESCRIPTION)
    private Integer fitNBoot;

    @DataField(id = "fitSigVersion", defaultValue = "RefSigv2", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_SIG_VERSION_DESCRIPTION)
    private String fitSigVersion;

    @DataField(id = "fitOrgan", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_ORGAN_DESCRIPTION)
    private String fitOrgan;

    @DataField(id = "fitThresholdPerc", defaultValue = "5f", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_THRESHOLD_PERC_DESCRIPTION)
    private Float fitThresholdPerc;

    @DataField(id = "fitThresholdPval", defaultValue = "0.05f",
            description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_THRESHOLD_PVAL_DESCRIPTION)
    private Float fitThresholdPval;

    @DataField(id = "fitMaxRareSigs", defaultValue = "1", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_MAX_RARE_SIGS_DESCRIPTION)
    private Integer fitMaxRareSigs;

    @DataField(id = "fitSignaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_SIGNATURES_FILE_DESCRIPTION)
    private String fitSignaturesFile;

    @DataField(id = "fitRareSignaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_RARE_SIGNATURES_FILE_DESCRIPTION)
    private String fitRareSignaturesFile;

    // Other
    @DataField(id = "skip", description = FieldConstants.MUTATIONAL_SIGNATURE_SKIP_DESCRIPTION)
    private String skip;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public MutationalSignatureAnalysisParams() {
    }

    public MutationalSignatureAnalysisParams(String id, String description, String sample, String query, String fitId, String fitMethod,
                                             Integer fitNBoot, String fitSigVersion, String fitOrgan, Float fitThresholdPerc,
                                             Float fitThresholdPval, Integer fitMaxRareSigs, String fitSignaturesFile,
                                             String fitRareSignaturesFile, String skip, String outdir) {
        this.id = id;
        this.description = description;
        this.sample = sample;
        this.query = query;
        this.fitId = fitId;
        this.fitMethod = fitMethod;
        this.fitNBoot = fitNBoot;
        this.fitSigVersion = fitSigVersion;
        this.fitOrgan = fitOrgan;
        this.fitThresholdPerc = fitThresholdPerc;
        this.fitThresholdPval = fitThresholdPval;
        this.fitMaxRareSigs = fitMaxRareSigs;
        this.fitSignaturesFile = fitSignaturesFile;
        this.fitRareSignaturesFile = fitRareSignaturesFile;
        this.skip = skip;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MutationalSignatureAnalysisParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", query='").append(query).append('\'');
        sb.append(", fitId='").append(fitId).append('\'');
        sb.append(", fitMethod='").append(fitMethod).append('\'');
        sb.append(", fitNBoot=").append(fitNBoot);
        sb.append(", fitSigVersion='").append(fitSigVersion).append('\'');
        sb.append(", fitOrgan='").append(fitOrgan).append('\'');
        sb.append(", fitThresholdPerc=").append(fitThresholdPerc);
        sb.append(", fitThresholdPval=").append(fitThresholdPval);
        sb.append(", fitMaxRareSigs=").append(fitMaxRareSigs);
        sb.append(", fitSignaturesFile='").append(fitSignaturesFile).append('\'');
        sb.append(", fitRareSignaturesFile='").append(fitRareSignaturesFile).append('\'');
        sb.append(", skip='").append(skip).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public MutationalSignatureAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MutationalSignatureAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public MutationalSignatureAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getQuery() {
        return query;
    }

    public MutationalSignatureAnalysisParams setQuery(String query) {
        this.query = query;
        return this;
    }

    public String getFitId() {
        return fitId;
    }

    public MutationalSignatureAnalysisParams setFitId(String fitId) {
        this.fitId = fitId;
        return this;
    }

    public String getFitMethod() {
        return fitMethod;
    }

    public MutationalSignatureAnalysisParams setFitMethod(String fitMethod) {
        this.fitMethod = fitMethod;
        return this;
    }

    public Integer getFitNBoot() {
        return fitNBoot;
    }

    public MutationalSignatureAnalysisParams setFitNBoot(Integer fitNBoot) {
        this.fitNBoot = fitNBoot;
        return this;
    }

    public String getFitSigVersion() {
        return fitSigVersion;
    }

    public MutationalSignatureAnalysisParams setFitSigVersion(String fitSigVersion) {
        this.fitSigVersion = fitSigVersion;
        return this;
    }

    public String getFitOrgan() {
        return fitOrgan;
    }

    public MutationalSignatureAnalysisParams setFitOrgan(String fitOrgan) {
        this.fitOrgan = fitOrgan;
        return this;
    }

    public Float getFitThresholdPerc() {
        return fitThresholdPerc;
    }

    public MutationalSignatureAnalysisParams setFitThresholdPerc(Float fitThresholdPerc) {
        this.fitThresholdPerc = fitThresholdPerc;
        return this;
    }

    public Float getFitThresholdPval() {
        return fitThresholdPval;
    }

    public MutationalSignatureAnalysisParams setFitThresholdPval(Float fitThresholdPval) {
        this.fitThresholdPval = fitThresholdPval;
        return this;
    }

    public Integer getFitMaxRareSigs() {
        return fitMaxRareSigs;
    }

    public MutationalSignatureAnalysisParams setFitMaxRareSigs(Integer fitMaxRareSigs) {
        this.fitMaxRareSigs = fitMaxRareSigs;
        return this;
    }

    public String getFitSignaturesFile() {
        return fitSignaturesFile;
    }

    public MutationalSignatureAnalysisParams setFitSignaturesFile(String fitSignaturesFile) {
        this.fitSignaturesFile = fitSignaturesFile;
        return this;
    }

    public String getFitRareSignaturesFile() {
        return fitRareSignaturesFile;
    }

    public MutationalSignatureAnalysisParams setFitRareSignaturesFile(String fitRareSignaturesFile) {
        this.fitRareSignaturesFile = fitRareSignaturesFile;
        return this;
    }

    public String getSkip() {
        return skip;
    }

    public MutationalSignatureAnalysisParams setSkip(String skip) {
        this.skip = skip;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public MutationalSignatureAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
