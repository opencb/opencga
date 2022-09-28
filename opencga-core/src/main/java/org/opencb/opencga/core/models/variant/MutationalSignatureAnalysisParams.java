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
    public static final String DESCRIPTION = "Mutational signature analysis params";

    @DataField(id = "id", description = FieldConstants.MUTATIONAL_SIGNATURE_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.MUTATIONAL_SIGNATURE_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "query", description = FieldConstants.MUTATIONAL_SIGNATURE_QUERY_DESCRIPTION)
    private String query;

    // For fitting method

    @DataField(id = "catalogues", description = FieldConstants.MUTATIONAL_SIGNATURE_CATALOGUES_DESCRIPTION)
    private String catalogues;

    @DataField(id = "cataloguesContent", description = FieldConstants.MUTATIONAL_SIGNATURE_CATALOGUES_CONTENT_DESCRIPTION)
    private String cataloguesContent;

    @DataField(id = "fitMethod", defaultValue = "FitMS", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_METHOD_DESCRIPTION)
    private String fitMethod;

    @DataField(id = "nBoot", description = FieldConstants.MUTATIONAL_SIGNATURE_N_BOOT_DESCRIPTION)
    private Integer nBoot;

    @DataField(id = "sigVersion", defaultValue = "RefSigv2", description = FieldConstants.MUTATIONAL_SIGNATURE_SIG_VERSION_DESCRIPTION)
    private String sigVersion;

    @DataField(id = "organ", description = FieldConstants.MUTATIONAL_SIGNATURE_ORGAN_DESCRIPTION)
    private String organ;

    @DataField(id = "thresholdPerc", defaultValue = "5f", description = FieldConstants.MUTATIONAL_SIGNATURE_THRESHOLD_PERC_DESCRIPTION)
    private Float thresholdPerc;

    @DataField(id = "thresholdPval", defaultValue = "0.05f", description = FieldConstants.MUTATIONAL_SIGNATURE_THRESHOLD_PVAL_DESCRIPTION)
    private Float thresholdPval;

    @DataField(id = "maxRareSigs", defaultValue = "1", description = FieldConstants.MUTATIONAL_SIGNATURE_MAX_RARE_SIGS_DESCRIPTION)
    private Integer maxRareSigs;

    @DataField(id = "signaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_SIGNATURES_FILE_DESCRIPTION)
    private String signaturesFile;

    @DataField(id = "rareSignaturesFile", description = FieldConstants.MUTATIONAL_SIGNATURE_RARE_SIGNATURES_FILE_DESCRIPTION)
    private String rareSignaturesFile;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public MutationalSignatureAnalysisParams() {
    }

    public MutationalSignatureAnalysisParams(String id, String description, String query, String catalogues, String cataloguesContent,
                                             String fitMethod, Integer nBoot, String sigVersion, String organ, Float thresholdPerc,
                                             Float thresholdPval, Integer maxRareSigs, String signaturesFile, String rareSignaturesFile,
                                             String outdir) {
        this.id = id;
        this.description = description;
        this.query = query;
        this.catalogues = catalogues;
        this.cataloguesContent = cataloguesContent;
        this.fitMethod = fitMethod;
        this.nBoot = nBoot;
        this.sigVersion = sigVersion;
        this.organ = organ;
        this.thresholdPerc = thresholdPerc;
        this.thresholdPval = thresholdPval;
        this.maxRareSigs = maxRareSigs;
        this.signaturesFile = signaturesFile;
        this.rareSignaturesFile = rareSignaturesFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MutationalSignatureAnalysisParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", query='").append(query).append('\'');
        sb.append(", catalogues='").append(catalogues).append('\'');
        sb.append(", cataloguesContent='").append(cataloguesContent).append('\'');
        sb.append(", fitMethod='").append(fitMethod).append('\'');
        sb.append(", nBoot=").append(nBoot);
        sb.append(", sigVersion='").append(sigVersion).append('\'');
        sb.append(", organ='").append(organ).append('\'');
        sb.append(", thresholdPerc=").append(thresholdPerc);
        sb.append(", thresholdPval=").append(thresholdPval);
        sb.append(", maxRareSigs=").append(maxRareSigs);
        sb.append(", signaturesFile='").append(signaturesFile).append('\'');
        sb.append(", rareSignaturesFile='").append(rareSignaturesFile).append('\'');
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

    public String getQuery() {
        return query;
    }

    public MutationalSignatureAnalysisParams setQuery(String query) {
        this.query = query;
        return this;
    }

    public String getCatalogues() {
        return catalogues;
    }

    public MutationalSignatureAnalysisParams setCatalogues(String catalogues) {
        this.catalogues = catalogues;
        return this;
    }

    public String getCataloguesContent() {
        return cataloguesContent;
    }

    public MutationalSignatureAnalysisParams setCataloguesContent(String cataloguesContent) {
        this.cataloguesContent = cataloguesContent;
        return this;
    }

    public String getFitMethod() {
        return fitMethod;
    }

    public MutationalSignatureAnalysisParams setFitMethod(String fitMethod) {
        this.fitMethod = fitMethod;
        return this;
    }

    public Integer getnBoot() {
        return nBoot;
    }

    public MutationalSignatureAnalysisParams setnBoot(Integer nBoot) {
        this.nBoot = nBoot;
        return this;
    }

    public String getSigVersion() {
        return sigVersion;
    }

    public MutationalSignatureAnalysisParams setSigVersion(String sigVersion) {
        this.sigVersion = sigVersion;
        return this;
    }

    public String getOrgan() {
        return organ;
    }

    public MutationalSignatureAnalysisParams setOrgan(String organ) {
        this.organ = organ;
        return this;
    }

    public Float getThresholdPerc() {
        return thresholdPerc;
    }

    public MutationalSignatureAnalysisParams setThresholdPerc(Float thresholdPerc) {
        this.thresholdPerc = thresholdPerc;
        return this;
    }

    public Float getThresholdPval() {
        return thresholdPval;
    }

    public MutationalSignatureAnalysisParams setThresholdPval(Float thresholdPval) {
        this.thresholdPval = thresholdPval;
        return this;
    }

    public Integer getMaxRareSigs() {
        return maxRareSigs;
    }

    public MutationalSignatureAnalysisParams setMaxRareSigs(Integer maxRareSigs) {
        this.maxRareSigs = maxRareSigs;
        return this;
    }

    public String getSignaturesFile() {
        return signaturesFile;
    }

    public MutationalSignatureAnalysisParams setSignaturesFile(String signaturesFile) {
        this.signaturesFile = signaturesFile;
        return this;
    }

    public String getRareSignaturesFile() {
        return rareSignaturesFile;
    }

    public MutationalSignatureAnalysisParams setRareSignaturesFile(String rareSignaturesFile) {
        this.rareSignaturesFile = rareSignaturesFile;
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
