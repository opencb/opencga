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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class SampleQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Sample QC analysis params. Mutational signature and genome plot are calculated for somatic"
    + " samples only";
    private String sample;

    // Variant stats params
    private String variantStatsId;
    private String variantStatsDescription;
    private AnnotationVariantQueryParams variantStatsQuery;

    // Mutationsl signature params

    @DataField(id = "signatureId", description = FieldConstants.MUTATIONAL_SIGNATURE_ID_DESCRIPTION)
    private String signatureId;

    @DataField(id = "signatureDescription", description = FieldConstants.MUTATIONAL_SIGNATURE_DESCRIPTION_DESCRIPTION)
    private String signatureDescription;

    @DataField(id = "signatureQuery", description = FieldConstants.MUTATIONAL_SIGNATURE_QUERY_DESCRIPTION)
    private String signatureQuery;

    @DataField(id = "signatureFitMethod", defaultValue = "FitMS", description = FieldConstants.MUTATIONAL_SIGNATURE_FIT_METHOD_DESCRIPTION)
    private String signatureFitMethod;

    @DataField(id = "signatureNBoot", description = FieldConstants.MUTATIONAL_SIGNATURE_N_BOOT_DESCRIPTION)
    private Integer signatureNBoot;

    @DataField(id = "signatureSigVersion", defaultValue = "RefSigv2", description = FieldConstants.MUTATIONAL_SIGNATURE_SIG_VERSION_DESCRIPTION)
    private String signatureSigVersion;

    @DataField(id = "signatureOrgan", description = FieldConstants.MUTATIONAL_SIGNATURE_ORGAN_DESCRIPTION)
    private String signatureOrgan;

    @DataField(id = "signatureThresholdPerc", defaultValue = "5f", description = FieldConstants.MUTATIONAL_SIGNATURE_THRESHOLD_PERC_DESCRIPTION)
    private Float signatureThresholdPerc;

    @DataField(id = "signatureThresholdPval", defaultValue = "0.05f", description = FieldConstants.MUTATIONAL_SIGNATURE_THRESHOLD_PVAL_DESCRIPTION)
    private Float signatureThresholdPval;

    @DataField(id = "signatureMaxRareSigs", defaultValue = "1", description = FieldConstants.MUTATIONAL_SIGNATURE_MAX_RARE_SIGS_DESCRIPTION)
    private Integer signatureMaxRareSigs;

    // Genome plot

    private String genomePlotId;
    private String genomePlotDescription;
    private String genomePlotConfigFile;

    private String outdir;

    public SampleQcAnalysisParams() {
    }

    public SampleQcAnalysisParams(String sample, String variantStatsId, String variantStatsDescription,
                                  AnnotationVariantQueryParams variantStatsQuery, String signatureId, String signatureDescription,
                                  String signatureQuery, String signatureFitMethod, Integer signatureNBoot, String signatureSigVersion,
                                  String signatureOrgan, Float signatureThresholdPerc, Float signatureThresholdPval,
                                  Integer signatureMaxRareSigs, String genomePlotId, String genomePlotDescription,
                                  String genomePlotConfigFile, String outdir) {
        this.sample = sample;
        this.variantStatsId = variantStatsId;
        this.variantStatsDescription = variantStatsDescription;
        this.variantStatsQuery = variantStatsQuery;
        this.signatureId = signatureId;
        this.signatureDescription = signatureDescription;
        this.signatureQuery = signatureQuery;
        this.signatureFitMethod = signatureFitMethod;
        this.signatureNBoot = signatureNBoot;
        this.signatureSigVersion = signatureSigVersion;
        this.signatureOrgan = signatureOrgan;
        this.signatureThresholdPerc = signatureThresholdPerc;
        this.signatureThresholdPval = signatureThresholdPval;
        this.signatureMaxRareSigs = signatureMaxRareSigs;
        this.genomePlotId = genomePlotId;
        this.genomePlotDescription = genomePlotDescription;
        this.genomePlotConfigFile = genomePlotConfigFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQcAnalysisParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", variantStatsId='").append(variantStatsId).append('\'');
        sb.append(", variantStatsDescription='").append(variantStatsDescription).append('\'');
        sb.append(", variantStatsQuery=").append(variantStatsQuery);
        sb.append(", signatureId='").append(signatureId).append('\'');
        sb.append(", signatureDescription='").append(signatureDescription).append('\'');
        sb.append(", signatureQuery='").append(signatureQuery).append('\'');
        sb.append(", signatureFitMethod='").append(signatureFitMethod).append('\'');
        sb.append(", signatureNBoot=").append(signatureNBoot);
        sb.append(", signatureSigVersion='").append(signatureSigVersion).append('\'');
        sb.append(", signatureOrgan='").append(signatureOrgan).append('\'');
        sb.append(", signatureThresholdPerc=").append(signatureThresholdPerc);
        sb.append(", signatureThresholdPval=").append(signatureThresholdPval);
        sb.append(", signatureMaxRareSigs=").append(signatureMaxRareSigs);
        sb.append(", genomePlotId='").append(genomePlotId).append('\'');
        sb.append(", genomePlotDescription='").append(genomePlotDescription).append('\'');
        sb.append(", genomePlotConfigFile='").append(genomePlotConfigFile).append('\'');
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

    public String getVariantStatsId() {
        return variantStatsId;
    }

    public SampleQcAnalysisParams setVariantStatsId(String variantStatsId) {
        this.variantStatsId = variantStatsId;
        return this;
    }

    public String getVariantStatsDescription() {
        return variantStatsDescription;
    }

    public SampleQcAnalysisParams setVariantStatsDescription(String variantStatsDescription) {
        this.variantStatsDescription = variantStatsDescription;
        return this;
    }

    public AnnotationVariantQueryParams getVariantStatsQuery() {
        return variantStatsQuery;
    }

    public SampleQcAnalysisParams setVariantStatsQuery(AnnotationVariantQueryParams variantStatsQuery) {
        this.variantStatsQuery = variantStatsQuery;
        return this;
    }

    public String getSignatureId() {
        return signatureId;
    }

    public SampleQcAnalysisParams setSignatureId(String signatureId) {
        this.signatureId = signatureId;
        return this;
    }

    public String getSignatureDescription() {
        return signatureDescription;
    }

    public SampleQcAnalysisParams setSignatureDescription(String signatureDescription) {
        this.signatureDescription = signatureDescription;
        return this;
    }

    public String getSignatureQuery() {
        return signatureQuery;
    }

    public SampleQcAnalysisParams setSignatureQuery(String signatureQuery) {
        this.signatureQuery = signatureQuery;
        return this;
    }

    public String getSignatureFitMethod() {
        return signatureFitMethod;
    }

    public SampleQcAnalysisParams setSignatureFitMethod(String signatureFitMethod) {
        this.signatureFitMethod = signatureFitMethod;
        return this;
    }

    public Integer getSignatureNBoot() {
        return signatureNBoot;
    }

    public SampleQcAnalysisParams setSignatureNBoot(Integer signatureNBoot) {
        this.signatureNBoot = signatureNBoot;
        return this;
    }

    public String getSignatureSigVersion() {
        return signatureSigVersion;
    }

    public SampleQcAnalysisParams setSignatureSigVersion(String signatureSigVersion) {
        this.signatureSigVersion = signatureSigVersion;
        return this;
    }

    public String getSignatureOrgan() {
        return signatureOrgan;
    }

    public SampleQcAnalysisParams setSignatureOrgan(String signatureOrgan) {
        this.signatureOrgan = signatureOrgan;
        return this;
    }

    public Float getSignatureThresholdPerc() {
        return signatureThresholdPerc;
    }

    public SampleQcAnalysisParams setSignatureThresholdPerc(Float signatureThresholdPerc) {
        this.signatureThresholdPerc = signatureThresholdPerc;
        return this;
    }

    public Float getSignatureThresholdPval() {
        return signatureThresholdPval;
    }

    public SampleQcAnalysisParams setSignatureThresholdPval(Float signatureThresholdPval) {
        this.signatureThresholdPval = signatureThresholdPval;
        return this;
    }

    public Integer getSignatureMaxRareSigs() {
        return signatureMaxRareSigs;
    }

    public SampleQcAnalysisParams setSignatureMaxRareSigs(Integer signatureMaxRareSigs) {
        this.signatureMaxRareSigs = signatureMaxRareSigs;
        return this;
    }

    public String getGenomePlotId() {
        return genomePlotId;
    }

    public SampleQcAnalysisParams setGenomePlotId(String genomePlotId) {
        this.genomePlotId = genomePlotId;
        return this;
    }

    public String getGenomePlotDescription() {
        return genomePlotDescription;
    }

    public SampleQcAnalysisParams setGenomePlotDescription(String genomePlotDescription) {
        this.genomePlotDescription = genomePlotDescription;
        return this;
    }

    public String getGenomePlotConfigFile() {
        return genomePlotConfigFile;
    }

    public SampleQcAnalysisParams setGenomePlotConfigFile(String genomePlotConfigFile) {
        this.genomePlotConfigFile = genomePlotConfigFile;
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
