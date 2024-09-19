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

package org.opencb.opencga.core.models.variant.qc;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.variant.AnnotationVariantQueryParams;
import org.opencb.opencga.core.tools.ToolParams;

public class SampleQcSignatureFittingAnalysisParams extends ToolParams {

    @DataField(id = "id", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_ID_DESCRIPTION)
    private String id;

    @DataField(id = "method", defaultValue = "FitMS", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_METHOD_DESCRIPTION)
    private String method;

    @DataField(id = "nBoot", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_N_BOOT_DESCRIPTION)
    private Integer nBoot;

    @DataField(id = "sigVersion", defaultValue = "RefSigv2", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_SIG_VERSION_DESCRIPTION)
    private String sigVersion;

    @DataField(id = "organ", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_ORGAN_DESCRIPTION)
    private String organ;

    @DataField(id = "thresholdPerc", defaultValue = "5f", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_THRESHOLD_PERC_DESCRIPTION)
    private Float thresholdPerc;

    @DataField(id = "thresholdPval", defaultValue = "0.05f",
            description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_THRESHOLD_PVAL_DESCRIPTION)
    private Float thresholdPval;

    @DataField(id = "maxRareSigs", defaultValue = "1", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_MAX_RARE_SIGS_DESCRIPTION)
    private Integer maxRareSigs;

    @DataField(id = "signaturesFile", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_SIGNATURES_FILE_DESCRIPTION)
    private String signaturesFile;

    @DataField(id = "rareSignaturesFile", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_RARE_SIGNATURES_FILE_DESCRIPTION)
    private String rareSignaturesFile;

    public SampleQcSignatureFittingAnalysisParams() {
    }

    public SampleQcSignatureFittingAnalysisParams(String id, String method, Integer nBoot, String sigVersion, String organ,
                                                  Float thresholdPerc, Float thresholdPval, Integer maxRareSigs, String signaturesFile,
                                                  String rareSignaturesFile) {
        this.id = id;
        this.method = method;
        this.nBoot = nBoot;
        this.sigVersion = sigVersion;
        this.organ = organ;
        this.thresholdPerc = thresholdPerc;
        this.thresholdPval = thresholdPval;
        this.maxRareSigs = maxRareSigs;
        this.signaturesFile = signaturesFile;
        this.rareSignaturesFile = rareSignaturesFile;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQcSignatureFittingAnalysisParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", method='").append(method).append('\'');
        sb.append(", nBoot=").append(nBoot);
        sb.append(", sigVersion='").append(sigVersion).append('\'');
        sb.append(", organ='").append(organ).append('\'');
        sb.append(", thresholdPerc=").append(thresholdPerc);
        sb.append(", thresholdPval=").append(thresholdPval);
        sb.append(", maxRareSigs=").append(maxRareSigs);
        sb.append(", signaturesFile='").append(signaturesFile).append('\'');
        sb.append(", rareSignaturesFile='").append(rareSignaturesFile).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public SampleQcSignatureFittingAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public SampleQcSignatureFittingAnalysisParams setMethod(String method) {
        this.method = method;
        return this;
    }

    public Integer getnBoot() {
        return nBoot;
    }

    public SampleQcSignatureFittingAnalysisParams setnBoot(Integer nBoot) {
        this.nBoot = nBoot;
        return this;
    }

    public String getSigVersion() {
        return sigVersion;
    }

    public SampleQcSignatureFittingAnalysisParams setSigVersion(String sigVersion) {
        this.sigVersion = sigVersion;
        return this;
    }

    public String getOrgan() {
        return organ;
    }

    public SampleQcSignatureFittingAnalysisParams setOrgan(String organ) {
        this.organ = organ;
        return this;
    }

    public Float getThresholdPerc() {
        return thresholdPerc;
    }

    public SampleQcSignatureFittingAnalysisParams setThresholdPerc(Float thresholdPerc) {
        this.thresholdPerc = thresholdPerc;
        return this;
    }

    public Float getThresholdPval() {
        return thresholdPval;
    }

    public SampleQcSignatureFittingAnalysisParams setThresholdPval(Float thresholdPval) {
        this.thresholdPval = thresholdPval;
        return this;
    }

    public Integer getMaxRareSigs() {
        return maxRareSigs;
    }

    public SampleQcSignatureFittingAnalysisParams setMaxRareSigs(Integer maxRareSigs) {
        this.maxRareSigs = maxRareSigs;
        return this;
    }

    public String getSignaturesFile() {
        return signaturesFile;
    }

    public SampleQcSignatureFittingAnalysisParams setSignaturesFile(String signaturesFile) {
        this.signaturesFile = signaturesFile;
        return this;
    }

    public String getRareSignaturesFile() {
        return rareSignaturesFile;
    }

    public SampleQcSignatureFittingAnalysisParams setRareSignaturesFile(String rareSignaturesFile) {
        this.rareSignaturesFile = rareSignaturesFile;
        return this;
    }
}
