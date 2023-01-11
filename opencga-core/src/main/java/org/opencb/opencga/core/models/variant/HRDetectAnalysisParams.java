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

public class HRDetectAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "HRDetect analysis parameters.";

    @DataField(id = "id", description = FieldConstants.HRDETECT_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.HRDETECT_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "sampleId", description = FieldConstants.SAMPLE_ID_DESCRIPTION)
    private String sampleId;

    @DataField(id = "svnFittingId", description = FieldConstants.HRDETECT_SNV_FITTING_ID_DESCRIPTION)
    private String snvFittingId;

    @DataField(id = "svFittingId", description = FieldConstants.HRDETECT_SV_FITTING_ID_DESCRIPTION)
    private String svFittingId;

    @DataField(id = "cnvQuery", description = FieldConstants.HRDETECT_CNV_QUERY_DESCRIPTION)
    private String cnvQuery;

    @DataField(id = "indelQuery", description = FieldConstants.HRDETECT_INDEL_QUERY_DESCRIPTION)
    private String indelQuery;

    @DataField(id = "snv3CustomName", description = FieldConstants.HRDETECT_SNV3_CUSTOM_NAME_DESCRIPTION)
    private String snv3CustomName;

    @DataField(id = "snv8CustomName", description = FieldConstants.HRDETECT_SNV8_CUSTOM_NAME_DESCRIPTION)
    private String snv8CustomName;

    @DataField(id = "sv3CustomName", description = FieldConstants.HRDETECT_SV3_CUSTOM_NAME_DESCRIPTION)
    private String sv3CustomName;

    @DataField(id = "sv8CustomName", description = FieldConstants.HRDETECT_SV8_CUSTOM_NAME_DESCRIPTION)
    private String sv8CustomName;

    @DataField(id = "bootstrap", description = FieldConstants.HRDETECT_BOOTSTRAP_DESCRIPTION)
    private Boolean bootstrap;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public HRDetectAnalysisParams() {
    }

    public HRDetectAnalysisParams(String id, String description, String sampleId, String snvFittingId, String svFittingId, String cnvQuery,
                                  String indelQuery, String snv3CustomName, String snv8CustomName, String sv3CustomName,
                                  String sv8CustomName, Boolean bootstrap, String outdir) {
        this.id = id;
        this.description = description;
        this.sampleId = sampleId;
        this.snvFittingId = snvFittingId;
        this.svFittingId = svFittingId;
        this.cnvQuery = cnvQuery;
        this.indelQuery = indelQuery;
        this.snv3CustomName = snv3CustomName;
        this.snv8CustomName = snv8CustomName;
        this.sv3CustomName = sv3CustomName;
        this.sv8CustomName = sv8CustomName;
        this.bootstrap = bootstrap;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HRDetectAnalysisParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", sampleId='").append(sampleId).append('\'');
        sb.append(", snvFittingId='").append(snvFittingId).append('\'');
        sb.append(", svFittingId='").append(svFittingId).append('\'');
        sb.append(", cnvQuery='").append(cnvQuery).append('\'');
        sb.append(", indelQuery='").append(indelQuery).append('\'');
        sb.append(", snv3CustomName='").append(snv3CustomName).append('\'');
        sb.append(", snv8CustomName='").append(snv8CustomName).append('\'');
        sb.append(", sv3CustomName='").append(sv3CustomName).append('\'');
        sb.append(", sv8CustomName='").append(sv8CustomName).append('\'');
        sb.append(", bootstrap=").append(bootstrap);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public HRDetectAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public HRDetectAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public HRDetectAnalysisParams setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getSnvFittingId() {
        return snvFittingId;
    }

    public HRDetectAnalysisParams setSnvFittingId(String snvFittingId) {
        this.snvFittingId = snvFittingId;
        return this;
    }

    public String getSvFittingId() {
        return svFittingId;
    }

    public HRDetectAnalysisParams setSvFittingId(String svFittingId) {
        this.svFittingId = svFittingId;
        return this;
    }

    public String getCnvQuery() {
        return cnvQuery;
    }

    public HRDetectAnalysisParams setCnvQuery(String cnvQuery) {
        this.cnvQuery = cnvQuery;
        return this;
    }

    public String getIndelQuery() {
        return indelQuery;
    }

    public HRDetectAnalysisParams setIndelQuery(String indelQuery) {
        this.indelQuery = indelQuery;
        return this;
    }

    public String getSnv3CustomName() {
        return snv3CustomName;
    }

    public HRDetectAnalysisParams setSnv3CustomName(String snv3CustomName) {
        this.snv3CustomName = snv3CustomName;
        return this;
    }

    public String getSnv8CustomName() {
        return snv8CustomName;
    }

    public HRDetectAnalysisParams setSnv8CustomName(String snv8CustomName) {
        this.snv8CustomName = snv8CustomName;
        return this;
    }

    public String getSv3CustomName() {
        return sv3CustomName;
    }

    public HRDetectAnalysisParams setSv3CustomName(String sv3CustomName) {
        this.sv3CustomName = sv3CustomName;
        return this;
    }

    public String getSv8CustomName() {
        return sv8CustomName;
    }

    public HRDetectAnalysisParams setSv8CustomName(String sv8CustomName) {
        this.sv8CustomName = sv8CustomName;
        return this;
    }

    public Boolean isBootstrap() {
        return bootstrap;
    }

    public HRDetectAnalysisParams setBootstrap(Boolean bootstrap) {
        this.bootstrap = bootstrap;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public HRDetectAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}

