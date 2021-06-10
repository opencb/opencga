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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.ToolParams;

public class SampleQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Sample QC analysis params. Mutational signature and genome plot are calculated for somatic"
    + " samples only";
    private String sample;
    private String variantStatsId;
    private String variantStatsDescription;
    private AnnotationVariantQueryParams variantStatsQuery;
    private String signatureId;
    private String signatureDescription;
    private ObjectMap signatureQuery;
    private String signatureRelease;
    private String genomePlotId;
    private String genomePlotDescription;
    private String genomePlotConfigFile;
    private String outdir;

    public SampleQcAnalysisParams() {
    }

    public SampleQcAnalysisParams(String sample, String variantStatsId, String variantStatsDescription,
                                  AnnotationVariantQueryParams variantStatsQuery, String signatureId, String signatureDescription,
                                  ObjectMap signatureQuery, String signatureRelease, String genomePlotId, String genomePlotDescription,
                                  String genomePlotConfigFile, String outdir) {
        this.sample = sample;
        this.variantStatsId = variantStatsId;
        this.variantStatsDescription = variantStatsDescription;
        this.variantStatsQuery = variantStatsQuery;
        this.signatureId = signatureId;
        this.signatureDescription = signatureDescription;
        this.signatureQuery = signatureQuery;
        this.signatureRelease = signatureRelease;
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
        sb.append(", signatureQuery=").append(signatureQuery);
        sb.append(", signatureRelease=").append(signatureRelease);
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

    public ObjectMap getSignatureQuery() {
        return signatureQuery;
    }

    public SampleQcAnalysisParams setSignatureQuery(ObjectMap signatureQuery) {
        this.signatureQuery = signatureQuery;
        return this;
    }

    public String getSignatureRelease() {
        return signatureRelease;
    }

    public SampleQcAnalysisParams setSignatureRelease(String signatureRelease) {
        this.signatureRelease = signatureRelease;
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
