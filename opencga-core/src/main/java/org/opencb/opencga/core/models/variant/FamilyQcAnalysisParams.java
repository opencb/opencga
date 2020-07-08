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

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;
import java.util.Map;

public class FamilyQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Family QC analysis params. Family ID. Relatedness method, by default 'PLINK/IBD'. Minor "
            + " allele frequence (MAF) is used to filter variants before computing relatedness, e.g.: 1kg_phase3:CEU>0.35 or cohort:ALL>0.05";
    private String family;
    private String relatednessMethod;
    private String relatednessMaf;

    private String outdir;

    public FamilyQcAnalysisParams() {
    }

    public FamilyQcAnalysisParams(String family, String relatednessMethod, String relatednessMaf, String outdir) {
        this.family = family;
        this.relatednessMethod = relatednessMethod;
        this.relatednessMaf = relatednessMaf;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQcAnalysisParams{");
        sb.append("family='").append(family).append('\'');
        sb.append(", relatednessMethod='").append(relatednessMethod).append('\'');
        sb.append(", relatednessMaf='").append(relatednessMaf).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFamily() {
        return family;
    }

    public FamilyQcAnalysisParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public FamilyQcAnalysisParams setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public String getRelatednessMaf() {
        return relatednessMaf;
    }

    public FamilyQcAnalysisParams setRelatednessMaf(String relatednessMaf) {
        this.relatednessMaf = relatednessMaf;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public FamilyQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
