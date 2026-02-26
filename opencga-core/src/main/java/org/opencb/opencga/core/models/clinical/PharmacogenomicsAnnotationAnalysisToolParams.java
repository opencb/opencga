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

package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

/**
 * Parameters for pharmacogenomics annotation analysis.
 */
public class PharmacogenomicsAnnotationAnalysisToolParams extends ToolParams {

    @JsonProperty("alleleTyperContent")
    private String alleleTyperContent;

    @JsonProperty("outdir")
    private String outdir;

    public PharmacogenomicsAnnotationAnalysisToolParams() {
    }

    public PharmacogenomicsAnnotationAnalysisToolParams(String alleleTyperContent, String outdir) {
        this.alleleTyperContent = alleleTyperContent;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PharmacogenomicsAnnotationAnalysisToolParams{");
        sb.append("alleleTyperContent='").append(alleleTyperContent).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAlleleTyperContent() {
        return alleleTyperContent;
    }

    public PharmacogenomicsAnnotationAnalysisToolParams setAlleleTyperContent(String alleleTyperContent) {
        this.alleleTyperContent = alleleTyperContent;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public PharmacogenomicsAnnotationAnalysisToolParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
