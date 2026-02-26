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
 * Parameters for pharmacogenomics allele typing endpoint.
 */
public class PharmacogenomicsAlleleTyperToolParams extends ToolParams {

    @JsonProperty("genotypingContent")
    private String genotypingContent;

    @JsonProperty("translationContent")
    private String translationContent;

    @JsonProperty("annotate")
    private Boolean annotate;

    @JsonProperty("outdir")
    private String outdir;

    public PharmacogenomicsAlleleTyperToolParams() {
    }

    public PharmacogenomicsAlleleTyperToolParams(String genotypingContent, String translationContent, Boolean annotate, String outdir) {
        this.genotypingContent = genotypingContent;
        this.translationContent = translationContent;
        this.annotate = annotate;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PharmacogenomicsAlleleTyperToolParams{");
        sb.append("genotypingContent='").append(genotypingContent).append('\'');
        sb.append(", translationContent='").append(translationContent).append('\'');
        sb.append(", annotate=").append(annotate);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getGenotypingContent() {
        return genotypingContent;
    }

    public PharmacogenomicsAlleleTyperToolParams setGenotypingContent(String genotypingContent) {
        this.genotypingContent = genotypingContent;
        return this;
    }

    public String getTranslationContent() {
        return translationContent;
    }

    public PharmacogenomicsAlleleTyperToolParams setTranslationContent(String translationContent) {
        this.translationContent = translationContent;
        return this;
    }

    public Boolean getAnnotate() {
        return annotate;
    }

    public PharmacogenomicsAlleleTyperToolParams setAnnotate(Boolean annotate) {
        this.annotate = annotate;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public PharmacogenomicsAlleleTyperToolParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
