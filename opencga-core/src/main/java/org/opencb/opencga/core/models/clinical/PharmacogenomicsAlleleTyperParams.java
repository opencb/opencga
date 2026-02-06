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

/**
 * Parameters for pharmacogenomics allele typing endpoint.
 */
public class PharmacogenomicsAlleleTyperParams {

    @JsonProperty("genotypingContent")
    private String genotypingContent;

    @JsonProperty("translationContent")
    private String translationContent;

    public PharmacogenomicsAlleleTyperParams() {
    }

    public PharmacogenomicsAlleleTyperParams(String genotypingContent, String translationContent) {
        this.genotypingContent = genotypingContent;
        this.translationContent = translationContent;
    }

    public String getGenotypingContent() {
        return genotypingContent;
    }

    public PharmacogenomicsAlleleTyperParams setGenotypingContent(String genotypingContent) {
        this.genotypingContent = genotypingContent;
        return this;
    }

    public String getTranslationContent() {
        return translationContent;
    }

    public PharmacogenomicsAlleleTyperParams setTranslationContent(String translationContent) {
        this.translationContent = translationContent;
        return this;
    }

    @Override
    public String toString() {
        return "PharmacogenomicsAlleleTyperParams{" +
                "genotypingContent=" + (genotypingContent != null ? genotypingContent.length() + " chars" : "null") +
                ", translationContent=" + (translationContent != null ? translationContent.length() + " chars" : "null") +
                '}';
    }
}
