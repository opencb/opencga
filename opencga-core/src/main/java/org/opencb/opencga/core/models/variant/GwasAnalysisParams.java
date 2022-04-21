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
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import java.util.List;

public class GwasAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Gwas analysis params";
    private String phenotype;
    private Boolean index;
    private String indexScoreId;
    private GwasConfiguration.Method method;
    private GwasConfiguration.FisherMode fisherMode;
    private String caseCohort;
    private String caseCohortSamplesAnnotation;
    private List<String> caseCohortSamples;
    private String controlCohort;
    private String controlCohortSamplesAnnotation;
    private List<String> controlCohortSamples;
    private String outdir;

    public GwasAnalysisParams() {
    }

    public GwasAnalysisParams(String phenotype, boolean index, String indexScoreId, GwasConfiguration.Method method,
                              GwasConfiguration.FisherMode fisherMode,
                              String caseCohort, String caseCohortSamplesAnnotation, List<String> caseCohortSamples,
                              String controlCohort, String controlCohortSamplesAnnotation, List<String> controlCohortSamples,
                              String outdir) {
        this.phenotype = phenotype;
        this.index = index;
        this.indexScoreId = indexScoreId;
        this.method = method;
        this.fisherMode = fisherMode;
        this.caseCohort = caseCohort;
        this.caseCohortSamplesAnnotation = caseCohortSamplesAnnotation;
        this.caseCohortSamples = caseCohortSamples;
        this.controlCohort = controlCohort;
        this.controlCohortSamplesAnnotation = controlCohortSamplesAnnotation;
        this.controlCohortSamples = controlCohortSamples;
        this.outdir = outdir;
    }

    public String getPhenotype() {
        return phenotype;
    }

    public GwasAnalysisParams setPhenotype(String phenotype) {
        this.phenotype = phenotype;
        return this;
    }

    public Boolean isIndex() {
        return index;
    }

    public GwasAnalysisParams setIndex(Boolean index) {
        this.index = index;
        return this;
    }

    public String getIndexScoreId() {
        return indexScoreId;
    }

    public GwasAnalysisParams setIndexScoreId(String indexScoreId) {
        this.indexScoreId = indexScoreId;
        return this;
    }

    public GwasConfiguration.Method getMethod() {
        return method;
    }

    public GwasAnalysisParams setMethod(GwasConfiguration.Method method) {
        this.method = method;
        return this;
    }

    public GwasConfiguration.FisherMode getFisherMode() {
        return fisherMode;
    }

    public GwasAnalysisParams setFisherMode(GwasConfiguration.FisherMode fisherMode) {
        this.fisherMode = fisherMode;
        return this;
    }

    public String getCaseCohort() {
        return caseCohort;
    }

    public GwasAnalysisParams setCaseCohort(String caseCohort) {
        this.caseCohort = caseCohort;
        return this;
    }

    public String getCaseCohortSamplesAnnotation() {
        return caseCohortSamplesAnnotation;
    }

    public GwasAnalysisParams setCaseCohortSamplesAnnotation(String caseCohortSamplesAnnotation) {
        this.caseCohortSamplesAnnotation = caseCohortSamplesAnnotation;
        return this;
    }

    public List<String> getCaseCohortSamples() {
        return caseCohortSamples;
    }

    public GwasAnalysisParams setCaseCohortSamples(List<String> caseCohortSamples) {
        this.caseCohortSamples = caseCohortSamples;
        return this;
    }

    public String getControlCohort() {
        return controlCohort;
    }

    public GwasAnalysisParams setControlCohort(String controlCohort) {
        this.controlCohort = controlCohort;
        return this;
    }

    public String getControlCohortSamplesAnnotation() {
        return controlCohortSamplesAnnotation;
    }

    public GwasAnalysisParams setControlCohortSamplesAnnotation(String controlCohortSamplesAnnotation) {
        this.controlCohortSamplesAnnotation = controlCohortSamplesAnnotation;
        return this;
    }

    public List<String> getControlCohortSamples() {
        return controlCohortSamples;
    }

    public GwasAnalysisParams setControlCohortSamples(List<String> controlCohortSamples) {
        this.controlCohortSamples = controlCohortSamples;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public GwasAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
