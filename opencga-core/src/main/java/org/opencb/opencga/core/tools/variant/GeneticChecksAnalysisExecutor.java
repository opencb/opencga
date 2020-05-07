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

package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.variant.GeneticChecksReport;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class GeneticChecksAnalysisExecutor extends OpenCgaToolExecutor {

    public enum GeneticCheck {
        INFERRED_SEX, RELATEDNESS, MENDELIAN_ERRORS
    }

    private String studyId;
    private String familyId;
    private List<String> sampleIds;
    private GeneticCheck geneticCheck;
    private String minorAlleleFreq;
    private String relatednessMethod;

    private GeneticChecksReport report;

    public GeneticChecksAnalysisExecutor() {
        report = new GeneticChecksReport();
    }

    public String getStudyId() {
        return studyId;
    }

    public GeneticChecksAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public GeneticChecksAnalysisExecutor setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public GeneticChecksAnalysisExecutor setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public GeneticCheck getGeneticCheck() {
        return geneticCheck;
    }

    public GeneticChecksAnalysisExecutor setGeneticCheck(GeneticCheck geneticCheck) {
        this.geneticCheck = geneticCheck;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public GeneticChecksAnalysisExecutor setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public GeneticChecksAnalysisExecutor setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public GeneticChecksReport getReport() {
        return report;
    }

    public GeneticChecksAnalysisExecutor setReport(GeneticChecksReport report) {
        this.report = report;
        return this;
    }
}
