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

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.tools.ToolParams;

/**
 * Basic set of VariantQueryParams, containing only the most used ones.
 */
public class BasicVariantQueryParams extends ToolParams {
    private String id;
    private String region;
    private String gene;
    private String type;
    private String project;
    private String study;
    private String panel;
    private String cohortStatsRef;
    private String cohortStatsAlt;
    private String cohortStatsMaf;
    private String ct;
    private String xref;
    private String biotype;
    private String proteinSubstitution;
    private String conservation;
    private String populationFrequencyMaf;
    private String populationFrequencyAlt;
    private String populationFrequencyRef;
    private String transcriptFlag;
    private String functionalScore;
    private String clinicalSignificance;

    public Query toQuery() {
        return new Query(toObjectMap());
    }

    public String getId() {
        return id;
    }

    public BasicVariantQueryParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public BasicVariantQueryParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public BasicVariantQueryParams setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getType() {
        return type;
    }

    public BasicVariantQueryParams setType(String type) {
        this.type = type;
        return this;
    }

    public String getProject() {
        return project;
    }

    public BasicVariantQueryParams setProject(String project) {
        this.project = project;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public BasicVariantQueryParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getPanel() {
        return panel;
    }

    public BasicVariantQueryParams setPanel(String panel) {
        this.panel = panel;
        return this;
    }

    public String getCohortStatsRef() {
        return cohortStatsRef;
    }

    public BasicVariantQueryParams setCohortStatsRef(String cohortStatsRef) {
        this.cohortStatsRef = cohortStatsRef;
        return this;
    }

    public String getCohortStatsAlt() {
        return cohortStatsAlt;
    }

    public BasicVariantQueryParams setCohortStatsAlt(String cohortStatsAlt) {
        this.cohortStatsAlt = cohortStatsAlt;
        return this;
    }

    public String getCohortStatsMaf() {
        return cohortStatsMaf;
    }

    public BasicVariantQueryParams setCohortStatsMaf(String cohortStatsMaf) {
        this.cohortStatsMaf = cohortStatsMaf;
        return this;
    }

    public String getCt() {
        return ct;
    }

    public BasicVariantQueryParams setCt(String ct) {
        this.ct = ct;
        return this;
    }

    public String getXref() {
        return xref;
    }

    public BasicVariantQueryParams setXref(String xref) {
        this.xref = xref;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public BasicVariantQueryParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getProteinSubstitution() {
        return proteinSubstitution;
    }

    public BasicVariantQueryParams setProteinSubstitution(String proteinSubstitution) {
        this.proteinSubstitution = proteinSubstitution;
        return this;
    }

    public String getConservation() {
        return conservation;
    }

    public BasicVariantQueryParams setConservation(String conservation) {
        this.conservation = conservation;
        return this;
    }

    public String getPopulationFrequencyMaf() {
        return populationFrequencyMaf;
    }

    public BasicVariantQueryParams setPopulationFrequencyMaf(String populationFrequencyMaf) {
        this.populationFrequencyMaf = populationFrequencyMaf;
        return this;
    }

    public String getPopulationFrequencyAlt() {
        return populationFrequencyAlt;
    }

    public BasicVariantQueryParams setPopulationFrequencyAlt(String populationFrequencyAlt) {
        this.populationFrequencyAlt = populationFrequencyAlt;
        return this;
    }

    public String getPopulationFrequencyRef() {
        return populationFrequencyRef;
    }

    public BasicVariantQueryParams setPopulationFrequencyRef(String populationFrequencyRef) {
        this.populationFrequencyRef = populationFrequencyRef;
        return this;
    }

    public String getTranscriptFlag() {
        return transcriptFlag;
    }

    public BasicVariantQueryParams setTranscriptFlag(String transcriptFlag) {
        this.transcriptFlag = transcriptFlag;
        return this;
    }

    public String getFunctionalScore() {
        return functionalScore;
    }

    public BasicVariantQueryParams setFunctionalScore(String functionalScore) {
        this.functionalScore = functionalScore;
        return this;
    }

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public BasicVariantQueryParams setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }
}
