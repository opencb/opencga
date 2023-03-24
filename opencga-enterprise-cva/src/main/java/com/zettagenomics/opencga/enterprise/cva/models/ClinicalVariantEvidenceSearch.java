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

package com.zettagenomics.opencga.enterprise.cva.models;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClinicalVariantEvidenceSearch {

    @Field("id")
    private String id;

    @Field("cveVariantId")
    private String cveVariantId;

    @Field("cveInterpretationId")
    private String cveInterpretationId;

    @Field("cvePhenotypeNames")
    private List<String> cvePhenotypeNames;

    @Field("cveGeneName")
    private String cveGeneName;

    @Field("cveConsequenceTypeIds")
    private List<String> cveConsequenceTypeIds;

    @Field("cveXrefIds")
    private List<String> cveXrefIds;

    @Field("cvePanelId")
    private String cvePanelId;

    @Field("cveAcmgs")
    private List<String> cveAcmgs;

    @Field("cveTier")
    private String cveTier;

    @Field("cveClinicalSignificance")
    private String cveClinicalSignificance;

    @Field("cveDrugResponse")
    private String cveDrugResponse;

    @Field("cveTraitAssociation")
    private String cveTraitAssociation;

    @Field("cveFunctionalEffect")
    private String cveFunctionalEffect;

    @Field("cveTumorigenesis")
    private String cveTumorigenesis;

    @Field("cveOtherClassifications")
    private List<String> cveOtherClassifications;

    @Field("cveRolesInCancer")
    private List<String> cveRolesInCancer;

    @Field("cveScore_*")
    private Map<String, Double> cveScores;

    // Clinical variant evidence stored in a JSON string
    @Field("cveJson")
    private String cveJson;

    public ClinicalVariantEvidenceSearch() {
        init();
    }

    private void init() {
        cvePhenotypeNames = new ArrayList<>();
        cveConsequenceTypeIds = new ArrayList<>();
        cveXrefIds = new ArrayList<>();
        cveAcmgs = new ArrayList<>();
        cveOtherClassifications = new ArrayList<>();
        cveRolesInCancer = new ArrayList<>();
        cveScores = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalVariantEvidenceSearch{");
        sb.append("id='").append(id).append('\'');
        sb.append(", cveVariantId='").append(cveVariantId).append('\'');
        sb.append(", cveInterpretationId='").append(cveInterpretationId).append('\'');
        sb.append(", cvePhenotypeNames=").append(cvePhenotypeNames);
        sb.append(", cveGeneName='").append(cveGeneName).append('\'');
        sb.append(", cveConsequenceTypeIds=").append(cveConsequenceTypeIds);
        sb.append(", cveXrefIds=").append(cveXrefIds);
        sb.append(", cvePanelId='").append(cvePanelId).append('\'');
        sb.append(", cveAcmgs=").append(cveAcmgs);
        sb.append(", cveTier='").append(cveTier).append('\'');
        sb.append(", cveClinicalSignificance='").append(cveClinicalSignificance).append('\'');
        sb.append(", cveDrugResponse='").append(cveDrugResponse).append('\'');
        sb.append(", cveTraitAssociation='").append(cveTraitAssociation).append('\'');
        sb.append(", cveFunctionalEffect='").append(cveFunctionalEffect).append('\'');
        sb.append(", cveTumorigenesis='").append(cveTumorigenesis).append('\'');
        sb.append(", cveOtherClassifications=").append(cveOtherClassifications);
        sb.append(", cveRolesInCancer=").append(cveRolesInCancer);
        sb.append(", cveScores=").append(cveScores);
        sb.append(", cveJson='").append(cveJson).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalVariantEvidenceSearch setId(String id) {
        this.id = id;
        return this;
    }

    public String getCveVariantId() {
        return cveVariantId;
    }

    public ClinicalVariantEvidenceSearch setCveVariantId(String cveVariantId) {
        this.cveVariantId = cveVariantId;
        return this;
    }

    public String getCveInterpretationId() {
        return cveInterpretationId;
    }

    public ClinicalVariantEvidenceSearch setCveInterpretationId(String cveInterpretationId) {
        this.cveInterpretationId = cveInterpretationId;
        return this;
    }

    public List<String> getCvePhenotypeNames() {
        return cvePhenotypeNames;
    }

    public ClinicalVariantEvidenceSearch setCvePhenotypeNames(List<String> cvePhenotypeNames) {
        this.cvePhenotypeNames = cvePhenotypeNames;
        return this;
    }

    public String getCveGeneName() {
        return cveGeneName;
    }

    public ClinicalVariantEvidenceSearch setCveGeneName(String cveGeneName) {
        this.cveGeneName = cveGeneName;
        return this;
    }

    public List<String> getCveConsequenceTypeIds() {
        return cveConsequenceTypeIds;
    }

    public ClinicalVariantEvidenceSearch setCveConsequenceTypeIds(List<String> cveConsequenceTypeIds) {
        this.cveConsequenceTypeIds = cveConsequenceTypeIds;
        return this;
    }

    public List<String> getCveXrefIds() {
        return cveXrefIds;
    }

    public ClinicalVariantEvidenceSearch setCveXrefIds(List<String> cveXrefIds) {
        this.cveXrefIds = cveXrefIds;
        return this;
    }

    public String getCvePanelId() {
        return cvePanelId;
    }

    public ClinicalVariantEvidenceSearch setCvePanelId(String cvePanelId) {
        this.cvePanelId = cvePanelId;
        return this;
    }

    public List<String> getCveAcmgs() {
        return cveAcmgs;
    }

    public ClinicalVariantEvidenceSearch setCveAcmgs(List<String> cveAcmgs) {
        this.cveAcmgs = cveAcmgs;
        return this;
    }

    public String getCveTier() {
        return cveTier;
    }

    public ClinicalVariantEvidenceSearch setCveTier(String cveTier) {
        this.cveTier = cveTier;
        return this;
    }

    public String getCveClinicalSignificance() {
        return cveClinicalSignificance;
    }

    public ClinicalVariantEvidenceSearch setCveClinicalSignificance(String cveClinicalSignificance) {
        this.cveClinicalSignificance = cveClinicalSignificance;
        return this;
    }

    public String getCveDrugResponse() {
        return cveDrugResponse;
    }

    public ClinicalVariantEvidenceSearch setCveDrugResponse(String cveDrugResponse) {
        this.cveDrugResponse = cveDrugResponse;
        return this;
    }

    public String getCveTraitAssociation() {
        return cveTraitAssociation;
    }

    public ClinicalVariantEvidenceSearch setCveTraitAssociation(String cveTraitAssociation) {
        this.cveTraitAssociation = cveTraitAssociation;
        return this;
    }

    public String getCveFunctionalEffect() {
        return cveFunctionalEffect;
    }

    public ClinicalVariantEvidenceSearch setCveFunctionalEffect(String cveFunctionalEffect) {
        this.cveFunctionalEffect = cveFunctionalEffect;
        return this;
    }

    public String getCveTumorigenesis() {
        return cveTumorigenesis;
    }

    public ClinicalVariantEvidenceSearch setCveTumorigenesis(String cveTumorigenesis) {
        this.cveTumorigenesis = cveTumorigenesis;
        return this;
    }

    public List<String> getCveOtherClassifications() {
        return cveOtherClassifications;
    }

    public ClinicalVariantEvidenceSearch setCveOtherClassifications(List<String> cveOtherClassifications) {
        this.cveOtherClassifications = cveOtherClassifications;
        return this;
    }

    public List<String> getCveRolesInCancer() {
        return cveRolesInCancer;
    }

    public ClinicalVariantEvidenceSearch setCveRolesInCancer(List<String> cveRolesInCancer) {
        this.cveRolesInCancer = cveRolesInCancer;
        return this;
    }

    public Map<String, Double> getCveScores() {
        return cveScores;
    }

    public ClinicalVariantEvidenceSearch setCveScores(Map<String, Double> cveScores) {
        this.cveScores = cveScores;
        return this;
    }

    public String getCveJson() {
        return cveJson;
    }

    public ClinicalVariantEvidenceSearch setCveJson(String cveJson) {
        this.cveJson = cveJson;
        return this;
    }
}


