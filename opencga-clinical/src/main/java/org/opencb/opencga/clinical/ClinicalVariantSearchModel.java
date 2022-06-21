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

package org.opencb.opencga.clinical;

import org.apache.solr.client.solrj.beans.Field;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClinicalVariantSearchModel extends VariantSearchModel {

    // ---------- ClinicalAnalysis ----------

    @Field("caId")
    private String caId;

    @Field("caDescription")
    private String caDescription;

    @Field("caDisorder")
    private String caDisorder;

    @Field("caFiles")
    private List<String> caFiles;

    @Field("caProbandId")
    private String caProbandId;

    @Field("caFamilyId")
    private String caFamilyId;

    @Field("caFamilyPhenotypeNames")
    private List<String> caFamilyPhenotypeNames;

    @Field("caFamilyMemberIds")
    private List<String> caFamilyMemberIds;

    // ---------- Interpretation----------

    // intBasicJson a JSON string containing all interpretation fields but the reported variant list
    @Field("intBasicJson")
    private String intBasicJson;

    @Field("intId")
    private String intId;

    @Field("intClinicalAnalysisId")
    private String intClinicalAnalysisId;

    @Field("intMethodName")
    private String intMethodName;

    @Field("intMethodVersion")
    private String intMethodVersion;

    @Field("intAnalystName")
    private String intAnalystName;

    @Field("intPanelNames")
    private List<String> intPanelNames;

    // intInfo contains the following interpretation info (by lines):
    // descriptions (DS), analyst (AN), dependencies (DP), filters (FT), comments (CM) and attributes (AT)
    @Field("intInfo")
    private List<String> intInfo;

    @Field("intCreationDate")
    private String intCreationDate;

    // ---------- Catalog attributes ----------

    @Field("projectId")
    private String projectId;

    @Field("assembly")
    private String assembly;

    @Field("studyId")
    private String studyId;

    // JSON containing the array of Study and all their fields
    @Field("studyJson")
    private String studyJson;

    // ---------- ClinicalVariant ----------

    @Field("cvPrimary")
    private boolean cvPrimary;

    @Field("cvDeNovoQualityScore")
    private Double cvDeNovoQualityScore;

    @Field("cvComments")
    private List<String> cvComments;

    // A JSON string containing all clinical variant evidences
    @Field("cvClinicalVariantEvidencesJson")
    private String cvClinicalVariantEvidencesJson;

    // A JSON string containing all attributes
    @Field("cvAttributesJson")
    private String cvAttributesJson;

    // ---------- ClinicalVariantEvidence----------

    @Field("cvePhenotypes")
    private List<String> cvePhenotypes;

    @Field("cveConsequenceTypeIds")
    private List<String> cveConsequenceTypeIds;

    @Field("cveGeneNames")
    private List<String> cveGeneNames;

    @Field("cveXrefs")
    private List<String> cveXrefs;

    @Field("cvePanelNames")
    private List<String> cvePanelNames;

    @Field("cveAcmg")
    private List<String> cveAcmg;

    @Field("cveClinicalSignificance")
    private List<String> cveClinicalSignificance;

    @Field("cveDrugResponse")
    private List<String> cveDrugResponse;

    @Field("cveTraitAssociation")
    private List<String> cveTraitAssociation;

    @Field("cveFunctionalEffect")
    private List<String> cveFunctionalEffect;

    @Field("cveTumorigenesis")
    private List<String> cveTumorigenesis;

    @Field("cveOtherClassification")
    private List<String> cveOtherClassification;

    @Field("cveRolesInCancer")
    private List<String> cveRolesInCancer;

    @Field("cveScore_*")
    private Map<String, Double> cveScores;

    public ClinicalVariantSearchModel() {
        // ---------- ClinicalAnalysis ----------
        caFiles = new ArrayList<>();
        caFamilyPhenotypeNames = new ArrayList<>();
        caFamilyMemberIds = new ArrayList<>();

        // ---------- Interpretation----------
        intPanelNames = new ArrayList<>();
        intInfo = new ArrayList<>();

        // ---------- ReportedVariant ----------
        cvComments = new ArrayList<>();

        // ---------- ReportedEvent----------
        cvePhenotypes = new ArrayList<>();
        cveConsequenceTypeIds = new ArrayList<>();
        cveGeneNames = new ArrayList<>();
        cveXrefs = new ArrayList<>();
        cvePanelNames = new ArrayList<>();
        cveAcmg = new ArrayList<>();
        cveClinicalSignificance = new ArrayList<>();
        cveDrugResponse = new ArrayList<>();
        cveTraitAssociation = new ArrayList<>();
        cveFunctionalEffect = new ArrayList<>();
        cveTumorigenesis = new ArrayList<>();
        cveOtherClassification = new ArrayList<>();
        cveRolesInCancer = new ArrayList<>();
        cveScores = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalVariantSearchModel{");
        sb.append("caId='").append(caId).append('\'');
        sb.append(", caDescription='").append(caDescription).append('\'');
        sb.append(", caDisorder='").append(caDisorder).append('\'');
        sb.append(", caFiles=").append(caFiles);
        sb.append(", caProbandId='").append(caProbandId).append('\'');
        sb.append(", caFamilyId='").append(caFamilyId).append('\'');
        sb.append(", caFamilyPhenotypeNames=").append(caFamilyPhenotypeNames);
        sb.append(", caFamilyMemberIds=").append(caFamilyMemberIds);
        sb.append(", intBasicJson='").append(intBasicJson).append('\'');
        sb.append(", intId='").append(intId).append('\'');
        sb.append(", intClinicalAnalysisId='").append(intClinicalAnalysisId).append('\'');
        sb.append(", intMethodName='").append(intMethodName).append('\'');
        sb.append(", intMethodVersion='").append(intMethodVersion).append('\'');
        sb.append(", intAnalystName='").append(intAnalystName).append('\'');
        sb.append(", intPanelNames=").append(intPanelNames);
        sb.append(", intInfo=").append(intInfo);
        sb.append(", intCreationDate='").append(intCreationDate).append('\'');
        sb.append(", projectId='").append(projectId).append('\'');
        sb.append(", assembly='").append(assembly).append('\'');
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", studyJson='").append(studyJson).append('\'');
        sb.append(", cvPrimary=").append(cvPrimary);
        sb.append(", cvDeNovoQualityScore=").append(cvDeNovoQualityScore);
        sb.append(", cvComments=").append(cvComments);
        sb.append(", cvClinicalVariantEvidencesJson='").append(cvClinicalVariantEvidencesJson).append('\'');
        sb.append(", cvAttributesJson='").append(cvAttributesJson).append('\'');
        sb.append(", cvePhenotypes=").append(cvePhenotypes);
        sb.append(", cveConsequenceTypeIds=").append(cveConsequenceTypeIds);
        sb.append(", cveGeneNames=").append(cveGeneNames);
        sb.append(", cveXrefs=").append(cveXrefs);
        sb.append(", cvePanelNames=").append(cvePanelNames);
        sb.append(", cveAcmg=").append(cveAcmg);
        sb.append(", cveClinicalSignificance=").append(cveClinicalSignificance);
        sb.append(", cveDrugResponse=").append(cveDrugResponse);
        sb.append(", cveTraitAssociation=").append(cveTraitAssociation);
        sb.append(", cveFunctionalEffect=").append(cveFunctionalEffect);
        sb.append(", cveTumorigenesis=").append(cveTumorigenesis);
        sb.append(", cveOtherClassification=").append(cveOtherClassification);
        sb.append(", cveRolesInCancer=").append(cveRolesInCancer);
        sb.append(", cveScores=").append(cveScores);
        sb.append('}');
        return sb.toString();
    }

    public String getCaId() {
        return caId;
    }

    public ClinicalVariantSearchModel setCaId(String caId) {
        this.caId = caId;
        return this;
    }

    public String getCaDescription() {
        return caDescription;
    }

    public ClinicalVariantSearchModel setCaDescription(String caDescription) {
        this.caDescription = caDescription;
        return this;
    }

    public String getCaDisorder() {
        return caDisorder;
    }

    public ClinicalVariantSearchModel setCaDisorder(String caDisorder) {
        this.caDisorder = caDisorder;
        return this;
    }

    public List<String> getCaFiles() {
        return caFiles;
    }

    public ClinicalVariantSearchModel setCaFiles(List<String> caFiles) {
        this.caFiles = caFiles;
        return this;
    }

    public String getCaProbandId() {
        return caProbandId;
    }

    public ClinicalVariantSearchModel setCaProbandId(String caProbandId) {
        this.caProbandId = caProbandId;
        return this;
    }

    public String getCaFamilyId() {
        return caFamilyId;
    }

    public ClinicalVariantSearchModel setCaFamilyId(String caFamilyId) {
        this.caFamilyId = caFamilyId;
        return this;
    }

    public List<String> getCaFamilyPhenotypeNames() {
        return caFamilyPhenotypeNames;
    }

    public ClinicalVariantSearchModel setCaFamilyPhenotypeNames(List<String> caFamilyPhenotypeNames) {
        this.caFamilyPhenotypeNames = caFamilyPhenotypeNames;
        return this;
    }

    public List<String> getCaFamilyMemberIds() {
        return caFamilyMemberIds;
    }

    public ClinicalVariantSearchModel setCaFamilyMemberIds(List<String> caFamilyMemberIds) {
        this.caFamilyMemberIds = caFamilyMemberIds;
        return this;
    }

    public String getIntBasicJson() {
        return intBasicJson;
    }

    public ClinicalVariantSearchModel setIntBasicJson(String intBasicJson) {
        this.intBasicJson = intBasicJson;
        return this;
    }

    public String getIntId() {
        return intId;
    }

    public ClinicalVariantSearchModel setIntId(String intId) {
        this.intId = intId;
        return this;
    }

    public String getIntClinicalAnalysisId() {
        return intClinicalAnalysisId;
    }

    public ClinicalVariantSearchModel setIntClinicalAnalysisId(String intClinicalAnalysisId) {
        this.intClinicalAnalysisId = intClinicalAnalysisId;
        return this;
    }

    public String getIntMethodName() {
        return intMethodName;
    }

    public ClinicalVariantSearchModel setIntMethodName(String intMethodName) {
        this.intMethodName = intMethodName;
        return this;
    }

    public String getIntMethodVersion() {
        return intMethodVersion;
    }

    public ClinicalVariantSearchModel setIntMethodVersion(String intMethodVersion) {
        this.intMethodVersion = intMethodVersion;
        return this;
    }

    public String getIntAnalystName() {
        return intAnalystName;
    }

    public ClinicalVariantSearchModel setIntAnalystName(String intAnalystName) {
        this.intAnalystName = intAnalystName;
        return this;
    }

    public List<String> getIntPanelNames() {
        return intPanelNames;
    }

    public ClinicalVariantSearchModel setIntPanelNames(List<String> intPanelNames) {
        this.intPanelNames = intPanelNames;
        return this;
    }

    public List<String> getIntInfo() {
        return intInfo;
    }

    public ClinicalVariantSearchModel setIntInfo(List<String> intInfo) {
        this.intInfo = intInfo;
        return this;
    }

    public String getIntCreationDate() {
        return intCreationDate;
    }

    public ClinicalVariantSearchModel setIntCreationDate(String intCreationDate) {
        this.intCreationDate = intCreationDate;
        return this;
    }

    public String getProjectId() {
        return projectId;
    }

    public ClinicalVariantSearchModel setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public ClinicalVariantSearchModel setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public ClinicalVariantSearchModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getStudyJson() {
        return studyJson;
    }

    public ClinicalVariantSearchModel setStudyJson(String studyJson) {
        this.studyJson = studyJson;
        return this;
    }

    public boolean isCvPrimary() {
        return cvPrimary;
    }

    public ClinicalVariantSearchModel setCvPrimary(boolean cvPrimary) {
        this.cvPrimary = cvPrimary;
        return this;
    }

    public Double getCvDeNovoQualityScore() {
        return cvDeNovoQualityScore;
    }

    public ClinicalVariantSearchModel setCvDeNovoQualityScore(Double cvDeNovoQualityScore) {
        this.cvDeNovoQualityScore = cvDeNovoQualityScore;
        return this;
    }

    public List<String> getCvComments() {
        return cvComments;
    }

    public ClinicalVariantSearchModel setCvComments(List<String> cvComments) {
        this.cvComments = cvComments;
        return this;
    }

    public String getCvClinicalVariantEvidencesJson() {
        return cvClinicalVariantEvidencesJson;
    }

    public ClinicalVariantSearchModel setCvClinicalVariantEvidencesJson(String cvClinicalVariantEvidencesJson) {
        this.cvClinicalVariantEvidencesJson = cvClinicalVariantEvidencesJson;
        return this;
    }

    public String getCvAttributesJson() {
        return cvAttributesJson;
    }

    public ClinicalVariantSearchModel setCvAttributesJson(String cvAttributesJson) {
        this.cvAttributesJson = cvAttributesJson;
        return this;
    }

    public List<String> getCvePhenotypes() {
        return cvePhenotypes;
    }

    public ClinicalVariantSearchModel setCvePhenotypes(List<String> cvePhenotypes) {
        this.cvePhenotypes = cvePhenotypes;
        return this;
    }

    public List<String> getCveConsequenceTypeIds() {
        return cveConsequenceTypeIds;
    }

    public ClinicalVariantSearchModel setCveConsequenceTypeIds(List<String> cveConsequenceTypeIds) {
        this.cveConsequenceTypeIds = cveConsequenceTypeIds;
        return this;
    }

    public List<String> getCveGeneNames() {
        return cveGeneNames;
    }

    public ClinicalVariantSearchModel setCveGeneNames(List<String> cveGeneNames) {
        this.cveGeneNames = cveGeneNames;
        return this;
    }

    public List<String> getCveXrefs() {
        return cveXrefs;
    }

    public ClinicalVariantSearchModel setCveXrefs(List<String> cveXrefs) {
        this.cveXrefs = cveXrefs;
        return this;
    }

    public List<String> getCvePanelNames() {
        return cvePanelNames;
    }

    public ClinicalVariantSearchModel setCvePanelNames(List<String> cvePanelNames) {
        this.cvePanelNames = cvePanelNames;
        return this;
    }

    public List<String> getCveAcmg() {
        return cveAcmg;
    }

    public ClinicalVariantSearchModel setCveAcmg(List<String> cveAcmg) {
        this.cveAcmg = cveAcmg;
        return this;
    }

    public List<String> getCveClinicalSignificance() {
        return cveClinicalSignificance;
    }

    public ClinicalVariantSearchModel setCveClinicalSignificance(List<String> cveClinicalSignificance) {
        this.cveClinicalSignificance = cveClinicalSignificance;
        return this;
    }

    public List<String> getCveDrugResponse() {
        return cveDrugResponse;
    }

    public ClinicalVariantSearchModel setCveDrugResponse(List<String> cveDrugResponse) {
        this.cveDrugResponse = cveDrugResponse;
        return this;
    }

    public List<String> getCveTraitAssociation() {
        return cveTraitAssociation;
    }

    public ClinicalVariantSearchModel setCveTraitAssociation(List<String> cveTraitAssociation) {
        this.cveTraitAssociation = cveTraitAssociation;
        return this;
    }

    public List<String> getCveFunctionalEffect() {
        return cveFunctionalEffect;
    }

    public ClinicalVariantSearchModel setCveFunctionalEffect(List<String> cveFunctionalEffect) {
        this.cveFunctionalEffect = cveFunctionalEffect;
        return this;
    }

    public List<String> getCveTumorigenesis() {
        return cveTumorigenesis;
    }

    public ClinicalVariantSearchModel setCveTumorigenesis(List<String> cveTumorigenesis) {
        this.cveTumorigenesis = cveTumorigenesis;
        return this;
    }

    public List<String> getCveOtherClassification() {
        return cveOtherClassification;
    }

    public ClinicalVariantSearchModel setCveOtherClassification(List<String> cveOtherClassification) {
        this.cveOtherClassification = cveOtherClassification;
        return this;
    }

    public List<String> getCveRolesInCancer() {
        return cveRolesInCancer;
    }

    public ClinicalVariantSearchModel setCveRolesInCancer(List<String> cveRolesInCancer) {
        this.cveRolesInCancer = cveRolesInCancer;
        return this;
    }

    public Map<String, Double> getCveScores() {
        return cveScores;
    }

    public ClinicalVariantSearchModel setCveScores(Map<String, Double> cveScores) {
        this.cveScores = cveScores;
        return this;
    }
}


