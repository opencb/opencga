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

public class ReportedVariantSearchModel extends VariantSearchModel {

    // ---------- ClinicalAnalysis ----------

    @Field("caName")
    private String caName;

    @Field("caDescription")
    private String caDescription;

    @Field("caDisease")
    private String caDisease;

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

    @Field("intSofwareName")
    private String intSoftwareName;

    @Field("intSofwareVersion")
    private String intSoftwareVersion;

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

    // ---------- ReportedVariant ----------

    @Field("rvDeNovoQualityScore")
    private Double rvDeNovoQualityScore;

    @Field("rvComments")
    private List<String> rvComments;

    // A JSON string containing all reported events
    @Field("rvReportedEventsJson")
    private String rvReportedEventsJson;

    // A JSON string containing all attributes
    @Field("rvAttributesJson")
    private String rvAttributesJson;

    // ---------- ReportedEvent----------

    @Field("rePhenotypes")
    private List<String> rePhenotypes;

    @Field("reConsequenceTypeIds")
    private List<String> reConsequenceTypeIds;

    @Field("reGeneNames")
    private List<String> reGeneNames;

    @Field("reXrefs")
    private List<String> reXrefs;

    @Field("rePanelNames")
    private List<String> rePanelNames;

    @Field("reAcmg")
    private List<String> reAcmg;

    @Field("reClinicalSignificance")
    private List<String> reClinicalSignificance;

    @Field("reDrugResponse")
    private List<String> reDrugResponse;

    @Field("reTraitAssociation")
    private List<String> reTraitAssociation;

    @Field("reFunctionalEffect")
    private List<String> reFunctionalEffect;

    @Field("reTumorigenesis")
    private List<String> reTumorigenesis;

    @Field("reOtherClassification")
    private List<String> reOtherClassification;

    @Field("reRolesInCancer")
    private List<String> reRolesInCancer;

    @Field("reScore_*")
    private Map<String, Double> reScores;

    public ReportedVariantSearchModel() {
        // ---------- ClinicalAnalysis ----------
        caFiles = new ArrayList<>();
        caFamilyPhenotypeNames = new ArrayList<>();
        caFamilyMemberIds = new ArrayList<>();

        // ---------- Interpretation----------
        intPanelNames = new ArrayList<>();
        intInfo = new ArrayList<>();

        // ---------- ReportedVariant ----------
        rvComments = new ArrayList<>();

        // ---------- ReportedEvent----------
        rePhenotypes = new ArrayList<>();
        reConsequenceTypeIds = new ArrayList<>();
        reGeneNames = new ArrayList<>();
        reXrefs = new ArrayList<>();
        rePanelNames = new ArrayList<>();
        reAcmg = new ArrayList<>();
        reClinicalSignificance = new ArrayList<>();
        reDrugResponse = new ArrayList<>();
        reTraitAssociation = new ArrayList<>();
        reFunctionalEffect = new ArrayList<>();
        reTumorigenesis = new ArrayList<>();
        reOtherClassification = new ArrayList<>();
        reRolesInCancer = new ArrayList<>();
        reScores = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ReportedVariantSearchModel{");
        sb.append("caName='").append(caName).append('\'');
        sb.append(", caDescription='").append(caDescription).append('\'');
        sb.append(", caDisease='").append(caDisease).append('\'');
        sb.append(", caFiles=").append(caFiles);
        sb.append(", caProbandId='").append(caProbandId).append('\'');
        sb.append(", caFamilyId='").append(caFamilyId).append('\'');
        sb.append(", caFamilyPhenotypeNames=").append(caFamilyPhenotypeNames);
        sb.append(", caFamilyMemberIds=").append(caFamilyMemberIds);
        sb.append(", intBasicJson='").append(intBasicJson).append('\'');
        sb.append(", intId='").append(intId).append('\'');
        sb.append(", intClinicalAnalysisId='").append(intClinicalAnalysisId).append('\'');
        sb.append(", intSoftwareName='").append(intSoftwareName).append('\'');
        sb.append(", intSoftwareVersion='").append(intSoftwareVersion).append('\'');
        sb.append(", intAnalystName='").append(intAnalystName).append('\'');
        sb.append(", intPanelNames=").append(intPanelNames);
        sb.append(", intInfo=").append(intInfo);
        sb.append(", intCreationDate='").append(intCreationDate).append('\'');
        sb.append(", projectId='").append(projectId).append('\'');
        sb.append(", assembly='").append(assembly).append('\'');
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", studyJson='").append(studyJson).append('\'');
        sb.append(", rvDeNovoQualityScore=").append(rvDeNovoQualityScore);
        sb.append(", rvComments=").append(rvComments);
        sb.append(", rvReportedEventsJson='").append(rvReportedEventsJson).append('\'');
        sb.append(", rvAttributesJson='").append(rvAttributesJson).append('\'');
        sb.append(", rePhenotypes=").append(rePhenotypes);
        sb.append(", reConsequenceTypeIds=").append(reConsequenceTypeIds);
        sb.append(", reGeneNames=").append(reGeneNames);
        sb.append(", reXrefs=").append(reXrefs);
        sb.append(", rePanelNames=").append(rePanelNames);
        sb.append(", reAcmg=").append(reAcmg);
        sb.append(", reClinicalSignificance=").append(reClinicalSignificance);
        sb.append(", reDrugResponse=").append(reDrugResponse);
        sb.append(", reTraitAssociation=").append(reTraitAssociation);
        sb.append(", reFunctionalEffect=").append(reFunctionalEffect);
        sb.append(", reTumorigenesis=").append(reTumorigenesis);
        sb.append(", reOtherClassification=").append(reOtherClassification);
        sb.append(", reRolesInCancer=").append(reRolesInCancer);
        sb.append(", reScores=").append(reScores);
        sb.append('}');
        return sb.toString();
    }

    public String getCaName() {
        return caName;
    }

    public ReportedVariantSearchModel setCaName(String caName) {
        this.caName = caName;
        return this;
    }

    public String getCaDescription() {
        return caDescription;
    }

    public ReportedVariantSearchModel setCaDescription(String caDescription) {
        this.caDescription = caDescription;
        return this;
    }

    public String getCaDisease() {
        return caDisease;
    }

    public ReportedVariantSearchModel setCaDisease(String caDisease) {
        this.caDisease = caDisease;
        return this;
    }

    public List<String> getCaFiles() {
        return caFiles;
    }

    public ReportedVariantSearchModel setCaFiles(List<String> caFiles) {
        this.caFiles = caFiles;
        return this;
    }

    public String getCaProbandId() {
        return caProbandId;
    }

    public ReportedVariantSearchModel setCaProbandId(String caProbandId) {
        this.caProbandId = caProbandId;
        return this;
    }

    public String getCaFamilyId() {
        return caFamilyId;
    }

    public ReportedVariantSearchModel setCaFamilyId(String caFamilyId) {
        this.caFamilyId = caFamilyId;
        return this;
    }

    public List<String> getCaFamilyPhenotypeNames() {
        return caFamilyPhenotypeNames;
    }

    public ReportedVariantSearchModel setCaFamilyPhenotypeNames(List<String> caFamilyPhenotypeNames) {
        this.caFamilyPhenotypeNames = caFamilyPhenotypeNames;
        return this;
    }

    public List<String> getCaFamilyMemberIds() {
        return caFamilyMemberIds;
    }

    public ReportedVariantSearchModel setCaFamilyMemberIds(List<String> caFamilyMemberIds) {
        this.caFamilyMemberIds = caFamilyMemberIds;
        return this;
    }

    public String getIntBasicJson() {
        return intBasicJson;
    }

    public ReportedVariantSearchModel setIntBasicJson(String intBasicJson) {
        this.intBasicJson = intBasicJson;
        return this;
    }

    public String getIntId() {
        return intId;
    }

    public ReportedVariantSearchModel setIntId(String intId) {
        this.intId = intId;
        return this;
    }

    public String getIntClinicalAnalysisId() {
        return intClinicalAnalysisId;
    }

    public ReportedVariantSearchModel setIntClinicalAnalysisId(String intClinicalAnalysisId) {
        this.intClinicalAnalysisId = intClinicalAnalysisId;
        return this;
    }

    public String getIntSoftwareName() {
        return intSoftwareName;
    }

    public ReportedVariantSearchModel setIntSoftwareName(String intSoftwareName) {
        this.intSoftwareName = intSoftwareName;
        return this;
    }

    public String getIntSoftwareVersion() {
        return intSoftwareVersion;
    }

    public ReportedVariantSearchModel setIntSoftwareVersion(String intSoftwareVersion) {
        this.intSoftwareVersion = intSoftwareVersion;
        return this;
    }

    public String getIntAnalystName() {
        return intAnalystName;
    }

    public ReportedVariantSearchModel setIntAnalystName(String intAnalystName) {
        this.intAnalystName = intAnalystName;
        return this;
    }

    public List<String> getIntPanelNames() {
        return intPanelNames;
    }

    public ReportedVariantSearchModel setIntPanelNames(List<String> intPanelNames) {
        this.intPanelNames = intPanelNames;
        return this;
    }

    public List<String> getIntInfo() {
        return intInfo;
    }

    public ReportedVariantSearchModel setIntInfo(List<String> intInfo) {
        this.intInfo = intInfo;
        return this;
    }

    public String getIntCreationDate() {
        return intCreationDate;
    }

    public ReportedVariantSearchModel setIntCreationDate(String intCreationDate) {
        this.intCreationDate = intCreationDate;
        return this;
    }

    public String getProjectId() {
        return projectId;
    }

    public ReportedVariantSearchModel setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public ReportedVariantSearchModel setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public ReportedVariantSearchModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getStudyJson() {
        return studyJson;
    }

    public ReportedVariantSearchModel setStudyJson(String studyJson) {
        this.studyJson = studyJson;
        return this;
    }

    public Double getRvDeNovoQualityScore() {
        return rvDeNovoQualityScore;
    }

    public ReportedVariantSearchModel setRvDeNovoQualityScore(Double rvDeNovoQualityScore) {
        this.rvDeNovoQualityScore = rvDeNovoQualityScore;
        return this;
    }

    public List<String> getRvComments() {
        return rvComments;
    }

    public ReportedVariantSearchModel setRvComments(List<String> rvComments) {
        this.rvComments = rvComments;
        return this;
    }

    public String getRvReportedEventsJson() {
        return rvReportedEventsJson;
    }

    public ReportedVariantSearchModel setRvReportedEventsJson(String rvReportedEventsJson) {
        this.rvReportedEventsJson = rvReportedEventsJson;
        return this;
    }

    public String getRvAttributesJson() {
        return rvAttributesJson;
    }

    public ReportedVariantSearchModel setRvAttributesJson(String rvAttributesJson) {
        this.rvAttributesJson = rvAttributesJson;
        return this;
    }

    public List<String> getRePhenotypes() {
        return rePhenotypes;
    }

    public ReportedVariantSearchModel setRePhenotypes(List<String> rePhenotypes) {
        this.rePhenotypes = rePhenotypes;
        return this;
    }

    public List<String> getReConsequenceTypeIds() {
        return reConsequenceTypeIds;
    }

    public ReportedVariantSearchModel setReConsequenceTypeIds(List<String> reConsequenceTypeIds) {
        this.reConsequenceTypeIds = reConsequenceTypeIds;
        return this;
    }

    public List<String> getReGeneNames() {
        return reGeneNames;
    }

    public ReportedVariantSearchModel setReGeneNames(List<String> reGeneNames) {
        this.reGeneNames = reGeneNames;
        return this;
    }

    public List<String> getReXrefs() {
        return reXrefs;
    }

    public ReportedVariantSearchModel setReXrefs(List<String> reXrefs) {
        this.reXrefs = reXrefs;
        return this;
    }

    public List<String> getRePanelNames() {
        return rePanelNames;
    }

    public ReportedVariantSearchModel setRePanelNames(List<String> rePanelNames) {
        this.rePanelNames = rePanelNames;
        return this;
    }

    public List<String> getReAcmg() {
        return reAcmg;
    }

    public ReportedVariantSearchModel setReAcmg(List<String> reAcmg) {
        this.reAcmg = reAcmg;
        return this;
    }

    public List<String> getReClinicalSignificance() {
        return reClinicalSignificance;
    }

    public ReportedVariantSearchModel setReClinicalSignificance(List<String> reClinicalSignificance) {
        this.reClinicalSignificance = reClinicalSignificance;
        return this;
    }

    public List<String> getReDrugResponse() {
        return reDrugResponse;
    }

    public ReportedVariantSearchModel setReDrugResponse(List<String> reDrugResponse) {
        this.reDrugResponse = reDrugResponse;
        return this;
    }

    public List<String> getReTraitAssociation() {
        return reTraitAssociation;
    }

    public ReportedVariantSearchModel setReTraitAssociation(List<String> reTraitAssociation) {
        this.reTraitAssociation = reTraitAssociation;
        return this;
    }

    public List<String> getReFunctionalEffect() {
        return reFunctionalEffect;
    }

    public ReportedVariantSearchModel setReFunctionalEffect(List<String> reFunctionalEffect) {
        this.reFunctionalEffect = reFunctionalEffect;
        return this;
    }

    public List<String> getReTumorigenesis() {
        return reTumorigenesis;
    }

    public ReportedVariantSearchModel setReTumorigenesis(List<String> reTumorigenesis) {
        this.reTumorigenesis = reTumorigenesis;
        return this;
    }

    public List<String> getReOtherClassification() {
        return reOtherClassification;
    }

    public ReportedVariantSearchModel setReOtherClassification(List<String> reOtherClassification) {
        this.reOtherClassification = reOtherClassification;
        return this;
    }

    public List<String> getReRolesInCancer() {
        return reRolesInCancer;
    }

    public ReportedVariantSearchModel setReRolesInCancer(List<String> reRolesInCancer) {
        this.reRolesInCancer = reRolesInCancer;
        return this;
    }

    public Map<String, Double> getReScores() {
        return reScores;
    }

    public ReportedVariantSearchModel setReScores(Map<String, Double> reScores) {
        this.reScores = reScores;
        return this;
    }
}


