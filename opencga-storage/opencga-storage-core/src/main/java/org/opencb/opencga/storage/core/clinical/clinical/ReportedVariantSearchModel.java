package org.opencb.opencga.storage.core.clinical.clinical;

import org.apache.solr.client.solrj.beans.Field;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportedVariantSearchModel extends VariantSearchModel {

    // ---------- ClinicalAnalysis ----------

    @Field("caId")
    private String caId;

    @Field("caType")
    private String caType;

    @Field("caDisorderId")
    private String caDisorderId;

    @Field("caFiles")
    private List<String> caFiles;

    @Field("caProbandId")
    private String caProbandId;

    // caProbandPhenotypes contains both phenotype ID and name
    @Field("caProbandPhenotypes")
    private List<String> caProbandPhenotypes;

    // caProbandDisorders contains both phenotype ID and name
    @Field("caProbandDisorders")
    private List<String> caProbandDisorders;

    @Field("caProbandSampleIds")
    private List<String> caProbandSampleIds;

    @Field("caFamilyId")
    private String caFamilyId;

    @Field("caFamilyMemberIds")
    private List<String> caFamilyMemberIds;

    @Field("caAnalystId")
    private String caAnalystId;

    @Field("caPriority")
    private String caPriority;

    @Field("caFlags")
    private String caFlags;

    @Field("caQualityControl")
    private String caQualityControl;

    @Field("caStatus")
    private String caStatus;

    // TODO: Check if we need to also add the following fields
    /*
    private String creationDate;
    private String modificationDate;
    private String dueDate;
    private int release;
    *
    * */

    // caInfo contains the following interpretation info (by lines):
    // descriptions (DS) and comments (CM)
    @Field("caInfo")
    private List<String> caInfo;

    // caJson a JSON string containing all clinical analysis but the interpretation list
    @Field("caJson")
    private String caJson;


    // ---------- Interpretation----------

    @Field("intId")
    private String intId;

    @Field("intStatus")
    private String intStatus;

//    @Field("intSofwareName")
//    private String intSoftwareName;
//
//    @Field("intSofwareVersion")
//    private String intSoftwareVersion;

//    @Field("intAnalystName")
//    private String intAnalystName;
    @Field("intAnalystId")
    private String intAnalystId;

    @Field("intMethodNames")
    private String intMethodNames;

    // intPanels contains both panel ID and name
    @Field("intPanels")
    private List<String> intPanels;

    @Field("intCreationDate")
    private long intCreationDate;

    @Field("intCreationYear")
    private int intCreationYear;

    @Field("intCreationMonth")
    private int intCreationMonth;

    @Field("intCreationDay")
    private int intCreationDay;

    @Field("intCreationDayOfWeek")
    private String intCreationDayOfWeek;

    // intInfo contains the following interpretation info (by lines):
    // descriptions (DS), analyst (AN), dependencies (DP), filters (FT), comments (CM) and attributes (AT)
    @Field("intInfo")
    private List<String> intInfo;

    // intJson a JSON string containing all interpretation but the reported variant list
    @Field("intJson")
    private String intJson;

    // ---------- Catalog attributes ----------

    @Field("projectId")
    private String projectId;

    @Field("assembly")
    private String assembly;

    @Field("studyId")
    private String studyId;

    @Field("studyFqn")
    private String studyFqn;

    // JSON containing the array of Study and all their fields
    @Field("studyJson")
    private String studyJson;

    // ---------- Findings ----------

    @Field("fPrimary")
    private boolean fPrimary;

    @Field("fStatus")
    private String fStatus;

//    @Field("rvDeNovoQualityScore")
//    private Double rvDeNovoQualityScore;

    @Field("fInterpretationMethodNames")
    private List<String> fInterpretationMethodNames;

    @Field("fComments")
    private List<String> fComments;

    // A JSON string containing all evidences
    @Field("fEvidencesJson")
    private String fEvidencesJson;

    // A JSON string containing all attributes
    @Field("fAttributesJson")
    private String fAttributesJson;

    // ---------- Evidences----------

    @Field("eInterpretationMethodName")
    private String eInterpretationMethodName;

    @Field("ePhenotypes")
    private List<String> ePhenotypes;

    @Field("eGenomicFeatureId")
    private String eGenomicFeatureId;

    @Field("eGenomicFeatureType")
    private String eGenomicFeatureType;

    @Field("eGenomicFeatureTranscriptId")
    private String eGenomicFeatureTranscriptId;

    @Field("eGenomicFeatureGeneName")
    private String eGenomicFeatureGeneName;

    @Field("eGenomicFeatureConsequenceTypes")
    private List<String> eGenomicFeatureConsequenceTypes;

    @Field("eGenomicFeatureXrefs")
    private List<String> eGenomicFeatureXrefs;

    @Field("eModeOfInheritance")
    private String eModeOfInheritance;

    @Field("ePanelId")
    private String ePanelId;

    @Field("eClassificationTier")
    private String eClassificationTier;

    @Field("eClassificationAcmg")
    private List<String> eClassificationAcmg;

    @Field("eClassificationClinicalSignificance")
    private String eClassificationClinicalSignificance;

    @Field("eClassificationDrugResponse")
    private String eClassificationDrugResponse;

    @Field("eClassificationTraitAssociation")
    private String eClassificationTraitAssociation;

    @Field("eClassificationFunctionalEffect")
    private String eClassificationFunctionalEffect;

    @Field("eClassificationTumorigenesis")
    private String eClassificationTumoriGenesis;

    @Field("eClassificationOther")
    private List<String> eClassificationOther;

    @Field("ePenetrance")
    private String ePenetrance;

    @Field("eScore")
    private Double eScore;

    @Field("eFullyExplainPhenotypes")
    private boolean eFullyExplainPhenotypes;

    @Field("eCompoundHeterozygousVariantIds")
    private List<String> eCompoundHeterozygousVariantIds;

    @Field("eRoleInCancer")
    private String eRoleInCancer;

    @Field("eActionable")
    private boolean eActionable;

    @Field("eJustification")
    private String eJustification;




    @Field("reConsequenceTypeIds")
    private List<String> reConsequenceTypeIds;

    @Field("reGeneNames")
    private List<String> reGeneNames;

    @Field("reXrefs")
    private List<String> reXrefs;

    @Field("rePanelIds")
    private List<String> rePanelIds;

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

    @Field("reJustification_*")
    private Map<String, List<String>> reJustification;

    @Field("reTier")
    private List<String> reTier;

    @Field("reAux")
    private List<String> reAux;

    public ReportedVariantSearchModel() {
        // ---------- ClinicalAnalysis ----------
        caFiles = new ArrayList<>();
        caProbandPhenotypes = new ArrayList<>();
        caProbandDisorders = new ArrayList<>();
        caFamilyMemberIds = new ArrayList<>();

        // ---------- Interpretation----------
        intPanels = new ArrayList<>();
        intInfo = new ArrayList<>();

        // ---------- ReportedVariant ----------
        rvComments = new ArrayList<>();

        // ---------- ReportedEvent----------
        rePhenotypes = new ArrayList<>();
        reConsequenceTypeIds = new ArrayList<>();
        reGeneNames = new ArrayList<>();
        reXrefs = new ArrayList<>();
        rePanelIds = new ArrayList<>();
        reAcmg = new ArrayList<>();
        reClinicalSignificance = new ArrayList<>();
        reDrugResponse = new ArrayList<>();
        reTraitAssociation = new ArrayList<>();
        reFunctionalEffect = new ArrayList<>();
        reTumorigenesis = new ArrayList<>();
        reOtherClassification = new ArrayList<>();
        reRolesInCancer = new ArrayList<>();
        reJustification = new HashMap<>();
        reTier = new ArrayList<>();
        reAux = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ReportedVariantSearchModel{");
        sb.append("caId='").append(caId).append('\'');
        sb.append(", caDisorderId='").append(caDisorderId).append('\'');
        sb.append(", caFiles=").append(caFiles);
        sb.append(", caProbandId='").append(caProbandId).append('\'');
        sb.append(", caProbandPhenotypes=").append(caProbandPhenotypes);
        sb.append(", caProbandDisorders=").append(caProbandDisorders);
        sb.append(", caFamilyId='").append(caFamilyId).append('\'');
        sb.append(", caFamilyMemberIds=").append(caFamilyMemberIds);
        sb.append(", caInfo=").append(caInfo);
        sb.append(", caJson='").append(caJson).append('\'');
        sb.append(", intId='").append(intId).append('\'');
        sb.append(", intStatus='").append(intStatus).append('\'');
        sb.append(", intSoftwareName='").append(intSoftwareName).append('\'');
        sb.append(", intSoftwareVersion='").append(intSoftwareVersion).append('\'');
        sb.append(", intAnalystName='").append(intAnalystName).append('\'');
        sb.append(", intPanels=").append(intPanels);
        sb.append(", intInfo=").append(intInfo);
        sb.append(", intCreationDate=").append(intCreationDate);
        sb.append(", intCreationYear=").append(intCreationYear);
        sb.append(", intCreationMonth=").append(intCreationMonth);
        sb.append(", intCreationDay=").append(intCreationDay);
        sb.append(", intCreationDayOfWeek='").append(intCreationDayOfWeek).append('\'');
        sb.append(", intJson='").append(intJson).append('\'');
        sb.append(", projectId='").append(projectId).append('\'');
        sb.append(", assembly='").append(assembly).append('\'');
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", studyJson='").append(studyJson).append('\'');
        sb.append(", rvPrimaryFinding=").append(rvPrimaryFinding);
        sb.append(", rvStatus='").append(rvStatus).append('\'');
        sb.append(", rvDeNovoQualityScore=").append(rvDeNovoQualityScore);
        sb.append(", rvComments=").append(rvComments);
        sb.append(", rvReportedEventsJson='").append(rvReportedEventsJson).append('\'');
        sb.append(", rvAttributesJson='").append(rvAttributesJson).append('\'');
        sb.append(", rePhenotypes=").append(rePhenotypes);
        sb.append(", reConsequenceTypeIds=").append(reConsequenceTypeIds);
        sb.append(", reGeneNames=").append(reGeneNames);
        sb.append(", reXrefs=").append(reXrefs);
        sb.append(", rePanelIds=").append(rePanelIds);
        sb.append(", reAcmg=").append(reAcmg);
        sb.append(", reClinicalSignificance=").append(reClinicalSignificance);
        sb.append(", reDrugResponse=").append(reDrugResponse);
        sb.append(", reTraitAssociation=").append(reTraitAssociation);
        sb.append(", reFunctionalEffect=").append(reFunctionalEffect);
        sb.append(", reTumorigenesis=").append(reTumorigenesis);
        sb.append(", reOtherClassification=").append(reOtherClassification);
        sb.append(", reRolesInCancer=").append(reRolesInCancer);
        sb.append(", reJustification=").append(reJustification);
        sb.append(", reTier=").append(reTier);
        sb.append(", reAux=").append(reAux);
        sb.append('}');
        return sb.toString();
    }

    public String getCaId() {
        return caId;
    }

    public ReportedVariantSearchModel setCaId(String caId) {
        this.caId = caId;
        return this;
    }

    public String getCaDisorderId() {
        return caDisorderId;
    }

    public ReportedVariantSearchModel setCaDisorderId(String caDisorderId) {
        this.caDisorderId = caDisorderId;
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

    public List<String> getCaProbandPhenotypes() {
        return caProbandPhenotypes;
    }

    public ReportedVariantSearchModel setCaProbandPhenotypes(List<String> caProbandPhenotypes) {
        this.caProbandPhenotypes = caProbandPhenotypes;
        return this;
    }

    public List<String> getCaProbandDisorders() {
        return caProbandDisorders;
    }

    public ReportedVariantSearchModel setCaProbandDisorders(List<String> caProbandDisorders) {
        this.caProbandDisorders = caProbandDisorders;
        return this;
    }

    public String getCaFamilyId() {
        return caFamilyId;
    }

    public ReportedVariantSearchModel setCaFamilyId(String caFamilyId) {
        this.caFamilyId = caFamilyId;
        return this;
    }

    public List<String> getCaFamilyMemberIds() {
        return caFamilyMemberIds;
    }

    public ReportedVariantSearchModel setCaFamilyMemberIds(List<String> caFamilyMemberIds) {
        this.caFamilyMemberIds = caFamilyMemberIds;
        return this;
    }

    public List<String> getCaInfo() {
        return caInfo;
    }

    public ReportedVariantSearchModel setCaInfo(List<String> caInfo) {
        this.caInfo = caInfo;
        return this;
    }

    public String getCaJson() {
        return caJson;
    }

    public ReportedVariantSearchModel setCaJson(String caJson) {
        this.caJson = caJson;
        return this;
    }

    public String getIntId() {
        return intId;
    }

    public ReportedVariantSearchModel setIntId(String intId) {
        this.intId = intId;
        return this;
    }

    public String getIntStatus() {
        return intStatus;
    }

    public ReportedVariantSearchModel setIntStatus(String intStatus) {
        this.intStatus = intStatus;
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

    public List<String> getIntPanels() {
        return intPanels;
    }

    public ReportedVariantSearchModel setIntPanels(List<String> intPanels) {
        this.intPanels = intPanels;
        return this;
    }

    public List<String> getIntInfo() {
        return intInfo;
    }

    public ReportedVariantSearchModel setIntInfo(List<String> intInfo) {
        this.intInfo = intInfo;
        return this;
    }

    public long getIntCreationDate() {
        return intCreationDate;
    }

    public ReportedVariantSearchModel setIntCreationDate(long intCreationDate) {
        this.intCreationDate = intCreationDate;
        return this;
    }

    public int getIntCreationYear() {
        return intCreationYear;
    }

    public ReportedVariantSearchModel setIntCreationYear(int intCreationYear) {
        this.intCreationYear = intCreationYear;
        return this;
    }

    public int getIntCreationMonth() {
        return intCreationMonth;
    }

    public ReportedVariantSearchModel setIntCreationMonth(int intCreationMonth) {
        this.intCreationMonth = intCreationMonth;
        return this;
    }

    public int getIntCreationDay() {
        return intCreationDay;
    }

    public ReportedVariantSearchModel setIntCreationDay(int intCreationDay) {
        this.intCreationDay = intCreationDay;
        return this;
    }

    public String getIntCreationDayOfWeek() {
        return intCreationDayOfWeek;
    }

    public ReportedVariantSearchModel setIntCreationDayOfWeek(String intCreationDayOfWeek) {
        this.intCreationDayOfWeek = intCreationDayOfWeek;
        return this;
    }

    public String getIntJson() {
        return intJson;
    }

    public ReportedVariantSearchModel setIntJson(String intJson) {
        this.intJson = intJson;
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

    public boolean isRvPrimaryFinding() {
        return rvPrimaryFinding;
    }

    public ReportedVariantSearchModel setRvPrimaryFinding(boolean rvPrimaryFinding) {
        this.rvPrimaryFinding = rvPrimaryFinding;
        return this;
    }

    public String getRvStatus() {
        return rvStatus;
    }

    public ReportedVariantSearchModel setRvStatus(String rvStatus) {
        this.rvStatus = rvStatus;
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

    public List<String> getRePanelIds() {
        return rePanelIds;
    }

    public ReportedVariantSearchModel setRePanelIds(List<String> rePanelIds) {
        this.rePanelIds = rePanelIds;
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

    public Map<String, List<String>> getReJustification() {
        return reJustification;
    }

    public ReportedVariantSearchModel setReJustification(Map<String, List<String>> reJustification) {
        this.reJustification = reJustification;
        return this;
    }

    public List<String> getReTier() {
        return reTier;
    }

    public ReportedVariantSearchModel setReTier(List<String> reTier) {
        this.reTier = reTier;
        return this;
    }

    public List<String> getReAux() {
        return reAux;
    }

    public ReportedVariantSearchModel setReAux(List<String> reAux) {
        this.reAux = reAux;
        return this;
    }
}


