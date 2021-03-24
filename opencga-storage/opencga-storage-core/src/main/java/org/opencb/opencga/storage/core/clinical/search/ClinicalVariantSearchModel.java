package org.opencb.opencga.storage.core.clinical.search;

import org.apache.solr.client.solrj.beans.Field;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClinicalVariantSearchModel extends VariantSearchModel {

    // <!-- Clinical analysis -->

    // <field name="caId" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caId")
    private String caId;

    // <field name="caDisorderId" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caDisorderId")
    private String caDisorderId;

    // <field name="caType" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caType")
    private String caType;

    // <field name="caFiles" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caFiles")
    private List<String> caFiles;

    // <field name="caProbandId" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caProbandId")
    private String caProbandId;

    // <!-- caProbandPhenotypes contains both phenotype ID and name -->
    // <field name="caProbandPhenotypes" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caProbandPhenotypes")
    private List<String> caProbandPhenotypes;

    // <!-- caProbandDisorders contains both disorder ID and name -->
    // <field name="caProbandDisorders" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caProbandDisorders")
    private List<String> caProbandDisorders;

    // <field name="caProbandSampleIds" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caProbandSampleIds")
    private List<String> caProbandSampleIds;

    // <field name="caFamilyId" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caFamilyId")
    private String caFamilyId;

    // <field name="caFamilyMemberIds" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caFamilyMemberIds")
    private List<String> caFamilyMemberIds;

    // <field name="caConsent" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caConsent")
    private List<String> caConsent;

    // <field name="caPriority" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caPriority")
    private String caPriority;

    // <field name="caFlags" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caFlags")
    private List<String> caFlags;

    // <!-- Creation year, month and day will be used for facets -->
    // <field name="caCreationDate" type="long" indexed="true" stored="true" multiValued="false"/>
    @Field("caCreationDate")
    private long caCreationDate;

    // <field name="caCreationYear" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("caCreationYear")
    private int caCreationYear;

    // <field name="caCreationMonth" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("caCreationMonth")
    private int caCreationMonth;

    // <field name="caCreationDay" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("caCreationDay")
    private int caCreationDay;

    // <field name="caCreationDayOfWeek" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caCreationDayOfWeek")
    private String caCreationDayOfWeek;

    // <field name="caRelease" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("caRelease")
    private int caRelease;

    // <field name="caQualityControl" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caQualityControl")
    private String caQualityControl;

    // <field name="caAudit" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("caAudit")
    private List<String> caAudit;

    // <field name="caInternalStatus" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caInternalStatus")
    private String caInternalStatus;

    // <field name="caStatus" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caStatus")
    private String caStatus;

    // <field name="caAnalystName" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("caAnalystName")
    private String caAnalystName;

    // <!-- caInfo contains:
    // description     : DS - description text
    // comments        : CM - type - author - text
    // analyst         : AN - author - email - company
    // attributes      : AT - key=value
    // -->
    // <field name="caInfo" type="text_en" indexed="true" stored="true" multiValued="true"/>
    @Field("caInfo")
    private List<String> caInfo;

    // <!-- caJson contain all info about clinical analysis except the interpretations -->
    // <field name="caJson" type="string" indexed="false" stored="true" multiValued="false"/>
    @Field("caJson")
    private String caJson;

    // <!-- Interpretation -->

    // <field name="intId" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("intId")
    private String intId;

    // <field name="intMethodNames" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("intMethodNames")
    private String intMethodNames;

    // <!-- Creation year, month and day will be used for facets -->
    // <field name="intCreationDate" type="long" indexed="true" stored="true" multiValued="false"/>
    @Field("intCreationDate")
    private long intCreationDate;

    // <field name="intCreationYear" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("intCreationYear")
    private int intCreationYear;

    // <field name="intCreationMonth" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("intCreationMonth")
    private int intCreationMonth;

    // <field name="intCreationDay" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("intCreationDay")
    private int intCreationDay;

    // <field name="intCreationDayOfWeek" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("intCreationDayOfWeek")
    private String intCreationDayOfWeek;

    // <field name="intVersion" type="int" indexed="true" stored="true" multiValued="false"/>
    @Field("intVersion")
    private String intVersion;

    // <field name="intStatus" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("intStatus")
    private String intStatus;

    // <field name="intAnalystName" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("intAnalystName")
    private String intAnalystName;

    // <!-- intPanels contains both panel ID and name -->
    // <field name="intPanels" type="string" indexed="true" stored="true" multiValued="true"/>
    // intPanels contains both panel ID and name
    @Field("intPanels")
    private List<String> intPanels;

    // <!-- Interpretation intInfo contains:
    // description     : DS - description text
    // analyst         : AN - author - email - company
    // dependencies    : DP - name - version
    // filters         : FT - conservation=gerp<0.2
    // comments        : CM - type - author - text
    // attributes      : AT - key=value
    // -->
    // <field name="intInfo" type="text_en" indexed="true" stored="true" multiValued="true"/>
    @Field("intInfo")
    private List<String> intInfo;

    // <!-- intJson contain all info about interpretation except clinical variants and evidences  -->
    @Field("intJson")
    private String intJson;

    // <!-- Catalog attributes -->

    // <field name="projectId" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("projectId")
    private String projectId;

    // <field name="assembly" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("assembly")
    private String assembly;

    // <field name="studyId" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("studyId")
    private String studyId;

    // <field name="studyJson" type="string" indexed="false" stored="true" multiValued="false"/>
    @Field("studyJson")
    private String studyJson;


    // <!-- ClinicalVariant -->

    // <field name="cvSecondaryInterpretation" type="boolean" indexed="true" stored="true" multiValued="false"/>
    @Field("cvSecondaryInterpretation")
    private boolean cvSecondaryInterpretation;

    // <field name="cvInterpretationMethodNames" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cvInterpretationMethodNames")
    private List<String> cvInterpretationMethodNames;

    // <field name="cvStatus" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("cvStatus")
    private String cvStatus;

    // <field name="cvComments" type="text_en" indexed="true" stored="true" multiValued="true"/>
    @Field("cvComments")
    private List<String> cvComments;

    // <field name="cvDiscussion" type="string" indexed="true" stored="true" multiValued="false"/>
    @Field("cvDiscussion")
    private String cvDiscussion;

    // A JSON string containing all evidences
    // <field name="cvClinicalVariantEvidencesJson" type="string" indexed="false" stored="true" multiValued="false"/>
    @Field("cvClinicalVariantEvidencesJson")
    private String cvClinicalVariantEvidencesJson;

    // A JSON string containing all attributes
    // <field name="cvAttributesJson" type="string" indexed="false" stored="true" multiValued="false"/>
    @Field("cvAttributesJson")
    private String cvAttributesJson;


    // <!-- ClinicalVariantEvidence -->

    // <field name="cvePhenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cvePhenotypeNames")
    private List<String> cvePhenotypeNames;

    // <!-- cveConsequenceTypes contains consequence types from the genomic features -->
    // <field name="cveConsequenceTypes" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveConsequenceTypes")
    private List<String> cveConsequenceTypes;

    // <!-- cveXrefs contains IDs from the genomic features -->
    // <field name="cveXrefs" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveXrefs")
    private List<String> cveXrefs;

    // <field name="cveModeOfInheritances" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveModeOfInheritances")
    private List<String> cveModeOfInheritances;

    // <field name="cvePanelIds" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cvePanelIds")
    private List<String> cvePanelIds;

    // <field name="cvePenetrances" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cvePenetrances")
    private List<String> cvePenetrances;

    // <field name="cveTiers" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveTiers")
    private List<String> cveTiers;

    // <field name="cveAcmgs" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveAcmgs")
    private List<String> cveAcmgs;

    // <field name="cveClinicalSignificances" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveClinicalSignificances")
    private List<String> cveClinicalSignificances;

    // <field name="cveDrugResponses" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveDrugResponses")
    private List<String> cveDrugResponses;

    // <field name="cveTraitAssociations" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveTraitAssociations")
    private List<String> cveTraitAssociations;

    // <field name="cveFunctionalEffects" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveFunctionalEffects")
    private List<String> cveFunctionalEffects;

    // <field name="cveTumorigenesis" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveTumorigenesis")
    private List<String> cveTumorigenesis;

    // <field name="cveOtherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveOtherClassifications")
    private List<String> cveOtherClassifications;

    // <field name="cveRolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveRolesInCancer")
    private List<String> cveRolesInCancer;

    // <!-- The field cveAux will help us to query by combining multiple clinical variant evidences fields: panels, xrefs, acgm,... -->
    // <field name="cveAux" type="string" indexed="true" stored="true" multiValued="true"/>
    @Field("cveAux")
    private List<String> cveAux;

   // <dynamicFiled name="cveJustification_*" type="text_en" indexed="true" stored="true" multiValued="true"/>
    @Field("cveJustification_*")
    private Map<String, List<String>> cveJustification;

    public ClinicalVariantSearchModel() {
        this.caFiles = new ArrayList<>();
        this.caProbandPhenotypes = new ArrayList<>();
        this.caProbandDisorders = new ArrayList<>();
        this.caProbandSampleIds = new ArrayList<>();
        this.caFamilyMemberIds = new ArrayList<>();
        this.caConsent = new ArrayList<>();
        this.caFlags = new ArrayList<>();
        this.caAudit = new ArrayList<>();
        this.caInfo = new ArrayList<>();
        this.intPanels = new ArrayList<>();
        this.intInfo = new ArrayList<>();
        this.cvInterpretationMethodNames = new ArrayList<>();
        this.cvComments = new ArrayList<>();
        this.cvePhenotypeNames = new ArrayList<>();
        this.cveConsequenceTypes = new ArrayList<>();
        this.cveXrefs = new ArrayList<>();
        this.cveModeOfInheritances = new ArrayList<>();
        this.cvePanelIds = new ArrayList<>();
        this.cvePenetrances = new ArrayList<>();
        this.cveTiers = new ArrayList<>();
        this.cveAcmgs = new ArrayList<>();
        this.cveClinicalSignificances = new ArrayList<>();
        this.cveDrugResponses = new ArrayList<>();
        this.cveTraitAssociations = new ArrayList<>();
        this.cveFunctionalEffects = new ArrayList<>();
        this.cveTumorigenesis = new ArrayList<>();
        this.cveOtherClassifications = new ArrayList<>();
        this.cveRolesInCancer = new ArrayList<>();
        this.cveAux = new ArrayList<>();
        this.cveJustification = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalVariantSearchModel{");
        sb.append("caId='").append(caId).append('\'');
        sb.append(", caDisorderId='").append(caDisorderId).append('\'');
        sb.append(", caType='").append(caType).append('\'');
        sb.append(", caFiles=").append(caFiles);
        sb.append(", caProbandId='").append(caProbandId).append('\'');
        sb.append(", caProbandPhenotypes=").append(caProbandPhenotypes);
        sb.append(", caProbandDisorders=").append(caProbandDisorders);
        sb.append(", caProbandSampleIds=").append(caProbandSampleIds);
        sb.append(", caFamilyId='").append(caFamilyId).append('\'');
        sb.append(", caFamilyMemberIds=").append(caFamilyMemberIds);
        sb.append(", caConsent=").append(caConsent);
        sb.append(", caPriority='").append(caPriority).append('\'');
        sb.append(", caFlags=").append(caFlags);
        sb.append(", caCreationDate=").append(caCreationDate);
        sb.append(", caCreationYear=").append(caCreationYear);
        sb.append(", caCreationMonth=").append(caCreationMonth);
        sb.append(", caCreationDay=").append(caCreationDay);
        sb.append(", caCreationDayOfWeek='").append(caCreationDayOfWeek).append('\'');
        sb.append(", caRelease=").append(caRelease);
        sb.append(", caQualityControl='").append(caQualityControl).append('\'');
        sb.append(", caAudit=").append(caAudit);
        sb.append(", caInternalStatus='").append(caInternalStatus).append('\'');
        sb.append(", caStatus='").append(caStatus).append('\'');
        sb.append(", caAnalystName='").append(caAnalystName).append('\'');
        sb.append(", caInfo=").append(caInfo);
        sb.append(", caJson='").append(caJson).append('\'');
        sb.append(", intId='").append(intId).append('\'');
        sb.append(", intMethodNames='").append(intMethodNames).append('\'');
        sb.append(", intCreationDate=").append(intCreationDate);
        sb.append(", intCreationYear=").append(intCreationYear);
        sb.append(", intCreationMonth=").append(intCreationMonth);
        sb.append(", intCreationDay=").append(intCreationDay);
        sb.append(", intCreationDayOfWeek='").append(intCreationDayOfWeek).append('\'');
        sb.append(", intVersion='").append(intVersion).append('\'');
        sb.append(", intStatus='").append(intStatus).append('\'');
        sb.append(", intAnalystName='").append(intAnalystName).append('\'');
        sb.append(", intPanels=").append(intPanels);
        sb.append(", intInfo=").append(intInfo);
        sb.append(", intJson='").append(intJson).append('\'');
        sb.append(", projectId='").append(projectId).append('\'');
        sb.append(", assembly='").append(assembly).append('\'');
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", studyJson='").append(studyJson).append('\'');
        sb.append(", cvSecondaryInterpretation=").append(cvSecondaryInterpretation);
        sb.append(", cvInterpretationMethodNames=").append(cvInterpretationMethodNames);
        sb.append(", cvStatus='").append(cvStatus).append('\'');
        sb.append(", cvComments=").append(cvComments);
        sb.append(", cvDiscussion='").append(cvDiscussion).append('\'');
        sb.append(", cvClinicalVariantEvidencesJson='").append(cvClinicalVariantEvidencesJson).append('\'');
        sb.append(", cvAttributesJson='").append(cvAttributesJson).append('\'');
        sb.append(", cvePhenotypeNames=").append(cvePhenotypeNames);
        sb.append(", cveConsequenceTypes=").append(cveConsequenceTypes);
        sb.append(", cveXrefs=").append(cveXrefs);
        sb.append(", cveModeOfInheritances=").append(cveModeOfInheritances);
        sb.append(", cvePanelIds=").append(cvePanelIds);
        sb.append(", cvePenetrances=").append(cvePenetrances);
        sb.append(", cveTiers=").append(cveTiers);
        sb.append(", cveAcmgs=").append(cveAcmgs);
        sb.append(", cveClinicalSignificances=").append(cveClinicalSignificances);
        sb.append(", cveDrugResponses=").append(cveDrugResponses);
        sb.append(", cveTraitAssociations=").append(cveTraitAssociations);
        sb.append(", cveFunctionalEffects=").append(cveFunctionalEffects);
        sb.append(", cveTumorigenesis=").append(cveTumorigenesis);
        sb.append(", cveOtherClassifications=").append(cveOtherClassifications);
        sb.append(", cveRolesInCancer=").append(cveRolesInCancer);
        sb.append(", cveAux=").append(cveAux);
        sb.append(", cveJustification=").append(cveJustification);
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

    public String getCaDisorderId() {
        return caDisorderId;
    }

    public ClinicalVariantSearchModel setCaDisorderId(String caDisorderId) {
        this.caDisorderId = caDisorderId;
        return this;
    }

    public String getCaType() {
        return caType;
    }

    public ClinicalVariantSearchModel setCaType(String caType) {
        this.caType = caType;
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

    public List<String> getCaProbandPhenotypes() {
        return caProbandPhenotypes;
    }

    public ClinicalVariantSearchModel setCaProbandPhenotypes(List<String> caProbandPhenotypes) {
        this.caProbandPhenotypes = caProbandPhenotypes;
        return this;
    }

    public List<String> getCaProbandDisorders() {
        return caProbandDisorders;
    }

    public ClinicalVariantSearchModel setCaProbandDisorders(List<String> caProbandDisorders) {
        this.caProbandDisorders = caProbandDisorders;
        return this;
    }

    public List<String> getCaProbandSampleIds() {
        return caProbandSampleIds;
    }

    public ClinicalVariantSearchModel setCaProbandSampleIds(List<String> caProbandSampleIds) {
        this.caProbandSampleIds = caProbandSampleIds;
        return this;
    }

    public String getCaFamilyId() {
        return caFamilyId;
    }

    public ClinicalVariantSearchModel setCaFamilyId(String caFamilyId) {
        this.caFamilyId = caFamilyId;
        return this;
    }

    public List<String> getCaFamilyMemberIds() {
        return caFamilyMemberIds;
    }

    public ClinicalVariantSearchModel setCaFamilyMemberIds(List<String> caFamilyMemberIds) {
        this.caFamilyMemberIds = caFamilyMemberIds;
        return this;
    }

    public List<String> getCaConsent() {
        return caConsent;
    }

    public ClinicalVariantSearchModel setCaConsent(List<String> caConsent) {
        this.caConsent = caConsent;
        return this;
    }

    public String getCaPriority() {
        return caPriority;
    }

    public ClinicalVariantSearchModel setCaPriority(String caPriority) {
        this.caPriority = caPriority;
        return this;
    }

    public List<String> getCaFlags() {
        return caFlags;
    }

    public ClinicalVariantSearchModel setCaFlags(List<String> caFlags) {
        this.caFlags = caFlags;
        return this;
    }

    public long getCaCreationDate() {
        return caCreationDate;
    }

    public ClinicalVariantSearchModel setCaCreationDate(long caCreationDate) {
        this.caCreationDate = caCreationDate;
        return this;
    }

    public int getCaCreationYear() {
        return caCreationYear;
    }

    public ClinicalVariantSearchModel setCaCreationYear(int caCreationYear) {
        this.caCreationYear = caCreationYear;
        return this;
    }

    public int getCaCreationMonth() {
        return caCreationMonth;
    }

    public ClinicalVariantSearchModel setCaCreationMonth(int caCreationMonth) {
        this.caCreationMonth = caCreationMonth;
        return this;
    }

    public int getCaCreationDay() {
        return caCreationDay;
    }

    public ClinicalVariantSearchModel setCaCreationDay(int caCreationDay) {
        this.caCreationDay = caCreationDay;
        return this;
    }

    public String getCaCreationDayOfWeek() {
        return caCreationDayOfWeek;
    }

    public ClinicalVariantSearchModel setCaCreationDayOfWeek(String caCreationDayOfWeek) {
        this.caCreationDayOfWeek = caCreationDayOfWeek;
        return this;
    }

    public int getCaRelease() {
        return caRelease;
    }

    public ClinicalVariantSearchModel setCaRelease(int caRelease) {
        this.caRelease = caRelease;
        return this;
    }

    public String getCaQualityControl() {
        return caQualityControl;
    }

    public ClinicalVariantSearchModel setCaQualityControl(String caQualityControl) {
        this.caQualityControl = caQualityControl;
        return this;
    }

    public List<String> getCaAudit() {
        return caAudit;
    }

    public ClinicalVariantSearchModel setCaAudit(List<String> caAudit) {
        this.caAudit = caAudit;
        return this;
    }

    public String getCaInternalStatus() {
        return caInternalStatus;
    }

    public ClinicalVariantSearchModel setCaInternalStatus(String caInternalStatus) {
        this.caInternalStatus = caInternalStatus;
        return this;
    }

    public String getCaStatus() {
        return caStatus;
    }

    public ClinicalVariantSearchModel setCaStatus(String caStatus) {
        this.caStatus = caStatus;
        return this;
    }

    public String getCaAnalystName() {
        return caAnalystName;
    }

    public ClinicalVariantSearchModel setCaAnalystName(String caAnalystName) {
        this.caAnalystName = caAnalystName;
        return this;
    }

    public List<String> getCaInfo() {
        return caInfo;
    }

    public ClinicalVariantSearchModel setCaInfo(List<String> caInfo) {
        this.caInfo = caInfo;
        return this;
    }

    public String getCaJson() {
        return caJson;
    }

    public ClinicalVariantSearchModel setCaJson(String caJson) {
        this.caJson = caJson;
        return this;
    }

    public String getIntId() {
        return intId;
    }

    public ClinicalVariantSearchModel setIntId(String intId) {
        this.intId = intId;
        return this;
    }

    public String getIntMethodNames() {
        return intMethodNames;
    }

    public ClinicalVariantSearchModel setIntMethodNames(String intMethodNames) {
        this.intMethodNames = intMethodNames;
        return this;
    }

    public long getIntCreationDate() {
        return intCreationDate;
    }

    public ClinicalVariantSearchModel setIntCreationDate(long intCreationDate) {
        this.intCreationDate = intCreationDate;
        return this;
    }

    public int getIntCreationYear() {
        return intCreationYear;
    }

    public ClinicalVariantSearchModel setIntCreationYear(int intCreationYear) {
        this.intCreationYear = intCreationYear;
        return this;
    }

    public int getIntCreationMonth() {
        return intCreationMonth;
    }

    public ClinicalVariantSearchModel setIntCreationMonth(int intCreationMonth) {
        this.intCreationMonth = intCreationMonth;
        return this;
    }

    public int getIntCreationDay() {
        return intCreationDay;
    }

    public ClinicalVariantSearchModel setIntCreationDay(int intCreationDay) {
        this.intCreationDay = intCreationDay;
        return this;
    }

    public String getIntCreationDayOfWeek() {
        return intCreationDayOfWeek;
    }

    public ClinicalVariantSearchModel setIntCreationDayOfWeek(String intCreationDayOfWeek) {
        this.intCreationDayOfWeek = intCreationDayOfWeek;
        return this;
    }

    public String getIntVersion() {
        return intVersion;
    }

    public ClinicalVariantSearchModel setIntVersion(String intVersion) {
        this.intVersion = intVersion;
        return this;
    }

    public String getIntStatus() {
        return intStatus;
    }

    public ClinicalVariantSearchModel setIntStatus(String intStatus) {
        this.intStatus = intStatus;
        return this;
    }

    public String getIntAnalystName() {
        return intAnalystName;
    }

    public ClinicalVariantSearchModel setIntAnalystName(String intAnalystName) {
        this.intAnalystName = intAnalystName;
        return this;
    }

    public List<String> getIntPanels() {
        return intPanels;
    }

    public ClinicalVariantSearchModel setIntPanels(List<String> intPanels) {
        this.intPanels = intPanels;
        return this;
    }

    public List<String> getIntInfo() {
        return intInfo;
    }

    public ClinicalVariantSearchModel setIntInfo(List<String> intInfo) {
        this.intInfo = intInfo;
        return this;
    }

    public String getIntJson() {
        return intJson;
    }

    public ClinicalVariantSearchModel setIntJson(String intJson) {
        this.intJson = intJson;
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

    public boolean isCvSecondaryInterpretation() {
        return cvSecondaryInterpretation;
    }

    public ClinicalVariantSearchModel setCvSecondaryInterpretation(boolean cvSecondaryInterpretation) {
        this.cvSecondaryInterpretation = cvSecondaryInterpretation;
        return this;
    }

    public List<String> getCvInterpretationMethodNames() {
        return cvInterpretationMethodNames;
    }

    public ClinicalVariantSearchModel setCvInterpretationMethodNames(List<String> cvInterpretationMethodNames) {
        this.cvInterpretationMethodNames = cvInterpretationMethodNames;
        return this;
    }

    public String getCvStatus() {
        return cvStatus;
    }

    public ClinicalVariantSearchModel setCvStatus(String cvStatus) {
        this.cvStatus = cvStatus;
        return this;
    }

    public List<String> getCvComments() {
        return cvComments;
    }

    public ClinicalVariantSearchModel setCvComments(List<String> cvComments) {
        this.cvComments = cvComments;
        return this;
    }

    public String getCvDiscussion() {
        return cvDiscussion;
    }

    public ClinicalVariantSearchModel setCvDiscussion(String cvDiscussion) {
        this.cvDiscussion = cvDiscussion;
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

    public List<String> getCvePhenotypeNames() {
        return cvePhenotypeNames;
    }

    public ClinicalVariantSearchModel setCvePhenotypeNames(List<String> cvePhenotypeNames) {
        this.cvePhenotypeNames = cvePhenotypeNames;
        return this;
    }

    public List<String> getCveConsequenceTypes() {
        return cveConsequenceTypes;
    }

    public ClinicalVariantSearchModel setCveConsequenceTypes(List<String> cveConsequenceTypes) {
        this.cveConsequenceTypes = cveConsequenceTypes;
        return this;
    }

    public List<String> getCveXrefs() {
        return cveXrefs;
    }

    public ClinicalVariantSearchModel setCveXrefs(List<String> cveXrefs) {
        this.cveXrefs = cveXrefs;
        return this;
    }

    public List<String> getCveModeOfInheritances() {
        return cveModeOfInheritances;
    }

    public ClinicalVariantSearchModel setCveModeOfInheritances(List<String> cveModeOfInheritances) {
        this.cveModeOfInheritances = cveModeOfInheritances;
        return this;
    }

    public List<String> getCvePanelIds() {
        return cvePanelIds;
    }

    public ClinicalVariantSearchModel setCvePanelIds(List<String> cvePanelIds) {
        this.cvePanelIds = cvePanelIds;
        return this;
    }

    public List<String> getCvePenetrances() {
        return cvePenetrances;
    }

    public ClinicalVariantSearchModel setCvePenetrances(List<String> cvePenetrances) {
        this.cvePenetrances = cvePenetrances;
        return this;
    }

    public List<String> getCveTiers() {
        return cveTiers;
    }

    public ClinicalVariantSearchModel setCveTiers(List<String> cveTiers) {
        this.cveTiers = cveTiers;
        return this;
    }

    public List<String> getCveAcmgs() {
        return cveAcmgs;
    }

    public ClinicalVariantSearchModel setCveAcmgs(List<String> cveAcmgs) {
        this.cveAcmgs = cveAcmgs;
        return this;
    }

    public List<String> getCveClinicalSignificances() {
        return cveClinicalSignificances;
    }

    public ClinicalVariantSearchModel setCveClinicalSignificances(List<String> cveClinicalSignificances) {
        this.cveClinicalSignificances = cveClinicalSignificances;
        return this;
    }

    public List<String> getCveDrugResponses() {
        return cveDrugResponses;
    }

    public ClinicalVariantSearchModel setCveDrugResponses(List<String> cveDrugResponses) {
        this.cveDrugResponses = cveDrugResponses;
        return this;
    }

    public List<String> getCveTraitAssociations() {
        return cveTraitAssociations;
    }

    public ClinicalVariantSearchModel setCveTraitAssociations(List<String> cveTraitAssociations) {
        this.cveTraitAssociations = cveTraitAssociations;
        return this;
    }

    public List<String> getCveFunctionalEffects() {
        return cveFunctionalEffects;
    }

    public ClinicalVariantSearchModel setCveFunctionalEffects(List<String> cveFunctionalEffects) {
        this.cveFunctionalEffects = cveFunctionalEffects;
        return this;
    }

    public List<String> getCveTumorigenesis() {
        return cveTumorigenesis;
    }

    public ClinicalVariantSearchModel setCveTumorigenesis(List<String> cveTumorigenesis) {
        this.cveTumorigenesis = cveTumorigenesis;
        return this;
    }

    public List<String> getCveOtherClassifications() {
        return cveOtherClassifications;
    }

    public ClinicalVariantSearchModel setCveOtherClassifications(List<String> cveOtherClassifications) {
        this.cveOtherClassifications = cveOtherClassifications;
        return this;
    }

    public List<String> getCveRolesInCancer() {
        return cveRolesInCancer;
    }

    public ClinicalVariantSearchModel setCveRolesInCancer(List<String> cveRolesInCancer) {
        this.cveRolesInCancer = cveRolesInCancer;
        return this;
    }

    public List<String> getCveAux() {
        return cveAux;
    }

    public ClinicalVariantSearchModel setCveAux(List<String> cveAux) {
        this.cveAux = cveAux;
        return this;
    }

    public Map<String, List<String>> getCveJustification() {
        return cveJustification;
    }

    public ClinicalVariantSearchModel setCveJustification(Map<String, List<String>> cveJustification) {
        this.cveJustification = cveJustification;
        return this;
    }
}



