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

package com.zettagenomics.opencga.enterprise.cvdb.parsers;

import com.zettagenomics.opencga.enterprise.core.api.ParamConstants;
import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

public final class ClinicalQueryParam implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<ClinicalQueryParam> VALUES = new ArrayList<>();
    private static final String ACCEPTS_ALL_NONE = "Accepts '" + ALL + "' and '" + NONE + "'.";
    private static final String ACCEPTS_AND_OR = "Accepts AND (" + AND + ") and OR (" + OR + ") operators.";

    public static final String PROJECT_ID_DESCR = ParamConstants.PROJECT_PARAM_DESCRIPTION;
    public static final ClinicalQueryParam PROJECT_ID = new ClinicalQueryParam(ParamConstants.PROJECT_PARAM_NAME,
            STRING, PROJECT_ID_DESCR);

    // ---------- Commons

    private static final String OPT_LIST= " separated by commas)";

    // ---------- Clinical analysis (aka CA)

    // <field name="id" type="string" indexed="true" stored="true" required="true" multiValued="false" />
    public static final String CA_ID_NAME = "caId";
    public static final String CA_ID_DESCR = "Clinical analysis ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CA_ID = new ClinicalQueryParam(CA_ID_NAME, TEXT_ARRAY, CA_ID_DESCR);

    // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>

    // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>
    public static final String CA_TYPE_NAME = "caType";
    public static final String CA_TYPE_DESCR = "Clinical analysis type (or list of types" + OPT_LIST;
    public static final ClinicalQueryParam CA_TYPE = new ClinicalQueryParam(CA_TYPE_NAME, TEXT_ARRAY, CA_TYPE_DESCR);

    // <field name="disorderId" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CA_DISORDER_ID_NAME = "caDisorderId";
    public static final String CA_DISORDER_ID_DESCR = "Clinical analysis disorder ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CA_DISORDER_ID = new ClinicalQueryParam(CA_DISORDER_ID_NAME, TEXT_ARRAY, CA_DISORDER_ID_DESCR);

    // <field name="fileNames" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CA_FILENAME_NAME = "caFilename";
    public static final String CA_FILENAME_DESCR = "Clinical analysis filename (or list of filenames" + OPT_LIST;
    public static final ClinicalQueryParam CA_FILENAME_ID = new ClinicalQueryParam(CA_FILENAME_NAME, TEXT_ARRAY, CA_FILENAME_DESCR);

    // <field name="probandId" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CA_PROBAND_ID_NAME = "caProbandId";
    public static final String CA_PROBAND_ID_DESCR = "Clinical analysis proband ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CA_PROBAND_ID = new ClinicalQueryParam(CA_PROBAND_ID_NAME, TEXT_ARRAY, CA_PROBAND_ID_DESCR);

    // <field name="familyId" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CA_FAMILY_ID_NAME = "caFamilyId";
    public static final String CA_FAMILY_ID_DESCR = "Clinical analysis family ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CA_FAMILY_ID = new ClinicalQueryParam(CA_FAMILY_ID_NAME, TEXT_ARRAY, CA_FAMILY_ID_DESCR);

    // <field name="familyPhenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CA_FAMILY_PHENOTYPE_NAME_NAME = "caFamilyPhenotypeName";
    public static final String CA_FAMILY_PHENOTYPE_NAME_DESCR = "Clinical analysis family phenotype names (or list of names" + OPT_LIST;
    public static final ClinicalQueryParam CA_FAMILY_PHENOTYPE_NAME = new ClinicalQueryParam(CA_FAMILY_PHENOTYPE_NAME_NAME, TEXT_ARRAY,
            CA_FAMILY_PHENOTYPE_NAME_DESCR);

    // <field name="familyMemberIds" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CA_FAMILY_MEMBER_ID_NAME = "caFamilyMemberId";
    public static final String CA_FAMILY_MEMBER_ID_DESCR = "Clinical analysis family member ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CA_FAMILY_MEMBER_ID = new ClinicalQueryParam(CA_FAMILY_MEMBER_ID_NAME, TEXT_ARRAY,
            CA_FAMILY_MEMBER_ID_DESCR);

    // <field name="report" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="status" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

    // ---------- Clinical interpretation (aka CI)

    public static final String CI_ID_NAME = "ciId";
    public static final String CI_ID_DESCR = "Clinical interpretation ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CI_ID = new ClinicalQueryParam(CI_ID_NAME, TEXT_ARRAY, CI_ID_DESCR);

    // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
    // <field name="description" type="string" indexed="true" stored="true" multiValued="false"/>

    // <!-- Panel IDs contain both IDs and names -->
    // <field name="panelIds" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CI_PANEL_ID_NAME = "ciPanelId";
    public static final String CI_PANEL_ID_DESCR = "Clinical interpretation panel ID or name (or list of IDs or names" + OPT_LIST;
    public static final ClinicalQueryParam CI_PANEL_ID = new ClinicalQueryParam(CI_PANEL_ID_NAME, TEXT_ARRAY, CI_PANEL_ID_DESCR);

    // <field name="analystId" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_ANALYIST_ID_NAME = "ciAnalystId";
    public static final String CI_ANALYIST_ID_DESCR = "Clinical interpretation analyst ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CI_ANALYIST_ID = new ClinicalQueryParam(CI_ANALYIST_ID_NAME, TEXT_ARRAY, CI_ANALYIST_ID_DESCR);

    // <field name="analystName" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_ANALYIST_NAME_NAME = "ciAnalystName";
    public static final String CI_ANALYIST_NAME_DESCR = "Clinical interpretation analyst name (or list of names" + OPT_LIST;
    public static final ClinicalQueryParam CI_ANALYIST_NAME = new ClinicalQueryParam(CI_ANALYIST_NAME_NAME, TEXT_ARRAY,
            CI_ANALYIST_NAME_DESCR);

    // <field name="analystEmail" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_ANALYIST_EMAIL_NAME = "ciAnalystEmail";
    public static final String CI_ANALYIST_EMAIL_DESCR = "Clinical interpretation analyst e-mail (or list of e-mails" + OPT_LIST;
    public static final ClinicalQueryParam CI_ANALYIST_EMAIL = new ClinicalQueryParam(CI_ANALYIST_EMAIL_NAME, TEXT_ARRAY,
            CI_ANALYIST_EMAIL_DESCR);

    // <field name="analystAssignedBy" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_ANALYIST_ASSIGNED_BY_NAME = "ciAnalystAssignedBy";
    public static final String CI_ANALYIST_ASSIGNED_BY_DESCR = "Clinical interpretation analyst assignee name (or list of names" + OPT_LIST;
    public static final ClinicalQueryParam CI_ANALYIST_ASSIGNED_BY = new ClinicalQueryParam(CI_ANALYIST_ASSIGNED_BY_NAME, TEXT_ARRAY,
            CI_ANALYIST_ASSIGNED_BY_DESCR);

    // <field name="analystDate" type="string" indexed="true" stored="true" multiValued="false"/>

    // <field name="methodName" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_METHOD_NAME_NAME = "ciMethodName";
    public static final String CI_METHOD_NAME_DESCR = "Clinical interpretation method name (or list of names" + OPT_LIST;
    public static final ClinicalQueryParam CI_METHOD_NAME = new ClinicalQueryParam(CI_METHOD_NAME_NAME, TEXT_ARRAY,
            CI_METHOD_NAME_DESCR);

    // <field name="methodVersion" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_METHOD_VERSION_NAME = "ciMethodVersion";
    public static final String CI_METHOD_VERSION_DESCR = "Clinical interpretation method version (or list of versions" + OPT_LIST;
    public static final ClinicalQueryParam CI_METHOD_VERSION = new ClinicalQueryParam(CI_METHOD_VERSION_NAME, TEXT_ARRAY,
            CI_METHOD_VERSION_DESCR);

    // <field name="methodCommit" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_METHOD_COMMIT_NAME = "ciMethodCommit";
    public static final String CI_METHOD_COMMIT_DESCR = "Clinical interpretation method commit (or list of commits" + OPT_LIST;
    public static final ClinicalQueryParam CI_METHOD_COMMIT = new ClinicalQueryParam(CI_METHOD_NAME_NAME, TEXT_ARRAY,
            CI_METHOD_COMMIT_DESCR);

    // <!-- Method software/dependencies are stores: name == version -->
    // <field name="methodDependencies" type="string" indexed="true" stored="true" multiValued="true"/>
    // <!-- Comments are stores: author == message == tag1:tag2:.. == date -->
    // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
    // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

    // <field name="statusId" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_STATUS_ID_NAME = "ciStatusId";
    public static final String CI_STATUS_ID_DESCR = "Clinical interpretation status ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CI_STATUS_ID = new ClinicalQueryParam(CI_STATUS_ID_NAME, TEXT_ARRAY,
            CI_STATUS_ID_DESCR);

    // <field name="statusName" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CI_STATUS_NAME_NAME = "ciStatusName";
    public static final String CI_STATUS_NAME_DESCR = "Clinical interpretation status name (or list of names" + OPT_LIST;
    public static final ClinicalQueryParam CI_STATUS_NAME = new ClinicalQueryParam(CI_STATUS_NAME_NAME, TEXT_ARRAY,
            CI_STATUS_NAME_DESCR);

    // <field name="statusDescription" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="statusDate" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="creationDate" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="modificationDate" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="version" type="int" indexed="true" stored="true" multiValued="false"/>

    // ---------- Clinical variant (aka CV)

    public static final String CV_ID_NAME = "cvId";
    public static final String CV_ID_DESCR = "Clinical variant ID (or list of ID" + OPT_LIST;
    public static final ClinicalQueryParam CV_ID = new ClinicalQueryParam(CV_ID_NAME, TEXT_ARRAY, CV_ID_DESCR);

    public static final String CV_VARIANT_ID_DESCR = ParamConstants.VARIANT_QUERY_DESCRIPTION;
    public static final ClinicalQueryParam CV_VARIANT_ID = new ClinicalQueryParam(ParamConstants.VARIANT_QUERY_PARAM,
            TEXT_ARRAY, CV_VARIANT_ID_DESCR);

    // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
    // <!-- Comments are stores: author == message == tag1:tag2:.. == date -->
    // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
    // <!-- Filters are stored in two dynamic fields: one for string values, the other one for numeric ones -->
    // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
    // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
    // <field name="discussionAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="confidenceValue" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="confidenceAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="tags" type="string" indexed="true" stored="true" multiValued="true"/>
    // <field name="status" type="string" indexed="true" stored="true" multiValued="false"/>

    // Variant filters
    // <field name="variantId" type="string" indexed="false" stored="true" multiValued="false"/>
    // <field name="chromosome" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="start" type="int" indexed="true" stored="true" multiValued="false"/>
    // <field name="end" type="int" indexed="true" stored="true" multiValued="false"/>
    // <field name="xrefs" type="string" indexed="true" stored="true" multiValued="true"/>
    // <field name="type" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="release" type="int" indexed="true" stored="true" multiValued="false"/>
    // <field name="studies" type="string" indexed="true" stored="true" multiValued="true"/>
    // <field name="phastCons" type="double" indexed="true" stored="true" multiValued="false"/>
    // <field name="phylop" type="double" indexed="true" stored="true" multiValued="false"/>
    // <field name="gerp" type="double" indexed="true" stored="true" multiValued="false"/>
    // <field name="caddRaw" type="double" indexed="true" stored="true" multiValued="false"/>
    // <field name="caddScaled" type="double" indexed="true" stored="true" multiValued="false"/>
    // <field name="sift" type="double" indexed="true" stored="true" multiValued="false"/>
    // <field name="siftDesc" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="polyphen" type="double" indexed="true" stored="true" multiValued="false"/>
    // <field name="polyphenDesc" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="genes" type="string" indexed="false" stored="true" multiValued="true"/>
    // <field name="biotypes" type="string" indexed="true" stored="true" multiValued="true"/>
    // <field name="soAcc" type="int" indexed="true" stored="true" multiValued="true"/>
    // <field name="geneToSoAcc" type="string" indexed="true" stored="true" multiValued="true"/>
    // <field name="clinicalSig" type="string" indexed="true" stored="true" multiValued="true"/>
    // <field name="traits" type="text_en" indexed="true" stored="true" multiValued="true"/>
    // <field name="other" type="string" indexed="false" stored="true" multiValued="true"/>
    // <dynamicField name="passStats_*" type="float" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="altStats_*" type="float" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="popFreq_*" type="float" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="score_*" type="float" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="scorePValue_*" type="float" indexed="true" stored="true" multiValued="false"/>
    // <!-- These fields are only present when indexing one individual or a family -->
    // <dynamicField name="gt_*" type="string" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="dp_*" type="int" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="sampleFormat_*" type="string" indexed="false" stored="true" multiValued="false"/>
    // <dynamicField name="qual_*" type="float" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="filter_*" type="string" indexed="true" stored="true" multiValued="false"/>
    // <dynamicField name="fileInfo_*" type="string" indexed="false" stored="true" multiValued="false"/>

    // ---------- Clinical variant evidence (aka CVE)

    // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CVE_PHENOTYPE_NAME_NAME = "cvePhenotypeName";
    public static final String CVE_PHENOTYPE_NAME_DESCR = "Clinical variant evidence phenotype name (or names" + OPT_LIST;
    public static final ClinicalQueryParam CVE_PHENOTYPE_NAME = new ClinicalQueryParam(CVE_PHENOTYPE_NAME_NAME, TEXT_ARRAY,
            CVE_PHENOTYPE_NAME_DESCR);

    // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_GENE_NAME_NAME = "cveGeneName";
    public static final String CVE_GENE_NAME_DESCR = "Clinical variant evidence gene name (or names" + OPT_LIST;
    public static final ClinicalQueryParam CVE_GENE_NAME = new ClinicalQueryParam(CVE_GENE_NAME_NAME, TEXT_ARRAY, CVE_GENE_NAME_DESCR);

    // <field name="consequenceTypeIds" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CVE_CONSEQUENCE_TYPE_ID_NAME = "cveConsequenceTypeId";
    public static final String CVE_CONSEQUENCE_TYPE_ID_DESCR = "Clinical variant evidence consequence type ID (or IDs" + OPT_LIST;
    public static final ClinicalQueryParam CVE_CONSEQUENCE_TYPE_ID = new ClinicalQueryParam(CVE_CONSEQUENCE_TYPE_ID_NAME, TEXT_ARRAY,
            CVE_CONSEQUENCE_TYPE_ID_DESCR);

    // <field name="xrefIds" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CVE_XREF_ID_NAME = "cveXrefId";
    public static final String CVE_XREF_ID_DESCR = "Clinical variant evidence Xref ID (or IDs" + OPT_LIST;
    public static final ClinicalQueryParam CVE_XREF_ID = new ClinicalQueryParam(CVE_XREF_ID_NAME, TEXT_ARRAY, CVE_XREF_ID_DESCR);

    // <field name="panelId" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_PANEL_ID_NAME = "cvePanelId";
    public static final String CVE_PANEL_ID_DESCR = "Clinical variant evidence panel ID (or IDs" + OPT_LIST;
    public static final ClinicalQueryParam CVE_PANEL_ID = new ClinicalQueryParam(CVE_PANEL_ID_NAME, TEXT_ARRAY, CVE_PANEL_ID_DESCR);

    // <field name="acmgs" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CVE_ACGM_NAME = "cveAcmg";
    public static final String CVE_ACGM_DESCR = "Clinical variant evidence ACMG (or ACGMs" + OPT_LIST;
    public static final ClinicalQueryParam CVE_ACGM = new ClinicalQueryParam(CVE_ACGM_NAME, TEXT_ARRAY, CVE_ACGM_DESCR);

    // <field name="tier" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_TIER_NAME = "cveTier";
    public static final String CVE_TIER_DESCR = "Clinical variant evidence tier (or list of tier values" + OPT_LIST;
    public static final ClinicalQueryParam CVE_TIER = new ClinicalQueryParam(CVE_TIER_NAME, TEXT_ARRAY, CVE_TIER_DESCR);

    // <field name="clinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_CLINICAL_SIGNIFICANCE_NAME = "cveClinicalSignificance";
    public static final String CVE_CLINICAL_SIGNIFICANCE_DESCR = "Clinical variant evidence clinical significance (or list of clinical "
        + " significances" + OPT_LIST;
    public static final ClinicalQueryParam CVE_CLINICAL_SIGNIFICANCE = new ClinicalQueryParam(CVE_CLINICAL_SIGNIFICANCE_NAME, TEXT_ARRAY,
            CVE_CLINICAL_SIGNIFICANCE_DESCR);

    // <field name="drugResponse" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_DRUG_RESPONSE_NAME = "cveDrugResponse";
    public static final String CVE_DRUG_RESPONSE_DESCR = "Clinical variant evidence drug response (or list of drug responses"
            + OPT_LIST;
    public static final ClinicalQueryParam CVE_DRUG_RESPONSE = new ClinicalQueryParam(CVE_DRUG_RESPONSE_NAME, TEXT_ARRAY,
            CVE_DRUG_RESPONSE_DESCR);

    // <field name="traitAssociation" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_TRAIT_ASSOCIATION_NAME = "cveTraitAssociation";
    public static final String CVE_TRAIT_ASSOCIATION_DESCR = "Clinical variant evidence trait association (or list of traits" + OPT_LIST;
    public static final ClinicalQueryParam CVE_TRAIT_ASSOCIATION = new ClinicalQueryParam(CVE_TRAIT_ASSOCIATION_NAME, TEXT_ARRAY,
            CVE_TRAIT_ASSOCIATION_DESCR);

    // <field name="functionalEffect" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_FUNCTIONAL_EFFECT_NAME = "cveFunctionalEffect";
    public static final String CVE_FUNCTIONAL_EFFECT_DESCR = "Clinical variant evidence functional effect (or list of functional effects"
            + OPT_LIST;
    public static final ClinicalQueryParam CVE_FUNCTIONAL_EFFECT = new ClinicalQueryParam(CVE_FUNCTIONAL_EFFECT_NAME, TEXT_ARRAY,
            CVE_FUNCTIONAL_EFFECT_DESCR);

    // <field name="tumorigenesis" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_TUMORIGENESIS_NAME = "cveTumorigenesis";
    public static final String CVE_TUMORIGENESIS_DESCR = "Clinical variant evidence tumorigenesis (or list of tumorigenesis values"
            + OPT_LIST;
    public static final ClinicalQueryParam CVE_TUMORIGENESIS = new ClinicalQueryParam(CVE_TUMORIGENESIS_NAME, TEXT_ARRAY,
            CVE_TUMORIGENESIS_DESCR);

    // <field name="otherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CVE_OTHER_CLASSIFICATION_NAME = "cveOtherClassification";
    public static final String CVE_OTHER_CLASSIFICATION_DESCR = "Clinical variant evidence other-classification (or list of other "
        + " classification values" + OPT_LIST;
    public static final ClinicalQueryParam CVE_OTHER_CLASSIFICATION = new ClinicalQueryParam(CVE_OTHER_CLASSIFICATION_NAME, TEXT_ARRAY,
            CVE_OTHER_CLASSIFICATION_DESCR);

    // <field name="rolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CVE_ROL_IN_CANCER_NAME = "cveRolInCancer";
    public static final String CVE_ROL_IN_CANCER_DESCR = "Clinical variant evidence rol in cancer (or roles in cancer" + OPT_LIST;
    public static final ClinicalQueryParam CVE_ROL_IN_CANCER = new ClinicalQueryParam(CVE_ROL_IN_CANCER_NAME, TEXT_ARRAY,
            CVE_ROL_IN_CANCER_DESCR);

    // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>

    // Constructor
    private ClinicalQueryParam(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;

        VALUES.add(this);
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return key() + " [" + type() + "] : " + description();
    }

    public static List<ClinicalQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }
}
