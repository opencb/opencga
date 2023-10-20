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

//    public static final String RV_DE_NOVO_QUALITY_SCORE_DESCR = "List of reported variant de novo quality scores";
//    public static final ClinicalAnalysisQueryParam RV_DE_NOVO_QUALITY_SCORE = new ClinicalAnalysisQueryParam("rvDeNovoQualityScore",
//            TEXT_ARRAY, RV_DE_NOVO_QUALITY_SCORE_DESCR);
//
//    public static final String RV_COMMENTS_DESCR = "List of reported variant comments";
//    public static final ClinicalAnalysisQueryParam RV_COMMENTS = new ClinicalAnalysisQueryParam("rvComments", TEXT_ARRAY,
//            RV_COMMENTS_DESCR);

    // ---------- Reported event (aka RE)

//    public static final String RE_PHENOTYPE_NAMES_DESCR = "List of reported event phenotype names";
//    public static final ClinicalAnalysisQueryParam RE_PHENOTYPE_NAMES = new ClinicalAnalysisQueryParam("rePhenotypeNames", TEXT_ARRAY,
//            RE_PHENOTYPE_NAMES_DESCR);
//
//    public static final String RE_CONSEQUENCE_TYPE_IDS_DESCR = "List of reported event consequence type IDs";
//    public static final ClinicalAnalysisQueryParam RE_CONSEQUENCE_TYPE_IDS = new ClinicalAnalysisQueryParam("reConsequenceTypeIds",
//            TEXT_ARRAY, RE_CONSEQUENCE_TYPE_IDS_DESCR);
//
//    public static final String RE_GENE_NAMES_DESCR = "List of reported event gene names";
//    public static final ClinicalAnalysisQueryParam RE_GENE_NAMES = new ClinicalAnalysisQueryParam("reGeneNames", TEXT_ARRAY,
//            RE_GENE_NAMES_DESCR);
//
//    public static final String RE_XREFS_DESCR = "List of reported event phenotype xRefs";
//    public static final ClinicalAnalysisQueryParam RE_XREFS = new ClinicalAnalysisQueryParam("reXrefs", TEXT_ARRAY, RE_XREFS_DESCR);
//
//    public static final String RE_PANEL_NAMES_DESCR = "List of reported event panel names";
//    public static final ClinicalAnalysisQueryParam RE_PANEL_NAMES = new ClinicalAnalysisQueryParam("rePanelNames", TEXT_ARRAY,
//            RE_PANEL_NAMES_DESCR);
//
//    public static final String RE_ACMG_DESCR = "List of reported event ACMG";
//    public static final ClinicalAnalysisQueryParam RE_ACMG = new ClinicalAnalysisQueryParam("reAcmg", TEXT_ARRAY, RE_ACMG_DESCR);
//
//    public static final String RE_CLINICAL_SIGNIFICANCE_DESCR = "List of reported event clinical significance";
//    public static final ClinicalAnalysisQueryParam RE_CLINICAL_SIGNIFICANCE = new ClinicalAnalysisQueryParam("reClinicalSignificance",
//            TEXT_ARRAY, RE_CLINICAL_SIGNIFICANCE_DESCR);
//
//    public static final String RE_DRUG_RESPONSE_DESCR = "List of reported event drug response";
//    public static final ClinicalAnalysisQueryParam RE_DRUG_RESPONSE = new ClinicalAnalysisQueryParam("reDrugResponse", TEXT_ARRAY,
//            RE_DRUG_RESPONSE_DESCR);
//
//    public static final String RE_TRAIT_ASSOCIATION_DESCR = "List of reported event trait association";
//    public static final ClinicalAnalysisQueryParam RE_TRAIT_ASSOCIATION = new ClinicalAnalysisQueryParam("reTraitAssociation", TEXT_ARRAY,
//            RE_TRAIT_ASSOCIATION_DESCR);
//
//    public static final String RE_FUNCTIONAL_EFFECT_DESCR = "List of reported event functional effect";
//    public static final ClinicalAnalysisQueryParam RE_FUNCTIONAL_EFFECT = new ClinicalAnalysisQueryParam("reFunctionalEffect", TEXT_ARRAY,
//            RE_FUNCTIONAL_EFFECT_DESCR);
//
//    public static final String RE_TUMORIGENESIS_DESCR = "List of reported event tumorigenesis";
//    public static final ClinicalAnalysisQueryParam RE_TUMORIGENESIS = new ClinicalAnalysisQueryParam("reTumorigenesis", TEXT_ARRAY,
//            RE_TUMORIGENESIS_DESCR);
//
//    public static final String RE_OTHER_CLASSIFICATION_DESCR = "List of reported event other classification";
//    public static final ClinicalAnalysisQueryParam RE_OTHER_CLASSIFICATION = new ClinicalAnalysisQueryParam("reOtherClassification",
//            TEXT_ARRAY, RE_OTHER_CLASSIFICATION_DESCR);
//
//    public static final String RE_ROLES_IN_CANCER_DESCR = "List of reported event roles in cancer";
//    public static final ClinicalAnalysisQueryParam RE_ROLES_IN_CANCER = new ClinicalAnalysisQueryParam("reRolesInCancer", TEXT_ARRAY,
//            RE_ROLES_IN_CANCER_DESCR);
//
//    public static final String RE_SCORE_DESCR = "List of reported event scores";
//    public static final ClinicalAnalysisQueryParam RE_SCORE = new ClinicalAnalysisQueryParam("reScore", TEXT_ARRAY, RE_SCORE_DESCR);

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
