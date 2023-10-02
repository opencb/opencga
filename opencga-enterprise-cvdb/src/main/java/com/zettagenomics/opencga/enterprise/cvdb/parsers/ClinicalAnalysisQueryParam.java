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

public final class ClinicalAnalysisQueryParam implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<ClinicalAnalysisQueryParam> VALUES = new ArrayList<>();
    private static final String ACCEPTS_ALL_NONE = "Accepts '" + ALL + "' and '" + NONE + "'.";
    private static final String ACCEPTS_AND_OR = "Accepts AND (" + AND + ") and OR (" + OR + ") operators.";

    public static final String PROJECT_ID_DESCR = ParamConstants.PROJECT_PARAM_DESCRIPTION;
    public static final ClinicalAnalysisQueryParam PROJECT_ID = new ClinicalAnalysisQueryParam(ParamConstants.PROJECT_PARAM_NAME,
            STRING, PROJECT_ID_DESCR);

    // ---------- Clinical analysis (aka CA)

//    public static final String CA_ID_DESCR = "List of clinical analysis IDs";
//    public static final ClinicalAnalysisQueryParam CA_ID = new ClinicalAnalysisQueryParam("caId", TEXT_ARRAY,
//            CA_ID_DESCR);
//
//    public static final String CA_DESCRIPTION_DESCR = "Clinical analysis description";
//    public static final ClinicalAnalysisQueryParam CA_DESCRIPTION = new ClinicalAnalysisQueryParam("caDescription", TEXT_ARRAY,
//            CA_DESCRIPTION_DESCR);
//
//    public static final String CA_TYPE_DESCR = "List of clinical analysis types";
//    public static final ClinicalAnalysisQueryParam CA_TYPE = new ClinicalAnalysisQueryParam("caType", TEXT_ARRAY, CA_TYPE_DESCR);
//
//    public static final String CA_DISORDER_ID_DESCR = "List of clinical analysis disorder IDs";
//    public static final ClinicalAnalysisQueryParam CA_DISORDER_ID = new ClinicalAnalysisQueryParam("caDisorderId", TEXT_ARRAY,
//            CA_DISORDER_ID_DESCR);
//
//    public static final String CA_FILENAME_DESCR = "List of clinical analysis file names";
//    public static final ClinicalAnalysisQueryParam CA_FILENAME = new ClinicalAnalysisQueryParam("caFilename", TEXT_ARRAY, CA_FILENAME_DESCR);
//
//    public static final String CA_PROBAND_ID_DESCR = "List of clinical analysis proband IDs";
//    public static final ClinicalAnalysisQueryParam CA_PROBAND_ID = new ClinicalAnalysisQueryParam("caProbandId", TEXT_ARRAY,
//            CA_PROBAND_ID_DESCR);
//
//    public static final String CA_FAMILY_ID_DESCR = "List of clinical analysis family IDs";
//    public static final ClinicalAnalysisQueryParam CA_FAMILY_ID = new ClinicalAnalysisQueryParam("caFamilyId", TEXT_ARRAY,
//            CA_FAMILY_ID_DESCR);
//
//    public static final String CA_FAMILY_PHENOTYPE_NAME_DESCR = "List of clinical analysis family phenotype names";
//    public static final ClinicalAnalysisQueryParam CA_FAMILY_PHENOTYPE_NAME = new ClinicalAnalysisQueryParam("caFamilyPhenotypeName",
//            TEXT_ARRAY, CA_FAMILY_PHENOTYPE_NAME_DESCR);
//
//    public static final String CA_FAMILY_MEMBER_ID_DESCR = "List of clinical analysis family member IDs";
//    public static final ClinicalAnalysisQueryParam CA_FAMILY_MEMBER_ID = new ClinicalAnalysisQueryParam("caFamilyMemberId", TEXT_ARRAY,
//            CA_FAMILY_MEMBER_ID_DESCR);
//
//    public static final String CA_REPORT_DESCR = "List of clinical analysis reports";
//    public static final ClinicalAnalysisQueryParam CA_REPORT = new ClinicalAnalysisQueryParam("caReport", TEXT_ARRAY, CA_REPORT_DESCR);
//
//    public static final String CA_STATUS_DESCR = "List of clinical analysis status";
//    public static final ClinicalAnalysisQueryParam CA_STATUS = new ClinicalAnalysisQueryParam("caStatus", TEXT_ARRAY, CA_STATUS_DESCR);
//
//    public static final String CA_LOCK_DESCR = "Filter by (un)locked clinical analyses";
//    public static final ClinicalAnalysisQueryParam CA_LOCK = new ClinicalAnalysisQueryParam("caLock", BOOLEAN, CA_LOCK_DESCR);

    // ---------- Clinical interpretation (aka CI)

//    public static final String INT_ID_DESCR = "List of interpretation IDs";
//    public static final ClinicalAnalysisQueryParam INT_ID = new ClinicalAnalysisQueryParam("intId", TEXT_ARRAY, INT_ID_DESCR);
//
//    public static final String INT_SOFTWARE_NAME_DESCR = "List of interpretation software names";
//    public static final ClinicalAnalysisQueryParam INT_SOFTWARE_NAME = new ClinicalAnalysisQueryParam("intSoftwareName", TEXT_ARRAY,
//            INT_SOFTWARE_NAME_DESCR);
//
//    public static final String INT_SOFTWARE_VERSION_DESCR = "List of interpretation software versions";
//    public static final ClinicalAnalysisQueryParam INT_SOFTWARE_VERSION = new ClinicalAnalysisQueryParam("intSoftwareVersion", TEXT_ARRAY,
//            INT_SOFTWARE_VERSION_DESCR);
//
//    public static final String INT_ANALYST_NAME_DESCR = "List of interpretation analysist names";
//    public static final ClinicalAnalysisQueryParam INT_ANALYST_NAME = new ClinicalAnalysisQueryParam("intAnalystName", TEXT_ARRAY,
//            INT_ANALYST_NAME_DESCR);
//
//    public static final String INT_PANEL_NAMES_DESCR = "List of interpretation panel names";
//    public static final ClinicalAnalysisQueryParam INT_PANEL_NAMES = new ClinicalAnalysisQueryParam("intPanelNames", TEXT_ARRAY,
//            INT_PANEL_NAMES_DESCR);
//
//    public static final String INT_DESCRIPTION_DESCR = "Interpretation description";
//    public static final ClinicalAnalysisQueryParam INT_DESCRIPTION = new ClinicalAnalysisQueryParam("intDescription", TEXT_ARRAY,
//            INT_DESCRIPTION_DESCR);
//
//    public static final String INT_DEPENDENCY_NAME_DESCR = "List of interpretation dependency names";
//    public static final ClinicalAnalysisQueryParam INT_DEPENDENCY_NAME = new ClinicalAnalysisQueryParam("intDependencyName", TEXT_ARRAY,
//            INT_DEPENDENCY_NAME_DESCR);
//
//    public static final String INT_DEPENDENCY_VERSION_DESCR = "List of interpretation dependency versions";
//    public static final ClinicalAnalysisQueryParam INT_DEPENDENCY_VERSION = new ClinicalAnalysisQueryParam("intDependencyVersion", TEXT_ARRAY,
//            INT_DEPENDENCY_VERSION_DESCR);
//
//    public static final String INT_COMMENTS_DESCR = "List of interpretation comments";
//    public static final ClinicalAnalysisQueryParam INT_COMMENTS = new ClinicalAnalysisQueryParam("intComments", TEXT_ARRAY,
//            INT_COMMENTS_DESCR);
//
    // TODO: intrepretation creation date

    // ---------- Clinical variant (aka CV)

    public static final String CV_VARIANT_ID_DESCR = ParamConstants.VARIANT_QUERY_DESCRIPTION;
    public static final ClinicalAnalysisQueryParam CV_VARIANT_ID = new ClinicalAnalysisQueryParam(ParamConstants.VARIANT_QUERY_PARAM,
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
    private ClinicalAnalysisQueryParam(String key, Type type, String description) {
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

    public static List<ClinicalAnalysisQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }
}
