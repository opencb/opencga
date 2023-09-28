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

import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.BOOLEAN;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

public final class ClinicalVariantQueryParam implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<ClinicalVariantQueryParam> VALUES = new ArrayList<>();
    private static final String ACCEPTS_ALL_NONE = "Accepts '" + ALL + "' and '" + NONE + "'.";
    private static final String ACCEPTS_AND_OR = "Accepts AND (" + AND + ") and OR (" + OR + ") operators.";

    // ---------- Clinical analysis (aka CA)

    public static final String CA_ID_DESCR = "List of clinical analysis IDs";
    public static final ClinicalVariantQueryParam CA_ID = new ClinicalVariantQueryParam("caId", TEXT_ARRAY,
            CA_ID_DESCR);

    public static final String CA_DESCRIPTION_DESCR = "Clinical analysis description";
    public static final ClinicalVariantQueryParam CA_DESCRIPTION = new ClinicalVariantQueryParam("caDescription", TEXT_ARRAY,
            CA_DESCRIPTION_DESCR);

    public static final String CA_TYPE_DESCR = "List of clinical analysis types";
    public static final ClinicalVariantQueryParam CA_TYPE = new ClinicalVariantQueryParam("caType", TEXT_ARRAY, CA_TYPE_DESCR);

    public static final String CA_DISORDER_ID_DESCR = "List of clinical analysis disorder IDs";
    public static final ClinicalVariantQueryParam CA_DISORDER_ID = new ClinicalVariantQueryParam("caDisorderId", TEXT_ARRAY,
            CA_DISORDER_ID_DESCR);

    public static final String CA_FILENAME_DESCR = "List of clinical analysis file names";
    public static final ClinicalVariantQueryParam CA_FILENAME = new ClinicalVariantQueryParam("caFilename", TEXT_ARRAY, CA_FILENAME_DESCR);

    public static final String CA_PROBAND_ID_DESCR = "List of clinical analysis proband IDs";
    public static final ClinicalVariantQueryParam CA_PROBAND_ID = new ClinicalVariantQueryParam("caProbandId", TEXT_ARRAY,
            CA_PROBAND_ID_DESCR);

    public static final String CA_FAMILY_ID_DESCR = "List of clinical analysis family IDs";
    public static final ClinicalVariantQueryParam CA_FAMILY_ID = new ClinicalVariantQueryParam("caFamilyId", TEXT_ARRAY,
            CA_FAMILY_ID_DESCR);

    public static final String CA_FAMILY_PHENOTYPE_NAME_DESCR = "List of clinical analysis family phenotype names";
    public static final ClinicalVariantQueryParam CA_FAMILY_PHENOTYPE_NAME = new ClinicalVariantQueryParam("caFamilyPhenotypeName",
            TEXT_ARRAY, CA_FAMILY_PHENOTYPE_NAME_DESCR);

    public static final String CA_FAMILY_MEMBER_ID_DESCR = "List of clinical analysis family member IDs";
    public static final ClinicalVariantQueryParam CA_FAMILY_MEMBER_ID = new ClinicalVariantQueryParam("caFamilyMemberId", TEXT_ARRAY,
            CA_FAMILY_MEMBER_ID_DESCR);

    public static final String CA_REPORT_DESCR = "List of clinical analysis reports";
    public static final ClinicalVariantQueryParam CA_REPORT = new ClinicalVariantQueryParam("caReport", TEXT_ARRAY, CA_REPORT_DESCR);

    public static final String CA_STATUS_DESCR = "List of clinical analysis status";
    public static final ClinicalVariantQueryParam CA_STATUS = new ClinicalVariantQueryParam("caStatus", TEXT_ARRAY, CA_STATUS_DESCR);

    public static final String CA_LOCK_DESCR = "Filter by (un)locked clinical analyses";
    public static final ClinicalVariantQueryParam CA_LOCK = new ClinicalVariantQueryParam("caLock", BOOLEAN, CA_LOCK_DESCR);

    // ---------- Clinical interpretation (aka CI)

    public static final String INT_ID_DESCR = "List of interpretation IDs";
    public static final ClinicalVariantQueryParam INT_ID = new ClinicalVariantQueryParam("intId", TEXT_ARRAY, INT_ID_DESCR);

    public static final String INT_SOFTWARE_NAME_DESCR = "List of interpretation software names";
    public static final ClinicalVariantQueryParam INT_SOFTWARE_NAME = new ClinicalVariantQueryParam("intSoftwareName", TEXT_ARRAY,
            INT_SOFTWARE_NAME_DESCR);

    public static final String INT_SOFTWARE_VERSION_DESCR = "List of interpretation software versions";
    public static final ClinicalVariantQueryParam INT_SOFTWARE_VERSION = new ClinicalVariantQueryParam("intSoftwareVersion", TEXT_ARRAY,
            INT_SOFTWARE_VERSION_DESCR);

    public static final String INT_ANALYST_NAME_DESCR = "List of interpretation analysist names";
    public static final ClinicalVariantQueryParam INT_ANALYST_NAME = new ClinicalVariantQueryParam("intAnalystName", TEXT_ARRAY,
            INT_ANALYST_NAME_DESCR);

    public static final String INT_PANEL_NAMES_DESCR = "List of interpretation panel names";
    public static final ClinicalVariantQueryParam INT_PANEL_NAMES = new ClinicalVariantQueryParam("intPanelNames", TEXT_ARRAY,
            INT_PANEL_NAMES_DESCR);

    public static final String INT_DESCRIPTION_DESCR = "Interpretation description";
    public static final ClinicalVariantQueryParam INT_DESCRIPTION = new ClinicalVariantQueryParam("intDescription", TEXT_ARRAY,
            INT_DESCRIPTION_DESCR);

    public static final String INT_DEPENDENCY_NAME_DESCR = "List of interpretation dependency names";
    public static final ClinicalVariantQueryParam INT_DEPENDENCY_NAME = new ClinicalVariantQueryParam("intDependencyName", TEXT_ARRAY,
            INT_DEPENDENCY_NAME_DESCR);

    public static final String INT_DEPENDENCY_VERSION_DESCR = "List of interpretation dependency versions";
    public static final ClinicalVariantQueryParam INT_DEPENDENCY_VERSION = new ClinicalVariantQueryParam("intDependencyVersion", TEXT_ARRAY,
            INT_DEPENDENCY_VERSION_DESCR);

    public static final String INT_COMMENTS_DESCR = "List of interpretation comments";
    public static final ClinicalVariantQueryParam INT_COMMENTS = new ClinicalVariantQueryParam("intComments", TEXT_ARRAY,
            INT_COMMENTS_DESCR);

    // TODO: intrepretation creation date

    // ---------- Reported variant (aka RV)

    public static final String RV_DE_NOVO_QUALITY_SCORE_DESCR = "List of reported variant de novo quality scores";
    public static final ClinicalVariantQueryParam RV_DE_NOVO_QUALITY_SCORE = new ClinicalVariantQueryParam("rvDeNovoQualityScore",
            TEXT_ARRAY, RV_DE_NOVO_QUALITY_SCORE_DESCR);

    public static final String RV_COMMENTS_DESCR = "List of reported variant comments";
    public static final ClinicalVariantQueryParam RV_COMMENTS = new ClinicalVariantQueryParam("rvComments", TEXT_ARRAY,
            RV_COMMENTS_DESCR);

    // ---------- Reported event (aka RE)

    public static final String RE_PHENOTYPE_NAMES_DESCR = "List of reported event phenotype names";
    public static final ClinicalVariantQueryParam RE_PHENOTYPE_NAMES = new ClinicalVariantQueryParam("rePhenotypeNames", TEXT_ARRAY,
            RE_PHENOTYPE_NAMES_DESCR);

    public static final String RE_CONSEQUENCE_TYPE_IDS_DESCR = "List of reported event consequence type IDs";
    public static final ClinicalVariantQueryParam RE_CONSEQUENCE_TYPE_IDS = new ClinicalVariantQueryParam("reConsequenceTypeIds",
            TEXT_ARRAY, RE_CONSEQUENCE_TYPE_IDS_DESCR);

    public static final String RE_GENE_NAMES_DESCR = "List of reported event gene names";
    public static final ClinicalVariantQueryParam RE_GENE_NAMES = new ClinicalVariantQueryParam("reGeneNames", TEXT_ARRAY,
            RE_GENE_NAMES_DESCR);

    public static final String RE_XREFS_DESCR = "List of reported event phenotype xRefs";
    public static final ClinicalVariantQueryParam RE_XREFS = new ClinicalVariantQueryParam("reXrefs", TEXT_ARRAY, RE_XREFS_DESCR);

    public static final String RE_PANEL_NAMES_DESCR = "List of reported event panel names";
    public static final ClinicalVariantQueryParam RE_PANEL_NAMES = new ClinicalVariantQueryParam("rePanelNames", TEXT_ARRAY,
            RE_PANEL_NAMES_DESCR);

    public static final String RE_ACMG_DESCR = "List of reported event ACMG";
    public static final ClinicalVariantQueryParam RE_ACMG = new ClinicalVariantQueryParam("reAcmg", TEXT_ARRAY, RE_ACMG_DESCR);

    public static final String RE_CLINICAL_SIGNIFICANCE_DESCR = "List of reported event clinical significance";
    public static final ClinicalVariantQueryParam RE_CLINICAL_SIGNIFICANCE = new ClinicalVariantQueryParam("reClinicalSignificance",
            TEXT_ARRAY, RE_CLINICAL_SIGNIFICANCE_DESCR);

    public static final String RE_DRUG_RESPONSE_DESCR = "List of reported event drug response";
    public static final ClinicalVariantQueryParam RE_DRUG_RESPONSE = new ClinicalVariantQueryParam("reDrugResponse", TEXT_ARRAY,
            RE_DRUG_RESPONSE_DESCR);

    public static final String RE_TRAIT_ASSOCIATION_DESCR = "List of reported event trait association";
    public static final ClinicalVariantQueryParam RE_TRAIT_ASSOCIATION = new ClinicalVariantQueryParam("reTraitAssociation", TEXT_ARRAY,
            RE_TRAIT_ASSOCIATION_DESCR);

    public static final String RE_FUNCTIONAL_EFFECT_DESCR = "List of reported event functional effect";
    public static final ClinicalVariantQueryParam RE_FUNCTIONAL_EFFECT = new ClinicalVariantQueryParam("reFunctionalEffect", TEXT_ARRAY,
            RE_FUNCTIONAL_EFFECT_DESCR);

    public static final String RE_TUMORIGENESIS_DESCR = "List of reported event tumorigenesis";
    public static final ClinicalVariantQueryParam RE_TUMORIGENESIS = new ClinicalVariantQueryParam("reTumorigenesis", TEXT_ARRAY,
            RE_TUMORIGENESIS_DESCR);

    public static final String RE_OTHER_CLASSIFICATION_DESCR = "List of reported event other classification";
    public static final ClinicalVariantQueryParam RE_OTHER_CLASSIFICATION = new ClinicalVariantQueryParam("reOtherClassification",
            TEXT_ARRAY, RE_OTHER_CLASSIFICATION_DESCR);

    public static final String RE_ROLES_IN_CANCER_DESCR = "List of reported event roles in cancer";
    public static final ClinicalVariantQueryParam RE_ROLES_IN_CANCER = new ClinicalVariantQueryParam("reRolesInCancer", TEXT_ARRAY,
            RE_ROLES_IN_CANCER_DESCR);

    public static final String RE_SCORE_DESCR = "List of reported event scores";
    public static final ClinicalVariantQueryParam RE_SCORE = new ClinicalVariantQueryParam("reScore", TEXT_ARRAY, RE_SCORE_DESCR);

    // Constructor
    private ClinicalVariantQueryParam(String key, Type type, String description) {
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

    public static List<ClinicalVariantQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }
}
