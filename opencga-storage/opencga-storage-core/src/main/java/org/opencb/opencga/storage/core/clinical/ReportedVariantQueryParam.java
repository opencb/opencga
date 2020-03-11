/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.clinical;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.STRING;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

public final class ReportedVariantQueryParam implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<ReportedVariantQueryParam> VALUES = new ArrayList<>();
    private static final String ACCEPTS_ALL_NONE = "Accepts '" + ALL + "' and '" + NONE + "'.";
    private static final String ACCEPTS_AND_OR = "Accepts AND (" + AND + ") and OR (" + OR + ") operators.";

    // ---------- Catalog

    public static final String PROJECT_ID_DESCR = "List of project IDs";
    public static final ReportedVariantQueryParam PROJECT_ID = new ReportedVariantQueryParam("projectId", TEXT_ARRAY, PROJECT_ID_DESCR);

    public static final String ASSEMBLY_DESCR = "List of assemblies";
    public static final ReportedVariantQueryParam ASSEMBLY = new ReportedVariantQueryParam("assembly", TEXT_ARRAY, ASSEMBLY_DESCR);

    public static final String STUDY_ID_DESCR = "List of study IDs";
    public static final ReportedVariantQueryParam STUDY_ID = new ReportedVariantQueryParam("studyId", TEXT_ARRAY,
            STUDY_ID_DESCR);

    // ---------- Clinical Analysis (aka CA)

    public static final String CA_ID_DESCR = "List of clinical analysis IDs";
    public static final ReportedVariantQueryParam CA_ID = new ReportedVariantQueryParam("caId", TEXT_ARRAY,
            CA_ID_DESCR);

    public static final String CA_NAME_DESCR = "List of clinical analysis names";
    public static final ReportedVariantQueryParam CA_NAME = new ReportedVariantQueryParam("caName", TEXT_ARRAY,
            CA_NAME_DESCR);

    public static final String CA_DESCRIPTION_DESCR = "Clinical analysis description";
    public static final ReportedVariantQueryParam CA_DESCRIPTION = new ReportedVariantQueryParam("caDescription",
            TEXT_ARRAY, CA_DESCRIPTION_DESCR);

    public static final String CA_DISORDER_DESCR = "List of clinical analysis disorders";
    public static final ReportedVariantQueryParam CA_DISORDER = new ReportedVariantQueryParam("caDisorderId",
            TEXT_ARRAY, CA_DISORDER_DESCR);

    public static final String CA_FILE_DESCR = "List of clinical analysis files";
    public static final ReportedVariantQueryParam CA_FILE = new ReportedVariantQueryParam("caFiles", TEXT_ARRAY,
            CA_FILE_DESCR);

    public static final String CA_PROBAND_ID_DESCR = "List of proband IDs";
    public static final ReportedVariantQueryParam CA_PROBAND_ID = new ReportedVariantQueryParam("caProbandId",
            TEXT_ARRAY, CA_PROBAND_ID_DESCR);

    public static final String CA_PROBAND_DISORDERS_DESCR = "List of proband disorders";
    public static final ReportedVariantQueryParam CA_PROBAND_DISORDERS = new ReportedVariantQueryParam("caProbandDisorders",
            TEXT_ARRAY, CA_PROBAND_DISORDERS_DESCR);

    public static final String CA_PROBAND_PHENOTYPES_DESCR = "List of proband phenotypes";
    public static final ReportedVariantQueryParam CA_PROBAND_PHENOTYPES = new ReportedVariantQueryParam("caProbandPhenotypes",
            TEXT_ARRAY, CA_PROBAND_PHENOTYPES_DESCR);

    public static final String CA_FAMILY_ID_DESCR = "List of family IDs";
    public static final ReportedVariantQueryParam CA_FAMILY_ID = new ReportedVariantQueryParam("caFamilyId", TEXT_ARRAY,
            CA_FAMILY_ID_DESCR);

    public static final String CA_FAMILY_MEMBER_IDS_DESCR = "List of clinical analysis family member IDs";
    public static final ReportedVariantQueryParam CA_FAMILY_MEMBER_IDS = new ReportedVariantQueryParam("caFamilyMemberIds", TEXT_ARRAY,
            CA_FAMILY_MEMBER_IDS_DESCR);

    public static final String CA_COMMENTS_DESCR = "List of clinical analysis comments";
    public static final ReportedVariantQueryParam CA_COMMENTS = new ReportedVariantQueryParam("caComments", TEXT_ARRAY,
            CA_COMMENTS_DESCR);

    public static final String CA_INFO_DESCR = "";
    public static final ReportedVariantQueryParam CA_INFO = new ReportedVariantQueryParam("caInfo", TEXT_ARRAY, CA_INFO_DESCR);

    // ---------- Interpretation (aka INT)

    public static final String INT_ID_DESCR = "List of interpretation IDs";
    public static final ReportedVariantQueryParam INT_ID = new ReportedVariantQueryParam("intId", TEXT_ARRAY, INT_ID_DESCR);

    public static final String INT_SOFTWARE_NAME_DESCR = "List of interpretation software names";
    public static final ReportedVariantQueryParam INT_SOFTWARE_NAME = new ReportedVariantQueryParam("intSoftwareName", TEXT_ARRAY,
            INT_SOFTWARE_NAME_DESCR);

    public static final String INT_SOFTWARE_VERSION_DESCR = "List of interpretation software versions";
    public static final ReportedVariantQueryParam INT_SOFTWARE_VERSION = new ReportedVariantQueryParam("intSoftwareVersion", TEXT_ARRAY,
            INT_SOFTWARE_VERSION_DESCR);

    public static final String INT_ANALYST_NAME_DESCR = "List of interpretation analysist names";
    public static final ReportedVariantQueryParam INT_ANALYST_NAME = new ReportedVariantQueryParam("intAnalystName", TEXT_ARRAY,
            INT_ANALYST_NAME_DESCR);

    public static final String INT_PANELS_DESCR = "List of interpretation panels";
    public static final ReportedVariantQueryParam INT_PANELS = new ReportedVariantQueryParam("intPanels", TEXT_ARRAY,
            INT_PANELS_DESCR);

    public static final String INT_INFO_DESCR = "";
    public static final ReportedVariantQueryParam INT_INFO = new ReportedVariantQueryParam("intInfo", TEXT_ARRAY, INT_INFO_DESCR);

    public static final String INT_DESCRIPTION_DESCR = "Interpretation description";
    public static final ReportedVariantQueryParam INT_DESCRIPTION = new ReportedVariantQueryParam("intDescription", TEXT_ARRAY,
            INT_DESCRIPTION_DESCR);

    public static final String INT_DEPENDENCY_DESCR = "List of interpretation dependency, format: name:version, e.g. cellbase:4.0";
    public static final ReportedVariantQueryParam INT_DEPENDENCY = new ReportedVariantQueryParam("intDependency", TEXT_ARRAY,
            INT_DEPENDENCY_DESCR);

    public static final String INT_FILTERS_DESCR = "List of interpretation filters";
    public static final ReportedVariantQueryParam INT_FILTERS = new ReportedVariantQueryParam("intFilters", TEXT_ARRAY,
            INT_FILTERS_DESCR);

    public static final String INT_COMMENTS_DESCR = "List of interpretation comments";
    public static final ReportedVariantQueryParam INT_COMMENTS = new ReportedVariantQueryParam("intComments", TEXT_ARRAY,
            INT_COMMENTS_DESCR);

    public static final String INT_CREATION_DATE_DESCR = "Iinterpretation creation date (including date ranges)";
    public static final ReportedVariantQueryParam INT_CREATION_DATE = new ReportedVariantQueryParam("intCreationDate", STRING,
            INT_CREATION_DATE_DESCR);

    // ---------- Reported variant (aka RV)

    public static final String RV_DE_NOVO_QUALITY_SCORE_DESCR = "List of reported variant de novo quality scores";
    public static final ReportedVariantQueryParam RV_DE_NOVO_QUALITY_SCORE = new ReportedVariantQueryParam("rvDeNovoQualityScore",
            TEXT_ARRAY, RV_DE_NOVO_QUALITY_SCORE_DESCR);

    public static final String RV_COMMENTS_DESCR = "List of reported variant comments";
    public static final ReportedVariantQueryParam RV_COMMENTS = new ReportedVariantQueryParam("rvComments", TEXT_ARRAY,
            RV_COMMENTS_DESCR);

    // ---------- Reported event (aka RE)

    public static final String RE_PHENOTYPE_NAMES_DESCR = "List of reported event phenotype names";
    public static final ReportedVariantQueryParam RE_PHENOTYPE_NAMES = new ReportedVariantQueryParam("rePhenotypeNames", TEXT_ARRAY,
            RE_PHENOTYPE_NAMES_DESCR);

    public static final String RE_CONSEQUENCE_TYPE_IDS_DESCR = "List of reported event consequence type IDs";
    public static final ReportedVariantQueryParam RE_CONSEQUENCE_TYPE_IDS = new ReportedVariantQueryParam("reConsequenceTypeIds",
            TEXT_ARRAY, RE_CONSEQUENCE_TYPE_IDS_DESCR);

    public static final String RE_GENE_NAMES_DESCR = "List of reported event gene names";
    public static final ReportedVariantQueryParam RE_GENE_NAMES = new ReportedVariantQueryParam("reGeneNames", TEXT_ARRAY,
            RE_GENE_NAMES_DESCR);

    public static final String RE_XREFS_DESCR = "List of reported event phenotype xRefs";
    public static final ReportedVariantQueryParam RE_XREFS = new ReportedVariantQueryParam("reXrefs", TEXT_ARRAY, RE_XREFS_DESCR);

    public static final String RE_PANEL_IDS_DESCR = "List of reported event panel IDs";
    public static final ReportedVariantQueryParam RE_PANEL_IDS = new ReportedVariantQueryParam("rePanelNames", TEXT_ARRAY,
            RE_PANEL_IDS_DESCR);

    public static final String RE_ACMG_DESCR = "List of reported event ACMG";
    public static final ReportedVariantQueryParam RE_ACMG = new ReportedVariantQueryParam("reAcmg", TEXT_ARRAY, RE_ACMG_DESCR);

    public static final String RE_CLINICAL_SIGNIFICANCE_DESCR = "List of reported event clinical significance";
    public static final ReportedVariantQueryParam RE_CLINICAL_SIGNIFICANCE = new ReportedVariantQueryParam("reClinicalSignificance",
            TEXT_ARRAY, RE_CLINICAL_SIGNIFICANCE_DESCR);

    public static final String RE_DRUG_RESPONSE_DESCR = "List of reported event drug response";
    public static final ReportedVariantQueryParam RE_DRUG_RESPONSE = new ReportedVariantQueryParam("reDrugResponse", TEXT_ARRAY,
            RE_DRUG_RESPONSE_DESCR);

    public static final String RE_TRAIT_ASSOCIATION_DESCR = "List of reported event trait association";
    public static final ReportedVariantQueryParam RE_TRAIT_ASSOCIATION = new ReportedVariantQueryParam("reTraitAssociation", TEXT_ARRAY,
            RE_TRAIT_ASSOCIATION_DESCR);

    public static final String RE_FUNCTIONAL_EFFECT_DESCR = "List of reported event functional effect";
    public static final ReportedVariantQueryParam RE_FUNCTIONAL_EFFECT = new ReportedVariantQueryParam("reFunctionalEffect", TEXT_ARRAY,
            RE_FUNCTIONAL_EFFECT_DESCR);

    public static final String RE_TUMORIGENESIS_DESCR = "List of reported event tumorigenesis";
    public static final ReportedVariantQueryParam RE_TUMORIGENESIS = new ReportedVariantQueryParam("reTumorigenesis", TEXT_ARRAY,
            RE_TUMORIGENESIS_DESCR);

    public static final String RE_OTHER_CLASSIFICATION_DESCR = "List of reported event other classification";
    public static final ReportedVariantQueryParam RE_OTHER_CLASSIFICATION = new ReportedVariantQueryParam("reOtherClassification",
            TEXT_ARRAY, RE_OTHER_CLASSIFICATION_DESCR);

    public static final String RE_ROLES_IN_CANCER_DESCR = "List of reported event roles in cancer";
    public static final ReportedVariantQueryParam RE_ROLES_IN_CANCER = new ReportedVariantQueryParam("reRolesInCancer", TEXT_ARRAY,
            RE_ROLES_IN_CANCER_DESCR);

    public static final String RE_TIER_DESCR = "List of reported event tier";
    public static final ReportedVariantQueryParam RE_TIER = new ReportedVariantQueryParam("reTier", TEXT_ARRAY, RE_TIER_DESCR);

    public static final String RE_JUSTIFICATION_DESCR = "List of reported event justification";
    public static final ReportedVariantQueryParam RE_JUSTIFICATION = new ReportedVariantQueryParam("reJustification", TEXT_ARRAY,
            RE_JUSTIFICATION_DESCR);

    public static final String RE_AUX_DESCR = "";
    public static final ReportedVariantQueryParam RE_AUX = new ReportedVariantQueryParam("reAux", TEXT_ARRAY, RE_AUX_DESCR);

    // Constructor
    private ReportedVariantQueryParam(String key, Type type, String description) {
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

    public static List<ReportedVariantQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }
}
