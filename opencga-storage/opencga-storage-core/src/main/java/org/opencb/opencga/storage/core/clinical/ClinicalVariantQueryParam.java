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

public final class ClinicalVariantQueryParam implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<ClinicalVariantQueryParam> VALUES = new ArrayList<>();
    private static final String ACCEPTS_ALL_NONE = "Accepts '" + ALL + "' and '" + NONE + "'.";
    private static final String ACCEPTS_AND_OR = "Accepts AND (" + AND + ") and OR (" + OR + ") operators.";

    // ---------- Catalog

    public static final String PROJECT_ID_DESCR = "List of project IDs";
    public static final ClinicalVariantQueryParam PROJECT_ID = new ClinicalVariantQueryParam("projectId", TEXT_ARRAY, PROJECT_ID_DESCR);

    public static final String ASSEMBLY_DESCR = "List of assemblies";
    public static final ClinicalVariantQueryParam ASSEMBLY = new ClinicalVariantQueryParam("assembly", TEXT_ARRAY, ASSEMBLY_DESCR);

    public static final String STUDY_ID_DESCR = "List of study IDs";
    public static final ClinicalVariantQueryParam STUDY_ID = new ClinicalVariantQueryParam("studyId", TEXT_ARRAY,
            STUDY_ID_DESCR);

    // ---------- Clinical Analysis (aka CA)

    public static final String CA_ID_DESCR = "List of search analysis IDs";
    public static final ClinicalVariantQueryParam CA_ID = new ClinicalVariantQueryParam("caId", TEXT_ARRAY,
            CA_ID_DESCR);

    public static final String CA_NAME_DESCR = "List of search analysis names";
    public static final ClinicalVariantQueryParam CA_NAME = new ClinicalVariantQueryParam("caName", TEXT_ARRAY,
            CA_NAME_DESCR);

    public static final String CA_DESCRIPTION_DESCR = "Clinical analysis description";
    public static final ClinicalVariantQueryParam CA_DESCRIPTION = new ClinicalVariantQueryParam("caDescription",
            TEXT_ARRAY, CA_DESCRIPTION_DESCR);

    public static final String CA_DISORDER_DESCR = "List of search analysis disorders";
    public static final ClinicalVariantQueryParam CA_DISORDER = new ClinicalVariantQueryParam("caDisorderId",
            TEXT_ARRAY, CA_DISORDER_DESCR);

    public static final String CA_FILE_DESCR = "List of search analysis files";
    public static final ClinicalVariantQueryParam CA_FILE = new ClinicalVariantQueryParam("caFiles", TEXT_ARRAY,
            CA_FILE_DESCR);

    public static final String CA_PROBAND_ID_DESCR = "List of proband IDs";
    public static final ClinicalVariantQueryParam CA_PROBAND_ID = new ClinicalVariantQueryParam("caProbandId",
            TEXT_ARRAY, CA_PROBAND_ID_DESCR);

    public static final String CA_PROBAND_DISORDERS_DESCR = "List of proband disorders";
    public static final ClinicalVariantQueryParam CA_PROBAND_DISORDERS = new ClinicalVariantQueryParam("caProbandDisorders",
            TEXT_ARRAY, CA_PROBAND_DISORDERS_DESCR);

    public static final String CA_PROBAND_PHENOTYPES_DESCR = "List of proband phenotypes";
    public static final ClinicalVariantQueryParam CA_PROBAND_PHENOTYPES = new ClinicalVariantQueryParam("caProbandPhenotypes",
            TEXT_ARRAY, CA_PROBAND_PHENOTYPES_DESCR);

    public static final String CA_FAMILY_ID_DESCR = "List of family IDs";
    public static final ClinicalVariantQueryParam CA_FAMILY_ID = new ClinicalVariantQueryParam("caFamilyId", TEXT_ARRAY,
            CA_FAMILY_ID_DESCR);

    public static final String CA_FAMILY_MEMBER_IDS_DESCR = "List of search analysis family member IDs";
    public static final ClinicalVariantQueryParam CA_FAMILY_MEMBER_IDS = new ClinicalVariantQueryParam("caFamilyMemberIds", TEXT_ARRAY,
            CA_FAMILY_MEMBER_IDS_DESCR);

    public static final String CA_COMMENTS_DESCR = "List of search analysis comments";
    public static final ClinicalVariantQueryParam CA_COMMENTS = new ClinicalVariantQueryParam("caComments", TEXT_ARRAY,
            CA_COMMENTS_DESCR);

    public static final String CA_INFO_DESCR = "";
    public static final ClinicalVariantQueryParam CA_INFO = new ClinicalVariantQueryParam("caInfo", TEXT_ARRAY, CA_INFO_DESCR);

    // ---------- Interpretation (aka INT)

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

    public static final String INT_PANELS_DESCR = "List of interpretation panels";
    public static final ClinicalVariantQueryParam INT_PANELS = new ClinicalVariantQueryParam("intPanels", TEXT_ARRAY,
            INT_PANELS_DESCR);

    public static final String INT_INFO_DESCR = "";
    public static final ClinicalVariantQueryParam INT_INFO = new ClinicalVariantQueryParam("intInfo", TEXT_ARRAY, INT_INFO_DESCR);

    public static final String INT_DESCRIPTION_DESCR = "Interpretation description";
    public static final ClinicalVariantQueryParam INT_DESCRIPTION = new ClinicalVariantQueryParam("intDescription", TEXT_ARRAY,
            INT_DESCRIPTION_DESCR);

    public static final String INT_DEPENDENCY_DESCR = "List of interpretation dependency, format: name:version, e.g. cellbase:4.0";
    public static final ClinicalVariantQueryParam INT_DEPENDENCY = new ClinicalVariantQueryParam("intDependency", TEXT_ARRAY,
            INT_DEPENDENCY_DESCR);

    public static final String INT_FILTERS_DESCR = "List of interpretation filters";
    public static final ClinicalVariantQueryParam INT_FILTERS = new ClinicalVariantQueryParam("intFilters", TEXT_ARRAY,
            INT_FILTERS_DESCR);

    public static final String INT_COMMENTS_DESCR = "List of interpretation comments";
    public static final ClinicalVariantQueryParam INT_COMMENTS = new ClinicalVariantQueryParam("intComments", TEXT_ARRAY,
            INT_COMMENTS_DESCR);

    public static final String INT_CREATION_DATE_DESCR = "Iinterpretation creation date (including date ranges)";
    public static final ClinicalVariantQueryParam INT_CREATION_DATE = new ClinicalVariantQueryParam("intCreationDate", STRING,
            INT_CREATION_DATE_DESCR);

    // ---------- Clinical variant (aka CV)

    public static final String CV_DE_NOVO_QUALITY_SCORE_DESCR = "List of clinical variant de novo quality scores";
    public static final ClinicalVariantQueryParam CV_DE_NOVO_QUALITY_SCORE = new ClinicalVariantQueryParam("cvDeNovoQualityScore",
            TEXT_ARRAY, CV_DE_NOVO_QUALITY_SCORE_DESCR);

    public static final String CV_COMMENTS_DESCR = "List of clinical variant comments";
    public static final ClinicalVariantQueryParam CV_COMMENTS = new ClinicalVariantQueryParam("cvComments", TEXT_ARRAY,
            CV_COMMENTS_DESCR);

    // ---------- Clinical variant evidence (aka CVE)

    public static final String CVE_PHENOTYPE_NAMES_DESCR = "List of clinical variant evidence phenotype names";
    public static final ClinicalVariantQueryParam CVE_PHENOTYPE_NAMES = new ClinicalVariantQueryParam("cvePhenotypeNames", TEXT_ARRAY,
            CVE_PHENOTYPE_NAMES_DESCR);

    public static final String CVE_CONSEQUENCE_TYPE_IDS_DESCR = "List of clinical variant evidence consequence type IDs";
    public static final ClinicalVariantQueryParam CVE_CONSEQUENCE_TYPE_IDS = new ClinicalVariantQueryParam("cveConsequenceTypeIds",
            TEXT_ARRAY, CVE_CONSEQUENCE_TYPE_IDS_DESCR);

    public static final String CVE_GENE_NAMES_DESCR = "List of clinical variant evidence gene names";
    public static final ClinicalVariantQueryParam CVE_GENE_NAMES = new ClinicalVariantQueryParam("cveGeneNames", TEXT_ARRAY,
            CVE_GENE_NAMES_DESCR);

    public static final String CVE_XREFS_DESCR = "List of clinical variant evidence phenotype xRefs";
    public static final ClinicalVariantQueryParam CVE_XREFS = new ClinicalVariantQueryParam("cveXrefs", TEXT_ARRAY, CVE_XREFS_DESCR);

    public static final String CVE_PANEL_IDS_DESCR = "List of clinical variant evidence panel IDs";
    public static final ClinicalVariantQueryParam CVE_PANEL_IDS = new ClinicalVariantQueryParam("cvePanelNames", TEXT_ARRAY,
            CVE_PANEL_IDS_DESCR);

    public static final String CVE_ACMG_DESCR = "List of clinical variant evidence ACMG";
    public static final ClinicalVariantQueryParam CVE_ACMG = new ClinicalVariantQueryParam("cveAcmg", TEXT_ARRAY, CVE_ACMG_DESCR);

    public static final String CVE_CLINICAL_SIGNIFICANCE_DESCR = "List of clinical variant evidence clinical significance";
    public static final ClinicalVariantQueryParam CVE_CLINICAL_SIGNIFICANCE = new ClinicalVariantQueryParam("cveClinicalSignificance",
            TEXT_ARRAY, CVE_CLINICAL_SIGNIFICANCE_DESCR);

    public static final String CVE_DRUG_RESPONSE_DESCR = "List of clinical variant evidence drug response";
    public static final ClinicalVariantQueryParam CVE_DRUG_RESPONSE = new ClinicalVariantQueryParam("cveDrugResponse", TEXT_ARRAY,
            CVE_DRUG_RESPONSE_DESCR);

    public static final String CVE_TRAIT_ASSOCIATION_DESCR = "List of clinical variant evidence trait association";
    public static final ClinicalVariantQueryParam CVE_TRAIT_ASSOCIATION = new ClinicalVariantQueryParam("reTraitAssociation", TEXT_ARRAY,
            CVE_TRAIT_ASSOCIATION_DESCR);

    public static final String CVE_FUNCTIONAL_EFFECT_DESCR = "List of clinical variant evidence functional effect";
    public static final ClinicalVariantQueryParam CVE_FUNCTIONAL_EFFECT = new ClinicalVariantQueryParam("reFunctionalEffect", TEXT_ARRAY,
            CVE_FUNCTIONAL_EFFECT_DESCR);

    public static final String CVE_TUMORIGENESIS_DESCR = "List of clinical variant evidence tumorigenesis";
    public static final ClinicalVariantQueryParam CVE_TUMORIGENESIS = new ClinicalVariantQueryParam("reTumorigenesis", TEXT_ARRAY,
            CVE_TUMORIGENESIS_DESCR);

    public static final String CVE_OTHER_CLASSIFICATION_DESCR = "List of clinical variant evidence other classification";
    public static final ClinicalVariantQueryParam CVE_OTHER_CLASSIFICATION = new ClinicalVariantQueryParam("reOtherClassification",
            TEXT_ARRAY, CVE_OTHER_CLASSIFICATION_DESCR);

    public static final String CVE_ROLES_IN_CANCER_DESCR = "List of clinical variant evidence roles in cancer";
    public static final ClinicalVariantQueryParam CVE_ROLES_IN_CANCER = new ClinicalVariantQueryParam("reRolesInCancer", TEXT_ARRAY,
            CVE_ROLES_IN_CANCER_DESCR);

    public static final String CVE_TIER_DESCR = "List of clinical variant evidence tier";
    public static final ClinicalVariantQueryParam CVE_TIER = new ClinicalVariantQueryParam("reTier", TEXT_ARRAY, CVE_TIER_DESCR);

    public static final String CVE_JUSTIFICATION_DESCR = "List of clinical variant evidence justification";
    public static final ClinicalVariantQueryParam CVE_JUSTIFICATION = new ClinicalVariantQueryParam("reJustification", TEXT_ARRAY,
            CVE_JUSTIFICATION_DESCR);

    public static final String CVE_AUX_DESCR = "";
    public static final ClinicalVariantQueryParam CVE_AUX = new ClinicalVariantQueryParam("reAux", TEXT_ARRAY, CVE_AUX_DESCR);

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
