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

import static org.opencb.commons.datastore.core.QueryParam.Type.STRING;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;
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
    public static final String CA_STATUS_NAME = "ciStatusName";
    public static final String CA_STATUS_DESCR = "Clinical analysis status (or list of status" + OPT_LIST;
    public static final ClinicalQueryParam CA_STATUS = new ClinicalQueryParam(CA_STATUS_NAME, TEXT_ARRAY, CA_STATUS_DESCR);

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

    // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
    // <!-- Comments are stores: author == message == tag1:tag2:.. == date -->
    // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
    // <!-- Filters are stored in two dynamic fields: one for string values, the other one for numeric ones -->
    // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
    // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>

    // <field name="discussionAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CV_DISCUSSION_AUTHOR_NAME = "cvDiscussionAuthor";
    public static final String CV_DISCUSSION_AUTHOR_DESCR = "Clinical variant discussion author (or list of authors" + OPT_LIST;
    public static final ClinicalQueryParam CV_DISCUSSION_AUTHOR = new ClinicalQueryParam(CV_DISCUSSION_AUTHOR_NAME, TEXT_ARRAY,
            CV_DISCUSSION_AUTHOR_DESCR);

    // <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
    // <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>

    // <field name="confidenceValue" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CV_CONFIDENCE_VALUE_NAME = "cvConfidenceValue";
    public static final String CV_CONFIDENCE_VALUE_DESCR = "Clinical variant confidence value (or list of values" + OPT_LIST;
    public static final ClinicalQueryParam CV_CONFIDENCE_VALUE = new ClinicalQueryParam(CV_CONFIDENCE_VALUE_NAME, TEXT_ARRAY,
            CV_CONFIDENCE_VALUE_DESCR);

    // <field name="confidenceAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CV_CONFIDENCE_AUTHOR_NAME = "cvConfidenceAuthor";
    public static final String CV_CONFIDENCE_AUTHOR_DESCR = "Clinical variant confidence author (or list of authors" + OPT_LIST;
    public static final ClinicalQueryParam CV_CONFIDENCE_AUTHOR = new ClinicalQueryParam(CV_CONFIDENCE_AUTHOR_NAME, TEXT_ARRAY,
            CV_CONFIDENCE_AUTHOR_DESCR);

    // <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>

    // <field name="tags" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CV_TAG_NAME = "cvTag";
    public static final String CV_TAG_DESCR = "Clinical variant tag (or list of tags" + OPT_LIST;
    public static final ClinicalQueryParam CV_TAG = new ClinicalQueryParam(CV_TAG_NAME, TEXT_ARRAY, CV_TAG_DESCR);

    // <field name="status" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CV_STATUS_NAME = "cvStatus";
    public static final String CV_STATUS_DESCR = "Clinical variant status (or list of status" + OPT_LIST;
    public static final ClinicalQueryParam CV_STATUS = new ClinicalQueryParam(CV_STATUS_NAME, TEXT_ARRAY, CV_STATUS_DESCR);

    // Variant filters
    public static final String CV_ID_NAME = "cvId";
    public static final String CV_ID_DESCR = "Variant ID (or list of IDs" + OPT_LIST;
    public static final ClinicalQueryParam CV_ID = new ClinicalQueryParam(CV_ID_NAME, TEXT_ARRAY, CV_ID_DESCR);

    public static final String CV_REGION_NAME = "cvRegion";
    public static final String CV_REGION_DESCR = "Variant region (or list of regions, these can be just a single chromosome name"
            + " or regions in the format chr:start-end, e.g.: 2,3:100000-200000)";
    public static final ClinicalQueryParam CV_REGION = new ClinicalQueryParam(CV_REGION_NAME, TEXT_ARRAY, CV_REGION_DESCR);

    public static final String CV_ANNOT_BIOTYPE_NAME = "cvBiotype";
    public static final String CV_ANNOT_BIOTYPE_DESCR = "Variant biotype, e.g. protein_coding (or list of biotypes" + OPT_LIST;
    public static final ClinicalQueryParam CV_ANNOT_BIOTYPE = new ClinicalQueryParam(CV_ANNOT_BIOTYPE_NAME, TEXT_ARRAY,
            CV_ANNOT_BIOTYPE_DESCR);

    public static final String CV_ANNOT_CONSEQUENCE_TYPE_NAME = "cvCt";
    public static final String CV_ANNOT_CONSEQUENCE_TYPE_DESCR = "Variant SO consequence type (or list of SOs" + OPT_LIST
            + ", e.g. missense_variant,stop_lost or SO:0001583,SO:0001578. Accepts aliases 'loss_of_function' and 'protein_altering'";
    public static final ClinicalQueryParam CV_ANNOT_CONSEQUENCE_TYPE = new ClinicalQueryParam(CV_ANNOT_CONSEQUENCE_TYPE_NAME, TEXT_ARRAY,
            CV_ANNOT_CONSEQUENCE_TYPE_DESCR);

    public static final String CV_ANNOT_TRANSCRIPT_FLAG_NAME = "cvTranscriptFlag";
    public static final String CV_ANNOT_TRANSCRIPT_FLAG_DESCR = "Variant transcript flag (or list of flags" + OPT_LIST +", e.g."
            + " canonical, CCDS, basic, LRG, MANE Select, MANE Plus Clinical, EGLH_HaemOnc, TSO500";
    public static final ClinicalQueryParam CV_ANNOT_TRANSCRIPT_FLAG = new ClinicalQueryParam(CV_ANNOT_TRANSCRIPT_FLAG_NAME, TEXT_ARRAY,
            CV_ANNOT_TRANSCRIPT_FLAG_DESCR);

    public static final String CV_GENE_NAME = "cvGene";
    public static final String CV_GENE_DESCR = "Variant gene (or list genes" + OPT_LIST + ", most gene IDs are accepted (HGNC,"
            + " Ensembl gene, ...)";
    public static final ClinicalQueryParam CV_GENE = new ClinicalQueryParam(CV_GENE_NAME, TEXT_ARRAY, CV_GENE_DESCR);

    public static final String CV_ANNOT_XREF_NAME = "cvXref";
    public static final String CV_ANNOT_XREF_DESCR = "Variant external reference (or list of references" + OPT_LIST + ", these"
            + " can be genes, proteins or variants. Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, ...";
    public static final ClinicalQueryParam CV_ANNOT_XREF = new ClinicalQueryParam(CV_ANNOT_XREF_NAME, TEXT_ARRAY, CV_ANNOT_XREF_DESCR);

    public static final String CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME = "cvAnnotRoleInCancerGenes";
    public static final String CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR = "Variant rol in cancer genes (or list of roles"
            + OPT_LIST;
    public static final ClinicalQueryParam CV_ANNOT_GENE_ROLE_IN_CANER_GENES = new ClinicalQueryParam(
            CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, TEXT_ARRAY, CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR);

    public static final String CV_TYPE_NAME = "cvType";
    public static final String CV_TYPE_DESCR = "Variant type or list of types, accepted values are SNV, MNV, INDEL, SV, COPY_NUMBER,"
            + " COPY_NUMBER_LOSS, COPY_NUMBER_GAIN, INSERTION, DELETION, DUPLICATION, TANDEM_DUPLICATION, BREAKEND, e.g. SNV,INDEL";
    public static final ClinicalQueryParam CV_TYPE = new ClinicalQueryParam(CV_TYPE_NAME, TEXT_ARRAY, CV_TYPE_DESCR);

    public static final String CV_ANNOT_PROTEIN_SUBSTITUTION_NAME = "cvProteinSubstitution";
    public static final String CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR = "Variant protein substitution score (or list of scores"
            + OPT_LIST + ", include SIFT and PolyPhen. You can query using the score {protein_score}[<|>|<=|>=]{number} or the description"
            + " {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant";
    public static final ClinicalQueryParam CV_ANNOT_PROTEIN_SUBSTITUTION = new ClinicalQueryParam(CV_ANNOT_PROTEIN_SUBSTITUTION_NAME,
            TEXT_ARRAY, CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR);

    public static final String CV_ANNOT_CONSERVATION_NAME = "cvConservation";
    public static final String CV_ANNOT_CONSERVATION_DESCR = "Variant conservation score (or list of scores" + OPT_LIST
            + " with the format {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1";
    public static final ClinicalQueryParam CV_ANNOT_CONSERVATION = new ClinicalQueryParam(CV_ANNOT_CONSERVATION_NAME, TEXT_ARRAY,
            CV_ANNOT_CONSERVATION_DESCR);

    public static final String CV_ANNOT_FUNCTIONAL_SCORE_NAME = "cvFunctionalScore";
    public static final String CV_ANNOT_FUNCTIONAL_SCORE_DESCR = "Variant functional score (or list of scores" + OPT_LIST
            + " with the format {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3";
    public static final ClinicalQueryParam CV_ANNOT_FUNCTIONAL_SCORE = new ClinicalQueryParam(CV_ANNOT_FUNCTIONAL_SCORE_NAME, TEXT_ARRAY,
            CV_ANNOT_FUNCTIONAL_SCORE_DESCR);

    public static final String CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME = "cvPopulationFrequencyAlt";
    public static final String CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR = "Variant alternate population frequency (or list"
            + " of frequencies" + OPT_LIST + ", with the format {study}:{population}[<|>|<=|>=]{number}. e.g. 1000G:ALL<0.01";
    public static final ClinicalQueryParam CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY = new ClinicalQueryParam(
            CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, TEXT_ARRAY, CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR);

    public static final String CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME = "cvPopulationFrequencyMaf";
    public static final String CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR = "Variant population minor allele frequency (or"
            + " list of frequencies" + OPT_LIST + ", with the format {study}:{population}[<|>|<=|>=]{number}. e.g. 1000G:ALL<0.01";
    public static final ClinicalQueryParam CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY = new ClinicalQueryParam(
            CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME, TEXT_ARRAY, CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR);

    public static final String CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME = "cvPopulationFrequencyRef";
    public static final String CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR = "Variant reference population frequency (or"
            + " list of frequences" + OPT_LIST + ", with the format {study}:{population}[<|>|<=|>=]{number}. e.g. 1000G:ALL<0.01";
    public static final ClinicalQueryParam CV_ANNOT_POPULATION_REFERENCE_FREQUENCY = new ClinicalQueryParam(
            CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, TEXT_ARRAY, CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR);

    public static final String CV_STATS_ALT_NAME = "cvCohortStatsAlt";
    public static final String CV_STATS_ALT_DESCR = "Variant alternate allele frequency (or list of frequencies" + OPT_LIST
            + ", with the format [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4";
    public static final ClinicalQueryParam CV_STATS_ALT = new ClinicalQueryParam(CV_STATS_ALT_NAME, TEXT_ARRAY, CV_STATS_ALT_DESCR);

    public static final String CV_STATS_MAF_NAME = "cvCohortStatsMaf";
    public static final String CV_STATS_MAF_DESCR = "Variant minor allele frequency (or list of frequencies" + OPT_LIST
            + ", with the format [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4";
    public static final ClinicalQueryParam CV_STATS_MAF = new ClinicalQueryParam(CV_STATS_MAF_NAME, TEXT_ARRAY, CV_STATS_MAF_DESCR);

    public static final String CV_STATS_REF_NAME = "cvCohortStatsRef";
    public static final String CV_STATS_REF_DESCR = "Variant reference allele frequency (or list of frequencies" + OPT_LIST
            + ", with the foramt [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4";
    public static final ClinicalQueryParam CV_STATS_REF = new ClinicalQueryParam(CV_STATS_REF_NAME, TEXT_ARRAY, CV_STATS_REF_DESCR);

    public static final String CV_STATS_PASS_FREQ_NAME = "cvCohortStatsPass";
    public static final String CV_STATS_PASS_FREQ_DESCR = "Variant filter PASS frequency (or list of frequencies" + OPT_LIST
            + ", with the format [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL>0.8";
    public static final ClinicalQueryParam CV_STATS_PASS_FREQ = new ClinicalQueryParam(CV_STATS_PASS_FREQ_NAME, TEXT_ARRAY,
            CV_STATS_PASS_FREQ_DESCR);

    public static final String CV_SCORE_NAME = "cvScore";
    public static final String CV_SCORE_DESCR = "Variant score (or list of scores" + OPT_LIST + ", with the format:"
            + " [{study:}]{score}[<|>|<=|>=]{number}";
    public static final ClinicalQueryParam CV_SCORE = new ClinicalQueryParam(CV_SCORE_NAME, TEXT_ARRAY, CV_SCORE_DESCR);

    public static final String CV_ANNOT_GO_GENES_NAME = "cvAnnotGoGenes";
    public static final String CV_ANNOT_GO_GENES_DESCR = "Variant gene GO (or list of GOs" + OPT_LIST;
    public static final ClinicalQueryParam CV_ANNOT_GO_GENES = new ClinicalQueryParam(CV_ANNOT_GO_GENES_NAME, TEXT_ARRAY,
            CV_ANNOT_GO_GENES_DESCR);

    public static final String CV_ANNOT_EXPRESSION_GENES_NAME = "cvAnnotExpressionGenes";
    public static final String CV_ANNOT_EXPRESSION_GENES_DESCR = "Variant gene expression (or list of expressions" + OPT_LIST;
    public static final ClinicalQueryParam CV_ANNOT_EXPRESSION_GENES = new ClinicalQueryParam(CV_ANNOT_EXPRESSION_GENES_NAME, TEXT_ARRAY,
            CV_ANNOT_EXPRESSION_GENES_DESCR);

    public static final String CV_ANNOT_GENE_TRAIT_ID_NAME = "cvGeneTraitId";
    public static final String CV_ANNOT_GENE_TRAIT_ID_DESCR = "Variant gene trait association ID (or list of trait IDs" + OPT_LIST
            + ", e.g. \"umls:C0007222\" , \"OMIM:269600\"";
    public static final ClinicalQueryParam CV_ANNOT_GENE_TRAIT_ID = new ClinicalQueryParam(CV_ANNOT_GENE_TRAIT_ID_NAME, TEXT_ARRAY,
            CV_ANNOT_GENE_TRAIT_ID_DESCR);

    public static final String CV_ANNOT_TRAIT_NAME = "cvTrait";
    public static final String CV_ANNOT_TRAIT_DESCR = "Variant Trait (or list of traits" + OPT_LIST + ", based on ClinVar, HPO, COSMIC, i.e.:"
            + " IDs, histologies, descriptions,...";
    public static final ClinicalQueryParam CV_ANNOT_TRAIT = new ClinicalQueryParam(CV_ANNOT_TRAIT_NAME, TEXT_ARRAY, CV_ANNOT_TRAIT_DESCR);

    public static final String CV_ANNOT_PROTEIN_KEYWORD_NAME = "cvProteinKeyword";
    public static final String CV_ANNOT_PROTEIN_KEYWORD_DESCR = "Uniprot protein variant annotation keyword (or list of keywords"
            + OPT_LIST;
    public static final ClinicalQueryParam CV_ANNOT_PROTEIN_KEYWORD = new ClinicalQueryParam(CV_ANNOT_PROTEIN_KEYWORD_NAME, TEXT_ARRAY,
            CV_ANNOT_PROTEIN_KEYWORD_DESCR);

    public static final String CV_ANNOT_CLINICAL_NAME = "cvClinical";
    public static final String CV_ANNOT_CLINICAL_DESCR = "Variant clinical source (or list of sources" + OPT_LIST + ". Valid values:"
            + " clinvar, cosmic";
    public static final ClinicalQueryParam CV_ANNOT_CLINICAL = new ClinicalQueryParam(CV_ANNOT_CLINICAL_NAME, TEXT_ARRAY,
            CV_ANNOT_CLINICAL_DESCR);

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

    // <field name="mois" type="string" indexed="true" stored="true" multiValued="true"/>
    public static final String CVE_MOI_NAME = "cveMoi";
    public static final String CVE_MOI_DESCR = "Clinical variant evidence mode of inheritance (or list of modes of inheritance" + OPT_LIST
            + ", valid values: AUTOSOMAL_DOMINANT, AUTOSOMAL_RECESSIVE, X_LINKED_DOMINANT, X_LINKED_RECESSIVE, Y_LINKED, MITOCHONDRIAL,"
            + " DE_NOVO, MENDELIAN_ERROR, COMPOUND_HETEROZYGOUS, UNKNOWN";
    public static final ClinicalQueryParam CVE_MOI = new ClinicalQueryParam(CVE_MOI_NAME, TEXT_ARRAY, CVE_MOI_DESCR);

    // <field name="penetrance" type="string" indexed="true" stored="true" multiValued="false"/>
    public static final String CVE_PENETRANCE_NAME = "cvePenetrance";
    public static final String CVE_PENETRANCE_DESCR = "Clinical variant evidence penetrance (or list of penetrance values" + OPT_LIST
            + ", valid values: COMPLETE, INCOMPLETE, UNKNOWN";
    public static final ClinicalQueryParam CVE_PENETRANCE = new ClinicalQueryParam(CVE_PENETRANCE_NAME, TEXT_ARRAY, CVE_PENETRANCE_DESCR);

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
