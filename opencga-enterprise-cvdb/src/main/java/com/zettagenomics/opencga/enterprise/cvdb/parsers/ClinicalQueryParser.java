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

import com.zettagenomics.opencga.enterprise.cvdb.converters.SearchConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.core.tools.annotations.ApiImplicitParam;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;

import static com.zettagenomics.opencga.enterprise.cvdb.converters.SearchConverter.simpleDateFormat;
import static com.zettagenomics.opencga.enterprise.cvdb.converters.SearchConverter.solrDateFormat;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SCORE;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.FIELD_SEPARATOR;

public class ClinicalQueryParser {

    SolrQueryParser solrParser;

    public static Set<String> CA_FACET_FIELD_SET = new HashSet<>(Arrays.asList(CA_TYPE_NAME, CA_DISORDER_ID_NAME, CA_FILENAME_NAME,
            CA_PROBAND_ID_NAME, CA_FAMILY_ID_NAME, CA_FAMILY_PHENOTYPE_NAME_NAME, CA_FAMILY_MEMBER_ID_NAME, CA_STATUS_NAME,
            CA_LOCKED_NAME));

    public static final String CA_FACET_FIELDS = CA_TYPE_NAME + ", " + CA_DISORDER_ID_NAME + ", " + CA_FILENAME_NAME + ", "
            + CA_PROBAND_ID_NAME + ", " + CA_FAMILY_ID_NAME + ", " + CA_FAMILY_PHENOTYPE_NAME_NAME + ", " + CA_FAMILY_MEMBER_ID_NAME
            + ", " + CA_STATUS_NAME + ", " + CA_LOCKED_NAME;

    public static Set<String> CI_FACET_FIELD_SET = new HashSet<>(Arrays.asList(CI_ID_NAME, CI_PRIMARY_NAME, CI_PANEL_ID_NAME,
            CI_ANALYIST_ID_NAME, CI_ANALYIST_NAME_NAME, CI_ANALYIST_EMAIL_NAME, CI_ANALYIST_ASSIGNED_BY_NAME, CI_ANALYIST_DATE_NAME,
            CI_METHOD_NAME_NAME, CI_METHOD_VERSION_NAME, CI_METHOD_COMMIT_NAME, CI_LOCKED_NAME, CI_STATUS_ID_NAME, CI_STATUS_NAME_NAME,
            CI_STATUS_DATE_NAME, CI_CREATION_DATE_NAME, CI_MODIFICATION_DATE_NAME, CI_VERSION_NAME));

    public static final String CI_FACET_FIELDS = CI_ID_NAME + ", " + CI_PRIMARY_NAME + ", " + CI_PANEL_ID_NAME + ", " + CI_ANALYIST_ID_NAME
            + ", " + CI_ANALYIST_NAME_NAME + ", " + CI_ANALYIST_EMAIL_NAME + ", " + CI_ANALYIST_ASSIGNED_BY_NAME + ", "
            + CI_ANALYIST_DATE_NAME + ", " + CI_METHOD_NAME_NAME + ", " + CI_METHOD_VERSION_NAME + ", " + CI_METHOD_COMMIT_NAME + ", "
            + CI_LOCKED_NAME + ", " + CI_STATUS_ID_NAME + ", " + CI_STATUS_NAME_NAME + ", " + CI_STATUS_DATE_NAME + ", "
            + CI_CREATION_DATE_NAME + ", " + CI_MODIFICATION_DATE_NAME + ", " + CI_VERSION_NAME;

    public static Set<String> CV_FACET_FIELD_SET = new HashSet<>(Arrays.asList(CV_ID_NAME, CV_PRIMARY_NAME, CV_DISCUSSION_AUTHOR_NAME,
            CV_DISCUSSION_DATE_NAME, CV_CONFIDENCE_VALUE_NAME, CV_CONFIDENCE_AUTHOR_NAME, CV_CONFIDENCE_DATE_NAME, CV_TAG_NAME,
            CV_STATUS_NAME, CV_ANNOT_BIOTYPE_NAME, CV_ANNOT_CONSEQUENCE_TYPE_NAME,
            //CV_ANNOT_TRANSCRIPT_FLAG_NAME,
            CV_GENE_NAME,
            CV_ANNOT_XREF_NAME, CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, CV_TYPE_NAME
            //CV_ANNOT_PROTEIN_SUBSTITUTION_NAME,
            //CV_ANNOT_CONSERVATION_NAME, CV_ANNOT_FUNCTIONAL_SCORE_NAME, CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME,
            //CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME, CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, CV_STATS_ALT_NAME,
            //CV_STATS_MAF_NAME, CV_STATS_REF_NAME, CV_STATS_PASS_FREQ_NAME, CV_SCORE_NAME,
            //CV_ANNOT_GO_GENES_NAME,
            //CV_ANNOT_EXPRESSION_GENES_NAME, CV_ANNOT_GENE_TRAIT_ID_NAME, CV_ANNOT_TRAIT_NAME, CV_ANNOT_PROTEIN_KEYWORD_NAME
    ));

    public static final String CV_FACET_FIELDS = CV_ID_NAME + ", " + CV_PRIMARY_NAME + ", " + CV_DISCUSSION_AUTHOR_NAME + ", " +
            CV_DISCUSSION_DATE_NAME + ", " + CV_CONFIDENCE_VALUE_NAME + ", " + CV_CONFIDENCE_AUTHOR_NAME + ", " + CV_CONFIDENCE_DATE_NAME
            + ", " + CV_TAG_NAME + ", " + CV_STATUS_NAME + ", " + CV_ANNOT_BIOTYPE_NAME + ", " + CV_ANNOT_CONSEQUENCE_TYPE_NAME + ", "
            //+ CV_ANNOT_TRANSCRIPT_FLAG_NAME + ", "
            +  CV_GENE_NAME + ", " + CV_ANNOT_XREF_NAME + ", "
            + CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME + ", " + CV_TYPE_NAME
            //CV_ANNOT_PROTEIN_SUBSTITUTION_NAME,
            //CV_ANNOT_CONSERVATION_NAME, CV_ANNOT_FUNCTIONAL_SCORE_NAME, CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME,
            //CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME, CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, CV_STATS_ALT_NAME,
            //CV_STATS_MAF_NAME, CV_STATS_REF_NAME, CV_STATS_PASS_FREQ_NAME, CV_SCORE_NAME,
            //CV_ANNOT_GO_GENES_NAME,
            //+ ", " + CV_ANNOT_EXPRESSION_GENES_NAME + ", " + CV_ANNOT_GENE_TRAIT_ID_NAME + ", " + CV_ANNOT_TRAIT_NAME + ", "
            //+ CV_ANNOT_PROTEIN_KEYWORD_NAME
            ;

    public static Set<String> CVE_FACET_FIELD_SET = new HashSet<>(Arrays.asList(CVE_PHENOTYPE_NAME_NAME, CVE_GENE_NAME_NAME,
            CVE_CONSEQUENCE_TYPE_ID_NAME, CVE_XREF_ID_NAME, CVE_PANEL_ID_NAME, CVE_MOI_NAME, CVE_PENETRANCE_NAME, CVE_ACGM_NAME,
            CVE_TIER_NAME, CVE_CLINICAL_SIGNIFICANCE_NAME, CVE_DRUG_RESPONSE_NAME, CVE_TRAIT_ASSOCIATION_NAME, CVE_FUNCTIONAL_EFFECT_NAME,
            CVE_TUMORIGENESIS_NAME, CVE_OTHER_CLASSIFICATION_NAME, CVE_ROL_IN_CANCER_NAME));

    public static final String CVE_FACET_FIELDS = CVE_PHENOTYPE_NAME_NAME + ", " + CVE_GENE_NAME_NAME + ", "
            + CVE_CONSEQUENCE_TYPE_ID_NAME + ", " + CVE_XREF_ID_NAME + ", " + CVE_PANEL_ID_NAME + ", " + CVE_MOI_NAME + ", "
            + CVE_PENETRANCE_NAME + ", " + CVE_ACGM_NAME + ", " + CVE_TIER_NAME + ", " + CVE_CLINICAL_SIGNIFICANCE_NAME + ", "
            + CVE_DRUG_RESPONSE_NAME + ", " + CVE_TRAIT_ASSOCIATION_NAME + ", " + CVE_FUNCTIONAL_EFFECT_NAME + ", "
            + CVE_TUMORIGENESIS_NAME + ", " + CVE_OTHER_CLASSIFICATION_NAME + ", " + CVE_ROL_IN_CANCER_NAME;

    protected static Logger logger = LoggerFactory.getLogger(ClinicalQueryParser.class);

    protected ClinicalQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        solrParser = new SolrQueryParser(variantStorageMetadataManager);
    }

    public SolrQuery parse(Query query, QueryOptions queryOptions) throws CvdbException {
        return solrParser.parse(query, queryOptions);
    }

    public List<String> clinicalAnalysisFilters(Query query) {
        List<String> filters = new ArrayList<>();

        // <field name="id" type="string" indexed="true" stored="true" required="true" multiValued="false" />
        addStringFilters("id", query.getString(ClinicalQueryParam.CA_ID_NAME), filters);

        // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>
        addTextFilters("description", query.getString(ClinicalQueryParam.CA_DESCRIPTION_NAME), filters);

        // <field name="type" type="text_en" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("type", query.getString(ClinicalQueryParam.CA_TYPE_NAME), filters);

        // <field name="disorderId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("disorderId", query.getString(ClinicalQueryParam.CA_DISORDER_ID_NAME), filters);

        // <field name="fileNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("fileNames", query.getString(ClinicalQueryParam.CA_FILENAME_NAME), filters);

        // <field name="probandId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("probandId", query.getString(ClinicalQueryParam.CA_PROBAND_ID_NAME), filters);

        // <field name="familyId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("familyId", query.getString(ClinicalQueryParam.CA_FAMILY_ID_NAME), filters);

        // <field name="familyPhenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("familyPhenotypeNames", query.getString(ClinicalQueryParam.CA_FAMILY_PHENOTYPE_NAME_NAME), filters);

        // <field name="familyMemberIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("familyMemberIds", query.getString(ClinicalQueryParam.CA_FAMILY_MEMBER_ID_NAME), filters);

        // <field name="report" type="string" indexed="true" stored="true" multiValued="false"/>
        addTextFilters("report", query.getString(CA_REPORT_NAME), filters);

        // <field name="status" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("status", query.getString(ClinicalQueryParam.CA_STATUS_NAME), filters);

        // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("locked", query.getString(CA_LOCKED_NAME), filters);

        return filters;
    }

    public List<String> clinicalInterpretationFilters(Query query) {
        List<String> filters = new ArrayList<>();

        addStringFilters("id", query.getString(ClinicalQueryParam.CI_ID_NAME), filters);

        // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("primary", query.getString(CI_PRIMARY_NAME), filters);

        // <field name="description" type="string" indexed="true" stored="true" multiValued="false"/>
        addTextFilters("description", query.getString(CI_DESCRIPTION_NAME), filters);

        // <!-- Panel IDs contain both IDs and names -->
        // <field name="panelIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("panelIds", query.getString(ClinicalQueryParam.CI_PANEL_ID_NAME), filters);

        // <field name="analystId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystId", query.getString(ClinicalQueryParam.CI_ANALYIST_ID_NAME), filters);

        // <field name="analystName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystName", query.getString(ClinicalQueryParam.CI_ANALYIST_NAME_NAME), filters);

        // <field name="analystEmail" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystEmail", query.getString(ClinicalQueryParam.CI_ANALYIST_EMAIL_NAME), filters);

        // <field name="analystAssignedBy" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("analystAssignedBy", query.getString(ClinicalQueryParam.CI_ANALYIST_ASSIGNED_BY_NAME), filters);

        // <field name="analystDate" type="string" indexed="true" stored="true" multiValued="false"/>
        addDateFilters("analystDate", query.getString(ClinicalQueryParam.CI_ANALYIST_DATE_NAME), filters);

        // <field name="methodName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("methodName", query.getString(ClinicalQueryParam.CI_METHOD_NAME_NAME), filters);

        // <field name="methodVersion" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("methodVersion", query.getString(ClinicalQueryParam.CI_METHOD_VERSION_NAME), filters);

        // <field name="methodCommit" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("methodCommit", query.getString(ClinicalQueryParam.CI_METHOD_COMMIT_NAME), filters);

        // <!-- Method software/dependencies are stores: name == version -->
        // <field name="methodDependencies" type="string" indexed="true" stored="true" multiValued="true"/>
        addTextFilters("methodDependencies", query.getString(CI_METHOD_DEPENDENCIES_NAME), filters);

        // <!-- Comments are stores: author == message == tag1:tag2:.. == date -->
        // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
        addTextFilters("comments", query.getString(CI_COMMENTS_NAME), filters);

        // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("locked", query.getString(CI_LOCKED_NAME), filters);

        // <field name="statusId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("statusId", query.getString(ClinicalQueryParam.CI_STATUS_ID_NAME), filters);

        // <field name="statusName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("statusName", query.getString(ClinicalQueryParam.CI_STATUS_NAME_NAME), filters);

        // <field name="statusDescription" type="string" indexed="true" stored="true" multiValued="false"/>
        addTextFilters("statusDescription", query.getString(CI_STATUS_DESCRIPTION_NAME), filters);

        // <field name="statusDate" type="string" indexed="true" stored="true" multiValued="false"/>
        addDateFilters("statusDate", query.getString(ClinicalQueryParam.CI_STATUS_DATE_NAME), filters);

        // <field name="creationDate" type="string" indexed="true" stored="true" multiValued="false"/>
        addDateFilters("creationDate", query.getString(ClinicalQueryParam.CI_CREATION_DATE_NAME), filters);

        // <field name="modificationDate" type="string" indexed="true" stored="true" multiValued="false"/>
        addDateFilters("modificationDate", query.getString(ClinicalQueryParam.CI_MODIFICATION_DATE_NAME), filters);

        // <field name="version" type="int" indexed="true" stored="true" multiValued="false"/>
        addIntegerFilters("version", query.getString(ClinicalQueryParam.CI_VERSION_NAME), filters);

        return filters;
    }

    public List<String> clinicalVariantFilters(Query query) throws CvdbException {
        List<String> filters = new ArrayList<>();

        // Get variant filters from Solr variant parser
        Query variantQuery = buildVariantQuery(query);
        SolrQuery solrQuery = solrParser.parse(variantQuery, QueryOptions.empty());
        if (ArrayUtils.isNotEmpty(solrQuery.getFilterQueries())) {
            filters.addAll(Arrays.asList(solrQuery.getFilterQueries()));
        }

        // Clinical variant filters

        // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("primary", query.getString(CV_PRIMARY_NAME), filters);

        // <!-- Comments are stores: author == message == tag1:tag2:.. == date -->
        // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
        addTextFilters("comments", query.getString(CV_COMMENTS_NAME), filters);

        // <!-- Filters are stored in two dynamic fields: one for string values, the other one for numeric ones -->
        // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
        // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>

        // <field name="discussionAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("discussionAuthor", query.getString(ClinicalQueryParam.CV_DISCUSSION_AUTHOR_NAME), filters);

        // <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("discussionDate", query.getString(ClinicalQueryParam.CV_DISCUSSION_DATE_NAME), filters);

        // <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>
        addTextFilters("discussionText", query.getString(CV_DISCUSSION_TEXT_NAME), filters);

        // <field name="confidenceValue" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("confidenceValue", query.getString(ClinicalQueryParam.CV_CONFIDENCE_VALUE_NAME), filters);

        // <field name="confidenceAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("confidenceAuthor", query.getString(ClinicalQueryParam.CV_CONFIDENCE_AUTHOR_NAME), filters);

        // <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("confidenceDate", query.getString(ClinicalQueryParam.CV_CONFIDENCE_DATE_NAME), filters);

        // <field name="tags" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("tags", query.getString(ClinicalQueryParam.CV_TAG_NAME), filters);

        // <field name="status" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("status", query.getString(ClinicalQueryParam.CV_STATUS_NAME), filters);

        return filters;
    }

    public List<String> clinicalVariantEvidenceFilters(Query query) {
        List<String> filters = new ArrayList<>();

        // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("phenotypeNames", query.getString(ClinicalQueryParam.CVE_PHENOTYPE_NAME_NAME), filters);

        // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("geneName", query.getString(ClinicalQueryParam.CVE_GENE_NAME_NAME), filters);

        // <field name="consequenceTypeIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("consequenceTypeIds", query.getString(ClinicalQueryParam.CVE_CONSEQUENCE_TYPE_ID_NAME), filters);

        // <field name="xrefIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("xrefIds", query.getString(ClinicalQueryParam.CVE_XREF_ID_NAME), filters);

        // <field name="panelId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("panelId", query.getString(ClinicalQueryParam.CVE_PANEL_ID_NAME), filters);

        // <field name="mois" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("mois", query.getString(ClinicalQueryParam.CVE_MOI_NAME), filters);

        // <field name="penetrance" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("penetrance", query.getString(ClinicalQueryParam.CVE_PENETRANCE_NAME), filters);

        // <field name="acmgs" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("acmgs", query.getString(ClinicalQueryParam.CVE_ACGM_NAME), filters);

        // <field name="tier" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("tier", query.getString(ClinicalQueryParam.CVE_TIER_NAME), filters);

        // <field name="clinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("clinicalSignificance", query.getString(ClinicalQueryParam.CVE_CLINICAL_SIGNIFICANCE_NAME), filters);

        // <field name="drugResponse" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("drugResponse", query.getString(ClinicalQueryParam.CVE_DRUG_RESPONSE_NAME), filters);

        // <field name="traitAssociation" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("traitAssociation", query.getString(ClinicalQueryParam.CVE_TRAIT_ASSOCIATION_NAME), filters);

        // <field name="functionalEffect" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("functionalEffect", query.getString(ClinicalQueryParam.CVE_FUNCTIONAL_EFFECT_NAME), filters);

        // <field name="tumorigenesis" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("tumorigenesis", query.getString(ClinicalQueryParam.CVE_TUMORIGENESIS_NAME), filters);

        // <field name="otherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("otherClassifications", query.getString(ClinicalQueryParam.CVE_OTHER_CLASSIFICATION_NAME), filters);

        // <field name="rolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("rolesInCancer", query.getString(ClinicalQueryParam.CVE_ROL_IN_CANCER_NAME), filters);

        // <field name="reviewText" type="text_en" indexed="true" stored="true" multiValued="false"/>
        addTextFilters("reviewText", query.getString(ClinicalQueryParam.CVE_REVIEW_TEXT_NAME), filters);

        // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>

        return filters;
    }

    protected void addStringFilters(String fieldName, String fieldValue, List<String> filters) {
        if (StringUtils.isNotEmpty(fieldValue)) {
            List<String> values = Arrays.asList(fieldValue.split(","));

            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append(fieldName).append(": \"").append(value).append("\"");
            }
            filters.add(sb.toString());
        }
    }

    protected void addIntegerFilters(String fieldName, String fieldValue, List<String> filters) {
        if (StringUtils.isNotEmpty(fieldValue)) {
            List<String> values = Arrays.asList(fieldValue.split(","));

            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append(fieldName).append(": ").append(value);
            }
            filters.add(sb.toString());
        }
    }

    protected void addBooleanFilters(String fieldName, String fieldValue, List<String> filters) {
        if (StringUtils.isNotEmpty(fieldValue)) {
            String value = Boolean.parseBoolean(fieldValue) ? "true" : "false";
            filters.add(fieldName + ": " + value);
        }
    }

    protected void addTextFilters(String fieldName, String fieldValue, List<String> filters) {
        if (StringUtils.isNotEmpty(fieldValue)) {
            List<String> values = Arrays.asList(fieldValue.split("[,;]"));
            String op = fieldValue.contains(";") ? " AND " : " OR ";

            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                if (sb.length() > 0) {
                    sb.append(op);
                }
                sb.append(fieldName).append(": \"*").append(value).append("*\"");
            }
            filters.add(sb.toString());
        }
    }

    protected void addDateFilters(String fieldName, String fieldValue, List<String> filters) {
        if (StringUtils.isNotEmpty(fieldValue)) {
            List<String> values = Arrays.asList(fieldValue.split(","));

            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                try {
                    StringBuilder tmp = new StringBuilder();
                    if (value.contains("-")) {
                        // Interval: start date - end date
                        String[] split = value.split("-", -1);
                        tmp.append(fieldName).append(":[");
                        if (StringUtils.isEmpty(split[0])) {
                            tmp.append("*");
                        } else {
                            tmp.append(solrDateFormat.format(simpleDateFormat.parse(split[0])));
                        }
                        tmp.append(" TO ");
                        if (StringUtils.isEmpty(split[1])) {
                            tmp.append("*");
                        } else {
                            tmp.append(solrDateFormat.format(simpleDateFormat.parse(split[1])));
                        }
                        tmp.append("]");
                    } else {
                        // Single date
                        tmp.append(fieldName).append(": \"").append(solrDateFormat.format(simpleDateFormat.parse(value))).append("\"");
//                        tmp.append(fieldName).append(": [").append(solrDateFormat.format(simpleDateFormat.parse(value))).append(" TO ")
//                                .append(solrDateFormat.format(simpleDateFormat.parse(value))).append("]");
                    }
                    sb.append(tmp);
                } catch (ParseException e) {
                    logger.warn("Error parsing date {}: {}", value, e.getMessage());
                }
            }
            filters.add(sb.toString());
        }
    }

    protected void addStringFilters(List<String> filters, SolrQuery solrQuery) {
        addStringFilters(filters, null, solrQuery);
    }

    protected void addStringFilters(List<String> filters, String join, SolrQuery solrQuery) {
        if (CollectionUtils.isEmpty(filters)) {
            // Nothing to do
            return;
        }

        if (StringUtils.isEmpty(join)) {
            // No join query
            for (String filter : filters) {
                solrQuery.addFilterQuery(filter);
            }
        } else {
            // Join query
            String joinFilterQuery = join + "(" + StringUtils.join(filters, " AND ") + ")";
            solrQuery.addFilterQuery(joinFilterQuery);
        }
    }

    private Query buildVariantQuery(Query query) {
        Query variantQuery = new VariantQuery();
        if (query.containsKey(CV_ID_NAME)) {
            variantQuery.put(VariantQueryParam.ID.key(), query.get(CV_ID_NAME));
        }
        if (query.containsKey(CV_REGION_NAME)) {
            variantQuery.put(VariantQueryParam.REGION.key(), query.get(CV_REGION_NAME));
        }
        if (query.containsKey(CV_ANNOT_BIOTYPE_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_BIOTYPE.key(), query.get(CV_ANNOT_BIOTYPE_NAME));
        }
        if (query.containsKey(CV_ANNOT_CONSEQUENCE_TYPE_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), query.get(CV_ANNOT_CONSEQUENCE_TYPE_NAME));
        }
        if (query.containsKey(CV_ANNOT_TRANSCRIPT_FLAG_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_TRANSCRIPT_FLAG.key(), query.get(CV_ANNOT_TRANSCRIPT_FLAG_NAME));
        }
        if (query.containsKey(CV_GENE_NAME)) {
            variantQuery.put(VariantQueryParam.GENE.key(), query.get(CV_GENE_NAME));
        }
        if (query.containsKey(CV_ANNOT_XREF_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_XREF.key(), query.get(CV_ANNOT_XREF_NAME));
        }
        if (query.containsKey(CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME)) {
            variantQuery.put(VariantQueryUtils.ANNOT_GENE_ROLE_IN_CANER_GENES.key(), query.get(CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME));
        }
        if (query.containsKey(CV_TYPE_NAME)) {
            variantQuery.put(VariantQueryParam.TYPE.key(), query.get(CV_TYPE_NAME));
        }
        if (query.containsKey(CV_ANNOT_PROTEIN_SUBSTITUTION_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), query.get(CV_ANNOT_PROTEIN_SUBSTITUTION_NAME));
        }
        if (query.containsKey(CV_ANNOT_CONSERVATION_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_CONSERVATION.key(), query.get(CV_ANNOT_CONSERVATION_NAME));
        }
        if (query.containsKey(CV_ANNOT_FUNCTIONAL_SCORE_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key(), query.get(CV_ANNOT_FUNCTIONAL_SCORE_NAME));
        }
        if (query.containsKey(CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                    query.get(CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME));
        }
        if (query.containsKey(CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                    query.get(CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME));
        }
        if (query.containsKey(CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
                    query.get(CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME));
        }
        if (query.containsKey(CV_STATS_ALT_NAME)) {
            variantQuery.put(VariantQueryParam.STATS_ALT.key(), query.get(CV_STATS_ALT_NAME));
        }
        if (query.containsKey(CV_STATS_MAF_NAME)) {
            variantQuery.put(VariantQueryParam.STATS_MAF.key(), query.get(CV_STATS_MAF_NAME));
        }
        if (query.containsKey(CV_STATS_REF_NAME)) {
            variantQuery.put(VariantQueryParam.STATS_REF.key(), query.get(CV_STATS_REF_NAME));
        }
        if (query.containsKey(CV_STATS_PASS_FREQ_NAME)) {
            variantQuery.put(VariantQueryParam.STATS_PASS_FREQ.key(), query.get(CV_STATS_PASS_FREQ_NAME));
        }
        if (query.containsKey(CV_SCORE_NAME)) {
            variantQuery.put(VariantQueryParam.SCORE.key(), query.get(CV_SCORE_NAME));
        }
        if (query.containsKey(CV_ANNOT_GO_GENES_NAME)) {
            variantQuery.put(VariantQueryUtils.ANNOT_GO_GENES.key(), query.get(CV_ANNOT_GO_GENES_NAME));
        }
        if (query.containsKey(CV_ANNOT_EXPRESSION_GENES_NAME)) {
            variantQuery.put(VariantQueryUtils.ANNOT_EXPRESSION_GENES.key(), query.get(CV_ANNOT_EXPRESSION_GENES_NAME));
        }
        if (query.containsKey(CV_ANNOT_GENE_TRAIT_ID_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_GENE_TRAIT_ID.key(), query.get(CV_ANNOT_GENE_TRAIT_ID_NAME));
        }
//        if (query.containsKey(CV_ANNOT_GENE_TRAIT_NAME_NAME)) {
//            variantQuery.put(VariantQueryParam.ANNOT_GENE_TRAIT_NAME.key(), query.get(CV_ANNOT_GENE_TRAIT_NAME_NAME);
//        }
//        if (query.containsKey(CV_ANNOT_HPO_NAME)) {
//            variantQuery.put(VariantQueryParam.ANNOT_HPO.key(), query.get(CV_ANNOT_HPO_NAME);
//        }
        if (query.containsKey(CV_ANNOT_TRAIT_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_TRAIT.key(), query.get(CV_ANNOT_TRAIT_NAME));
        }
        if (query.containsKey(CV_ANNOT_PROTEIN_KEYWORD_NAME)) {
            variantQuery.put(VariantQueryParam.ANNOT_PROTEIN_KEYWORD.key(), query.get(CV_ANNOT_PROTEIN_KEYWORD_NAME));
        }

        return variantQuery;
    }

    //-------------------------------------------------------------------------
    //  A G G R E G A T I O N      S T A T S     /     F A C E T
    //-------------------------------------------------------------------------

    protected void parseFacet(Query query, QueryOptions queryOptions, SolrQuery solrQuery) throws CvdbException {
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            try {
                FacetQueryParser facetQueryParser = new FacetQueryParser();

                String facetQuery = parseFacet(queryOptions.getString(QueryOptions.FACET));
                String jsonFacet = facetQueryParser.parse(facetQuery);

                solrQuery.set("json.facet", jsonFacet);
                solrQuery.setRows(0);
                solrQuery.setStart(0);
                solrQuery.setFields();

                logger.debug(">>>>>> Solr Facet: " + solrQuery.toString());
            } catch (Exception e) {
                throw new CvdbException("Error parsing facet query", e);
            }
        }
    }

    protected String parseFacet(String facetQuery) {
        StringBuilder sb = new StringBuilder();
        String[] facets = facetQuery.split(FacetQueryParser.FACET_SEPARATOR);

        for (int i = 0; i < facets.length; i++) {
            if (i > 0) {
                sb.append(FacetQueryParser.FACET_SEPARATOR);
            }
            String[] nestedFacets = facets[i].split(FacetQueryParser.NESTED_FACET_SEPARATOR);
            for (int j = 0; j < nestedFacets.length; j++) {
                if (j > 0) {
                    sb.append(FacetQueryParser.NESTED_FACET_SEPARATOR);
                }
                String[] nestedSubfacets = nestedFacets[j].split(FacetQueryParser.NESTED_SUBFACET_SEPARATOR);
                for (int k = 0; k < nestedSubfacets.length; k++) {
                    if (k > 0) {
                        sb.append(FacetQueryParser.NESTED_SUBFACET_SEPARATOR);
                    }
                    // Convert to Solr schema fields, if necessary
                    sb.append(toSolrSchemaFields(nestedSubfacets[k]));
                }
            }
        }

        return sb.toString();
    }

    private String toSolrSchemaFields(String facet) {
        switch (facet) {

            // Clinical analysis
            case CA_TYPE_NAME:
                return "type";
            case CA_DISORDER_ID_NAME:
                return "disorderId";
            case CA_FILENAME_NAME:
                return "fileNames";
            case CA_PROBAND_ID_NAME:
                return "probandId";
            case CA_FAMILY_ID_NAME:
                return "familyId";
            case CA_FAMILY_PHENOTYPE_NAME_NAME:
                return "familyPhenotypeNames";
            case CA_FAMILY_MEMBER_ID_NAME:
                return "familyMemberIds";
            case CA_STATUS_NAME:
                return "status";
            case CA_LOCKED_NAME:
                return "locked";

            // Clinical interpretation
            case CI_ID_NAME:
                return "id";
            case CI_PRIMARY_NAME:
                return "primary";
            case CI_PANEL_ID_NAME:
                return "panelIds";
            case CI_ANALYIST_ID_NAME:
                return "analystId";
            case CI_ANALYIST_NAME_NAME:
                return "analystName";
            case CI_ANALYIST_EMAIL_NAME:
                return "analystEmail";
            case CI_ANALYIST_ASSIGNED_BY_NAME:
                return "analystAssignedBy";
            case CI_ANALYIST_DATE_NAME:
                return "analystDate";
            case CI_METHOD_NAME_NAME:
                return "methodName";
            case CI_METHOD_VERSION_NAME:
                return "methodVersion";
            case CI_METHOD_COMMIT_NAME:
                return "methodCommit";
            case CI_LOCKED_NAME:
                return "locked";
            case CI_STATUS_ID_NAME:
                return "statusId";
            case CI_STATUS_NAME_NAME:
                return "statusName";
            case CI_STATUS_DATE_NAME:
                return "statusDate";
            case CI_CREATION_DATE_NAME:
                return "creationDate";
            case CI_MODIFICATION_DATE_NAME:
                return "modificationDate";
            case CI_VERSION_NAME:
                return "version";

            // Clinical variant
            case CV_ID_NAME:
                return "variantId";
            case CV_PRIMARY_NAME:
                return "primary";
            case CV_DISCUSSION_AUTHOR_NAME:
                return "discussionAuthor";
            case CV_DISCUSSION_DATE_NAME:
                return "discussionDate";
            case CV_CONFIDENCE_VALUE_NAME:
                return "confidenceValue";
            case CV_CONFIDENCE_AUTHOR_NAME:
                return "confidenceAuthor";
            case CV_CONFIDENCE_DATE_NAME:
                return "confidenceDate";
            case CV_TAG_NAME:
                return "tags";
            case CV_STATUS_NAME:
                return "status";
            case CV_ANNOT_BIOTYPE_NAME:
                return "biotypes";
            case CV_ANNOT_CONSEQUENCE_TYPE_NAME:
                return "soAcc";
//            case CV_ANNOT_TRANSCRIPT_FLAG_NAME:
//                return "";
            case CV_GENE_NAME:
                return "genes";
            case CV_ANNOT_XREF_NAME:
                return "xrefs";
//            case CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME:
//                return "";
            case CV_TYPE_NAME:
                return "type";
//            case CV_ANNOT_EXPRESSION_GENES_NAME:
//                return "";
//            case CV_ANNOT_GENE_TRAIT_ID_NAME:
//                return "";
//            case CV_ANNOT_TRAIT_NAME:
//                return "";
//            case CV_ANNOT_PROTEIN_KEYWORD_NAME:
//                return "";

            // Clinical variant evidence
            case CVE_PHENOTYPE_NAME_NAME:
                return "phenotypeNames";
            case CVE_GENE_NAME_NAME:
                return "geneName";
            case CVE_CONSEQUENCE_TYPE_ID_NAME:
                return "consequenceTypeIds";
            case CVE_XREF_ID_NAME:
                return "xrefIds";
            case CVE_PANEL_ID_NAME:
                return "panelId";
            case CVE_MOI_NAME:
                return "mois";
            case CVE_PENETRANCE_NAME:
                return "penetrance";
            case CVE_ACGM_NAME:
                return "acmgs";
            case CVE_TIER_NAME:
                return "tier";
            case CVE_CLINICAL_SIGNIFICANCE_NAME:
                return "clinicalSignificance";
            case CVE_DRUG_RESPONSE_NAME:
                return "drugResponse";
            case CVE_TRAIT_ASSOCIATION_NAME:
                return "traitAssociation";
            case CVE_FUNCTIONAL_EFFECT_NAME:
                return "functionalEffect";
            case CVE_TUMORIGENESIS_NAME:
                return "tumorigenesis";
            case CVE_OTHER_CLASSIFICATION_NAME:
                return "otherClassifications";
            case CVE_ROL_IN_CANCER_NAME:
                return "rolesInCancer";

            // default
            default:
                return facet;
        }
//        if (facet.contains(CHROM_DENSITY)) {
//            return parseChromDensity(facet);
//        } else if (facet.contains(ANNOT_FUNCTIONAL_SCORE.key())) {
//            return parseFacet(facet, ANNOT_FUNCTIONAL_SCORE.key());
//        } else if (facet.contains(ANNOT_CONSERVATION.key())) {
//            return parseFacet(facet, ANNOT_CONSERVATION.key());
//        } else if (facet.contains(ANNOT_PROTEIN_SUBSTITUTION.key())) {
//            return parseFacet(facet, ANNOT_PROTEIN_SUBSTITUTION.key());
//        } else if (facet.contains(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key())) {
//            return parseFacetWithStudy(facet, "popFreq");
//        } else if (facet.contains(STATS_ALT.key())) {
//            return parseFacetWithStudy(facet, "altStats");
//        } else if (facet.contains(SCORE.key())) {
//            return parseFacetWithStudy(facet, SCORE.key());
//        } else {
//            return facet;
//        }
    }

//    private String parseFacet(String facet, String categoryName) {
//        if (facet.contains("(")) {
//            // Aggregation function
//            return facet.replace(categoryName, "").replace("[", "").replace("]", "");
//        } else if (facet.contains("..")) {
//            // Range
//            Matcher matcher = FACET_RANGE_PATTERN.matcher(facet);
//            if (matcher.find()) {
//                return matcher.group(2) + "[" + matcher.group(3) + "]:" + matcher.group(4);
//            } else {
//                throw VariantQueryException.malformedParam(categoryName, facet, "Invalid syntax for facet range.");
//            }
//        }
//        // Nothing to do
//        return facet;
//    }
//
//    private String parseFacetWithStudy(String facet, String categoryName) {
//        if (facet.contains("(")) {
//            // Aggregation function
//            Matcher matcher = FACET_FUNCTION_STUDY_PATTERN.matcher(facet);
//            if (matcher.find()) {
//                return matcher.group(1) + "(" + categoryName + FIELD_SEPARATOR + matcher.group(3) + FIELD_SEPARATOR + matcher.group(4)
//                        + ")";
//            } else {
//                throw VariantQueryException.malformedParam(categoryName, facet, "Invalid syntax for facet function.");
//            }
//        } else if (facet.contains("..")) {
//            // Range
//            Matcher matcher = FACET_RANGE_STUDY_PATTERN.matcher(facet);
//            if (matcher.find()) {
//                return categoryName + FIELD_SEPARATOR + matcher.group(2) + FIELD_SEPARATOR + matcher.group(3) + "[" + matcher.group(4)
//                        + "]:" + matcher.group(5);
//            } else {
//                throw VariantQueryException.malformedParam(categoryName, facet, "Invalid syntax for facet range.");
//            }
//        }
//        // Nothing to do
//        return facet;
//    }
//
//    private String parseChromDensity(String facet) {
//        // Categorical...
//        Matcher matcher = FacetQueryParser.CATEGORICAL_PATTERN.matcher(facet);
//        if (matcher.find()) {
//            if (matcher.group(1).equals(CHROM_DENSITY)) {
//                // Step management
//                int step = 1000000;
//                if (StringUtils.isNotEmpty(matcher.group(3))) {
//                    step = Integer.parseInt(matcher.group(3).substring(1));
//                }
//                int maxLength = 0;
//                // Include management
//                List<String> chromList;
//                String include = matcher.group(2);
//                if (StringUtils.isNotEmpty(include)) {
//                    chromList = new ArrayList<>();
//                    include = include.replace("]", "").replace("[", "");
//                    for (String value : include.split(FacetQueryParser.INCLUDE_SEPARATOR)) {
//                        chromList.add(value);
//                    }
//                } else {
//                    chromList = new ArrayList<>(chromosomeMap.keySet());
//                }
//
//                List<String> chromQueryList = new ArrayList<>();
//                for (String chrom : chromList) {
//                    if (chromosomeMap.get(chrom) > maxLength) {
//                        maxLength = chromosomeMap.get(chrom);
//                    }
//                    chromQueryList.add("chromosome:" + chrom);
//                }
//                return "start[1.." + maxLength + "]:" + step + ":chromDensity" + FacetQueryParser.LABEL_SEPARATOR + "chromosome:"
//                        + StringUtils.join(chromQueryList, " OR ");
//            } else {
//                throw VariantQueryException.malformedParam(CHROM_DENSITY, facet, "Invalid syntax.");
//            }
//        } else {
//            throw VariantQueryException.malformedParam(CHROM_DENSITY, facet, "Invalid syntax.");
//        }
//    }

    protected void addCommonFilters(Query query, List<String> filters) {
        // <field name="studyId" type="text_en" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("studyId", query.getString(ClinicalQueryParam.STUDY_ID.key()), filters);
        // <field name="viewers" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("viewers", query.getString(ClinicalQueryParam.VIEWER_NAME), filters);
    }
}
