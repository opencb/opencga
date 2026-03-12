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

import com.zettagenomics.opencga.enterprise.cvdb.CollectionNameGenerator;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.zettagenomics.opencga.enterprise.cvdb.converters.SearchConverter.simpleDateFormat;
import static com.zettagenomics.opencga.enterprise.cvdb.converters.SearchConverter.solrDateFormat;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;

public class ClinicalQueryParser {

    public static final String CA_FACET_FIELDS = "studyId, type, disorderId, fileNames, probandId, probandDisorderIds,"
        + " probandPhenotypeNames, familyId, familyPhenotypeNames, familyMemberIds, panelIds, status";

    public static final String CI_FACET_FIELDS = "caId, studyId, primary, panelIds, analystId, analystName, analystEmail,"
        + " analystAssignedBy, analystDate, methodName, methodVersion, methodCommit, statusId, statusDescription, statusType, statusDate,"
        + " creationDate, modificationDate, version";

    public static final String CV_FACET_FIELDS = "caId, ciId, variantId, studyId, primary, discussionAuthor, discussionDate,"
        + "discussionText, confidenceValue, confidenceAuthor, confidenceDate, tags, status, chromosome, start, end, xrefs, type, release,"
        + " studies, phastCons, phylop, gerp, caddRaw, caddScaled, sift, siftDesc, polyphen, polyphenDesc, genes, biotypes, soAcc,"
        + " clinicalSig";

    public static final String CVE_FACET_FIELDS =  "caId, ciId, cvId, variantId, studyId, phenotypeNames, geneName, transcriptId,"
        + " soTermNames, xrefIds, panelId, mois, penetrance, acmgs, tier, clinicalSignificance, drugResponse, traitAssociation,"
        + " functionalEffect, tumorigenesis, otherClassifications, rolesInCancer, reviewAcmgs, reviewTier, reviewClinicalSignificance";

    private SolrQueryParser solrParser;

    protected String collectionPrefix;
    protected String caCollectionName;
    protected String ciCollectionName;
    protected String cvCollectionName;
    protected String cveCollectionName;
    protected String viewerCollectionName;

    protected static Logger logger = LoggerFactory.getLogger(ClinicalQueryParser.class);

    protected ClinicalQueryParser(String collectionPrefix, SearchIndexMetadata searchIndexMetadata) {
        this.solrParser = new ClinicalSolrQueryParser(searchIndexMetadata);

        this.collectionPrefix = collectionPrefix;
        this.caCollectionName = CollectionNameGenerator.getClinicalAnalysisCollectionName(collectionPrefix);
        this.ciCollectionName = CollectionNameGenerator.getClinicalInterpretationCollectionName(collectionPrefix);
        this.cvCollectionName = CollectionNameGenerator.getClinicalVariantCollectionName(collectionPrefix);
        this.cveCollectionName = CollectionNameGenerator.getClinicalVariantEvidenceCollectionName(collectionPrefix);
        this.viewerCollectionName = CollectionNameGenerator.getClinicalViewerCollectionName(collectionPrefix);
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

        // <field name="probandDisorderIds" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("probandDisorderIds", query.getString(ClinicalQueryParam.CA_PROBAND_DISORDER_ID_NAME), filters);

        // <field name="probandPhenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("probandPhenotypeNames", query.getString(ClinicalQueryParam.CA_PROBAND_PHENOTYPE_NAME_NAME), filters);

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
            logger.warn("solrQuery.getFilterQueries() = {}", solrQuery.getFilterQueries());
        }

        addStringFilters("id", query.getString(ClinicalQueryParam.CV_ID_NAME), filters);

        // Clinical variant filters
        // <field name="variantId" type="string" indexed="false" stored="true" multiValued="false"/>
        addStringFilters("variantId", query.getString(CV_VARIANT_ID_NAME), filters);

        // <field name="primaryFinding" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("primaryFinding", query.getString(CV_PRIMARY_FINDING_NAME), filters);

        // <field name="primaryInterpretation" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("primaryInterpretation", query.getString(CV_PRIMARY_INTERPRETATION_NAME), filters);

        // <!-- Comments are stores: author == message == tag1:tag2:.. == date -->
        // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
        addTextFilters("comments", query.getString(CV_COMMENTS_NAME), filters);

        // <!-- Filters are stored in two dynamic fields: one for string values, the other one for numeric ones -->
        // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
        // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>

        // <field name="discussionAuthor" type="string" indexed="true" stored="true" multiValued="false"/>
        addTextFilters("discussionAuthor", query.getString(ClinicalQueryParam.CV_DISCUSSION_AUTHOR_NAME), filters);

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

        addStringFilters("id", query.getString(ClinicalQueryParam.CVE_ID_NAME), filters);

        //        addStringFilters(ClinicalQueryParam.CA_ID_NAME, query.getString(ClinicalQueryParam.CA_ID_NAME), filters);
//        addStringFilters(ClinicalQueryParam.CI_ID_NAME, query.getString(ClinicalQueryParam.CI_ID_NAME), filters);
//        addStringFilters(ClinicalQueryParam.CV_ID_NAME, query.getString(ClinicalQueryParam.CV_ID_NAME), filters);

        // <field name="variantId" type="string" indexed="false" stored="true" multiValued="false"/>
        addStringFilters(CVE_VARIANT_ID_NAME, query.getString(CVE_VARIANT_ID_NAME), filters);

        // <field name="primaryFinding" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("primaryFinding", query.getString(CVE_PRIMARY_FINDING_NAME), filters);

        // <field name="primaryInterpretation" type="boolean" indexed="true" stored="true" multiValued="false"/>
        addBooleanFilters("primaryInterpretation", query.getString(CVE_PRIMARY_INTERPRETATION_NAME), filters);

        // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("phenotypeNames", query.getString(ClinicalQueryParam.CVE_PHENOTYPE_NAME_NAME), filters);

        // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("geneName", query.getString(ClinicalQueryParam.CVE_GENE_NAME_NAME), filters);

        // <field name="transcriptId" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("transcriptId", query.getString(ClinicalQueryParam.CVE_TRANSCRIPT_ID_NAME), filters);

        // <field name="soTermNames" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("soTermNames", query.getString(ClinicalQueryParam.CVE_SO_TERM_NAME_NAME), filters);

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
        addStringFilters("rolesInCancer", query.getString(ClinicalQueryParam.CVE_ROLE_IN_CANCER_NAME), filters);

        // <field name="reviewAcmgs" type="string" indexed="true" stored="true" multiValued="true"/>
        addStringFilters("reviewAcmgs", query.getString(ClinicalQueryParam.CVE_REVIEW_ACGM_NAME), filters);

        // <field name="reviewTier" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("reviewTier", query.getString(ClinicalQueryParam.CVE_REVIEW_TIER_NAME), filters);

        // <field name="reviewClinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("reviewClinicalSignificance", query.getString(ClinicalQueryParam.CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME), filters);

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
//        if (query.containsKey(CV_VARIANT_ID_NAME)) {
//            variantQuery.put(VariantQueryParam.ID.key(), query.get(CV_VARIANT_ID_NAME));
//        }
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

    protected void logQueries(Query query, QueryOptions queryOptions, SolrQuery solrQuery, String title) {
        logger.debug("{} query: {}", title, query != null ? query.toJson() : null);
        logger.debug("{} query options: {}", title, query != null ? queryOptions.toJson() : null);
        logger.debug("Solr query: {}", solrQuery != null ? solrQuery.toQueryString() : null);
    }

    //-------------------------------------------------------------------------
    //  A G G R E G A T I O N      S T A T S     /     F A C E T
    //-------------------------------------------------------------------------

    protected void parseQueryOptions(QueryOptions queryOptions, SolrQuery solrQuery) {
        if (queryOptions.containsKey(QueryOptions.LIMIT)) {
            solrQuery.setRows(queryOptions.getInt(QueryOptions.LIMIT));
        }
        if (queryOptions.containsKey(QueryOptions.SKIP)) {
            solrQuery.setStart(queryOptions.getInt(QueryOptions.SKIP));
        }
    }

    //-------------------------------------------------------------------------
    //  A G G R E G A T I O N      S T A T S     /     F A C E T
    //-------------------------------------------------------------------------

    protected void parseFacet(Query query, QueryOptions queryOptions, SolrQuery solrQuery) throws CvdbException {
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            try {
                FacetQueryParser facetQueryParser = new FacetQueryParser();

                String facetQuery = queryOptions.getString(QueryOptions.FACET); //parseFacet(queryOptions.getString(QueryOptions.FACET));
                String jsonFacet = facetQueryParser.parse(facetQuery, queryOptions);

                solrQuery.set("json.facet", jsonFacet);
                solrQuery.setRows(0);
                solrQuery.setStart(0);
                solrQuery.setFields();

                logger.debug(">>>>>> Solr Facet: {}", solrQuery);
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
                    sb.append(toSolrSchemaField(nestedSubfacets[k]));
                }
            }
        }

        return sb.toString();
    }

    public static String toSolrSchemaField(String filterName) {
        switch (filterName) {

            // Clinical analysis
            case CA_TYPE_NAME:
                return "type";
            case CA_DISORDER_ID_NAME:
                return "disorderId";
            case CA_FILENAME_NAME:
                return "fileNames";
            case CA_PROBAND_ID_NAME:
                return "probandId";
            case CA_PROBAND_DISORDER_ID_NAME:
                return "probandDisorderIds";
            case CA_PROBAND_PHENOTYPE_NAME_NAME:
                return "probandPhenotypeNames";
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
                return "cvId";
            case CV_VARIANT_ID_NAME:
                return "variantId";
            case CV_PRIMARY_FINDING_NAME:
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
            case CVE_ID_NAME:
                return "cveId";
            case CVE_VARIANT_ID_NAME:
                return "variantId";
            case CVE_PHENOTYPE_NAME_NAME:
                return "phenotypeNames";
            case CVE_GENE_NAME_NAME:
                return "geneName";
            case CVE_TRANSCRIPT_ID_NAME:
                return "transcriptId";
            case CVE_SO_TERM_NAME_NAME:
                return "soTermAccessions";
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
            case CVE_ROLE_IN_CANCER_NAME:
                return "rolesInCancer";
            case CVE_REVIEW_TIER_NAME:
                return "reviewAcmgs";
            case CVE_REVIEW_ACGM_NAME:
                return "reviewTier";
            case CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME:
                return "reviewClinicalSignificance";

            // default
            default:
                return filterName;
        }
    }

    protected void addViewerFilter(Query query, String toValue, SolrQuery solrQuery) {
        List<String> filters = new ArrayList<>();
        addStringFilters("viewers", query.getString(ClinicalQueryParam.VIEWER_NAME), filters);
        addStringFilters("studyId", query.getString(ClinicalQueryParam.STUDY_ID.key()), filters);
        String join = "{!join from=id to=" + toValue + " fromIndex=" + viewerCollectionName + "}";
        addStringFilters(filters, join, solrQuery);
    }

    protected void addCommonFilters(Query query, List<String> filters) {
        // <field name="studyId" type="text_en" indexed="true" stored="true" multiValued="false"/>
        addStringFilters("studyId", query.getString(ClinicalQueryParam.STUDY_ID.key()), filters);
    }
}
