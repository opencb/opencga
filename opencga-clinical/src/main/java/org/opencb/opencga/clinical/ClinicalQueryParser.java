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

package org.opencb.opencga.clinical;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser;

public class ClinicalQueryParser {

    private SolrQueryParser solrQueryParser;

    public ClinicalQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        solrQueryParser = new SolrQueryParser(variantStorageMetadataManager);
    }

    public SolrQuery parse(Query query, QueryOptions queryOptions) {
        // First, call SolrQueryParser.parse
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);

        String key;

        // ---------- ClinicalAnalysis ----------
        //
        // ID, Name, description, disease, files, proband ID, family ID, family phenotype name, family member ID

        // ClinicalAnalysis name
        key = ReportedVariantQueryParam.CA_NAME.key();
        if (StringUtils.isNotEmpty(key)) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis description
        key = ReportedVariantQueryParam.CA_DESCRIPTION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis disease
        key = ReportedVariantQueryParam.CA_DISEASE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis germline and somatic files
        key = ReportedVariantQueryParam.CA_FILE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis proband ID
        key = ReportedVariantQueryParam.CA_PROBAND_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis family ID
        key = ReportedVariantQueryParam.CA_FAMILY_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalasys family phenotype names
        key = ReportedVariantQueryParam.CA_FAMILY_PHENOTYPE_NAMES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalasys family member IDs
        key = ReportedVariantQueryParam.CA_FAMILY_MEMBER_IDS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ---------- Interpretation ----------
        //
        //    ID, software name, software version, analyst name, panel name, creation date, more info

        // Interpretation ID
        key = ReportedVariantQueryParam.INT_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation software name
        key = ReportedVariantQueryParam.INT_SOFTWARE_NAME.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation software version
        key = ReportedVariantQueryParam.INT_SOFTWARE_VERSION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation analysit name
        key = ReportedVariantQueryParam.INT_ANALYST_NAME.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation panel names
        key = ReportedVariantQueryParam.INT_PANEL_NAMES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation description
        key = ReportedVariantQueryParam.INT_DESCRIPTION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInterpretationInfo(key, query.getString(key)));
        }

        // Interpretation dependency names
        key = ReportedVariantQueryParam.INT_DEPENDENCY_NAME.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInterpretationInfo(key, query.getString(key)));
        }

        // Interpretation dependency versions
        key = ReportedVariantQueryParam.INT_DEPENDENCY_VERSION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInterpretationInfo(key, query.getString(key)));
        }

        // Interpretation comments
        key = ReportedVariantQueryParam.INT_COMMENTS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInterpretationInfo(key, query.getString(key)));
        }

        // TODO: creation date management
        // Interpretation creation date

//        // ---------- Catalog ----------
//        //
//        //    project ID, assembly, study ID
//
//        // Project
//        key = "project";
//        if (StringUtils.isNotEmpty(query.getString(key))) {
//            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
//        }
//
//        // Assembly
//        key = "assembly";
//        if (StringUtils.isNotEmpty(query.getString(key))) {
//            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
//        }

        // ---------- ReportedVariant ----------
        //
        //   deNovo quality score, comments

        key = ReportedVariantQueryParam.RV_DE_NOVO_QUALITY_SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseNumericValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RV_COMMENTS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ---------- ReportedEvent ----------
        //
        //  phenotype names, consequence type IDs,

        key = ReportedVariantQueryParam.RE_PHENOTYPE_NAMES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_CONSEQUENCE_TYPE_IDS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_GENE_NAMES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_XREFS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_PANEL_NAMES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_ACMG.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_CLINICAL_SIGNIFICANCE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_DRUG_RESPONSE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_TRAIT_ASSOCIATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_FUNCTIONAL_EFFECT.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_TUMORIGENESIS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_OTHER_CLASSIFICATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_ROLES_IN_CANCER.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RE_SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        return solrQuery;
    }

    private String parseInterpretationInfo(String key, String string) {
        // TODO: parse intInfo
        return "";
    }
}
