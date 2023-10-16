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
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.util.List;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;

public class ClinicalAnalysisQueryParser extends ClinicalQueryParser {

    public ClinicalAnalysisQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        super(variantStorageMetadataManager);
    }

    @Override
    public SolrQuery parse(Query query, QueryOptions queryOptions) throws CvdbException {
        String projectId = query.getString(ParamConstants.PROJECT_PARAM_NAME);

        SolrQuery solrQuery = new SolrQuery("*:*");

        // /select?fq=
        // {!join from=join_field_of_A to=join_field_of_B fromIndex=collection_B}join_field_of_B:value1
        // AND
        // {!join from=join_field_of_A to=join_field_of_C fromIndex=collection_C}join_field_of_C:value2

        //---------------------------------------------------------------------
        // Clinical interpretation filters
        //---------------------------------------------------------------------

        if (query.containsKey(ClinicalAnalysisQueryParam.CI_PANEL_ID.key())) {
            String ciCollection = getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX);
            List<String> panelIds = query.getAsStringList(ClinicalAnalysisQueryParam.CI_PANEL_ID.key(), ",");

            StringBuilder sb = new StringBuilder();
            for (String panelId : panelIds) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("panelIds: \"").append(panelId).append("\"");
            }

            String joinFilterQuery = "{!join from=caId to=id fromIndex=" + ciCollection + "}(" + sb + ")";
            solrQuery.addFilterQuery(joinFilterQuery);
        }

        //---------------------------------------------------------------------
        // Clinical variant filters
        //---------------------------------------------------------------------

        if (query.containsKey(ClinicalAnalysisQueryParam.CV_VARIANT_ID.key())) {
            String cvCollection = getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX);
            List<String> variantIds = query.getAsStringList(ClinicalAnalysisQueryParam.CV_VARIANT_ID.key(), ",");

            StringBuilder sb = new StringBuilder();
            for (String variantId : variantIds) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("variantId: \"").append(variantId).append("\"");
            }

            String joinFilterQuery = "{!join from=caId to=id fromIndex=" + cvCollection + "}(" + sb + ")";
            solrQuery.addFilterQuery(joinFilterQuery);
        }

        // Return Solr query
        logger.info("Solr query: {}", solrQuery.toQueryString());

        return solrQuery;
    }
}
