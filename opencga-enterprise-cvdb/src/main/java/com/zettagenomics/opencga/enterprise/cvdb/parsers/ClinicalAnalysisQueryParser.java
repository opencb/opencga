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

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.CLINICAL_VARIANTS_COLLECTION_SUFFIX;
import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.getCollectionName;

public class ClinicalAnalysisQueryParser extends ClinicalQueryParser {

    public ClinicalAnalysisQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        super(variantStorageMetadataManager);
    }

    @Override
    public SolrQuery parse(Query query, QueryOptions queryOptions) throws CvdbException {
        String projectId = query.getString(ParamConstants.PROJECT_QUERY_PARAM);

        SolrQuery solrQuery = new SolrQuery("*:*");

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
