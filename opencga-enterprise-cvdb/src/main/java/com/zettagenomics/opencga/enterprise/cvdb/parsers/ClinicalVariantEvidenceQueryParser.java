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

public class ClinicalVariantEvidenceQueryParser extends ClinicalQueryParser {

    public ClinicalVariantEvidenceQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        super(variantStorageMetadataManager);
    }

    @Override
    public SolrQuery parse(Query query, QueryOptions queryOptions) throws CvdbException {
        String projectId = query.getString(ParamConstants.PROJECT_PARAM_NAME);

        SolrQuery solrQuery = new SolrQuery("*:*");

        // Process facet, if necessary
        parseFacet(query, queryOptions, solrQuery);

        List<String> filters;
        String join;

        // /select?fq=
        // {!join from=join_field_of_A to=join_field_of_B fromIndex=collection_B}join_field_of_B:value1
        // AND
        // {!join from=join_field_of_A to=join_field_of_C fromIndex=collection_C}join_field_of_C:value2

        // Clinical analysis filters
        filters = clinicalAnalysisFilters(query);
        join = "{!join from=id to=caId fromIndex=" + getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical interpretation filters
        filters = clinicalInterpretationFilters(query);
        join = "{!join from=id to=ciId fromIndex=" + getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant filters
        filters = clinicalVariantFilters(query);
        join = "{!join from=id to=cvId fromIndex=" + getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant evidences filters
        filters = clinicalVariantEvidenceFilters(query);
        addStringFilters(filters, solrQuery);

        // Return Solr query
        logger.info("Solr query: {}", solrQuery.toQueryString());

        return solrQuery;
    }
}
