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
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalIncludeHandler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.util.*;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;
import static org.opencb.commons.datastore.core.QueryOptions.EXCLUDE;
import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;

public class ClinicalAnalysisQueryParser extends ClinicalQueryParser {

    public ClinicalAnalysisQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        super(variantStorageMetadataManager);
    }

    @Override
    public SolrQuery parse(Query query, QueryOptions queryOptions) throws CvdbException {
        String projectId = query.getString(ParamConstants.PROJECT_PARAM_NAME);

        SolrQuery solrQuery = new SolrQuery("*:*");

        // Process query options, if necessary
        parseQueryOptions(queryOptions, solrQuery);

        // Process facet, if necessary
        parseFacet(query, queryOptions, solrQuery);

        List<String> filters;
        String join;

        // Clinical analysis filters
        filters = clinicalAnalysisFilters(query);
        addCommonFilters(query, filters);
        addStringFilters(filters, solrQuery);

        // Clinical interpretation filters
        filters = clinicalInterpretationFilters(query);
        join = "{!join from=caId to=id fromIndex=" + getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant filters
        filters = clinicalVariantFilters(query);
        join = "{!join from=caId to=id fromIndex=" + getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant evidences filters
        filters = clinicalVariantEvidenceFilters(query);
        join = "{!join from=caId to=id fromIndex=" + getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Return Solr query
        logger.info("Solr query: {}", solrQuery.toQueryString());

        return solrQuery;
    }

    @Override
    protected void parseQueryOptions(QueryOptions queryOptions, SolrQuery solrQuery) {
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            // Nothing to do
            return;
        }

        // Parse common options
        super.parseQueryOptions(queryOptions, solrQuery);

        // Parse include and exclude options to get Solr fields to include
        Set<String> casFields = getCasInclude(queryOptions);
        solrQuery.setFields(StringUtils.join(casFields, ","));
    }

    private Set<String> getCasInclude(QueryOptions queryOptions) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            return getCasIncludeFromInclude(queryOptions.getAsStringList(QueryOptions.INCLUDE));
        }
        if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            return getCasIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
        }
        return Collections.singleton("fullJson");
    }

    private Set<String> getCasIncludeFromInclude(List<String> caFields) {
        boolean useLiteJson = false;
        Set<String> casFields = new HashSet<>();
        for (String caField : caFields) {
            if (ClinicalIncludeHandler.caToCasFieldMap.containsKey(caField)) {
                casFields.add(ClinicalIncludeHandler.caToCasFieldMap.get(caField));
            } else if (needsFullJson(caField)) {
                return Collections.singleton("fullJson");
            } else {
                useLiteJson = true;
            }
        }
        if (useLiteJson) {
            return Collections.singleton("liteJson");
        }
        return casFields;
    }

    private Set<String> getCasIncludeFromExclude(List<String> caFields) {
        if (caFields.contains("panels")
                && caFields.contains("interpretation.panels")
                && caFields.contains("secondaryInterpretations.panels")) {
            return Collections.singleton("liteJson");
        }
        return Collections.singleton("fullJson");
    }

    private boolean needsFullJson(String caField) {
        // Checking:
        //     panels.variants | genes | strs | regions
        //     interpretation.panels.variants | genes | strs | regions
        //     secondaryInterpretations.panels.variants | genes | strs | regions
        return (caField.equals("panels")
                || caField.startsWith("panels.variants")
                || caField.startsWith("panels.genes")
                || caField.startsWith("panels.strs")
                || caField.startsWith("panels.regions")
                || caField.equals("interpretation.panels")
                || caField.startsWith("interpretation.panels.variants")
                || caField.startsWith("interpretation.panels.genes")
                || caField.startsWith("interpretation.panels.strs")
                || caField.startsWith("interpretation.panels.regions")
                || caField.equals("secondaryInterpretations.panels")
                || caField.startsWith("secondaryInterpretations.panels.variants")
                || caField.startsWith("secondaryInterpretations.panels.genes")
                || caField.startsWith("secondaryInterpretations.panels.strs")
                || caField.startsWith("secondaryInterpretations.panels.regions"));
    }
}
