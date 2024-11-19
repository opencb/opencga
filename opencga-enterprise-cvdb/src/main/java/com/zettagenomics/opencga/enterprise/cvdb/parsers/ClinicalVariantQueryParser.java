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
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser;

import java.util.*;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CV_TYPE_NAME;

public class ClinicalVariantQueryParser extends ClinicalQueryParser {

    // Map from clinical variant fields (keys) to Solr indexed fields (values)
    public static Map<String, List<String>> cvToCvsFieldMap;

    private SolrQueryParser solrQueryParser;

    public ClinicalVariantQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
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
        addCommonFilters(query, filters);
        addStringFilters(filters, solrQuery);

        // Clinical variant evidences filters
        filters = clinicalVariantEvidenceFilters(query);
        join = "{!join from=cvId to=id fromIndex=" + getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Log queries
        logQueries(query, queryOptions, solrQuery, "Clinical variant");

        // Return Solr query
        return solrQuery;
    }

    private Query buildVariantQuery(Query query) {
        Query variantQuery = new VariantQuery();
        if (query.containsKey(CV_TYPE_NAME)) {
            variantQuery.put(VariantQueryParam.TYPE.key(), query.get(CV_TYPE_NAME));
        }
        return variantQuery;
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
        Set<String> cvsFields = getCvsInclude(queryOptions);
        solrQuery.setFields(StringUtils.join(cvsFields, ","));
    }

    private Set<String> getCvsInclude(QueryOptions queryOptions) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            return getCvsIncludeFromInclude(queryOptions.getAsStringList(QueryOptions.INCLUDE));
        }
        if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            return getCvsIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
        }
        return Collections.singleton("json");
    }

    private Set<String> getCvsIncludeFromInclude(List<String> cvFields) {
        Set<String> cvsFields = new HashSet<>();
        for (String cvField : cvFields) {
            if (cvToCvsFieldMap.containsKey(cvField)) {
                // This field is stored in a Solr indexed field
                cvsFields.addAll(cvToCvsFieldMap.get(cvField));
            } else {
                return Collections.singleton("json");
            }
        }
        return cvsFields;
    }

    private Set<String> getCvsIncludeFromExclude(List<String> caFields) {
        return Collections.singleton("json");
    }

    static {
        // Map from clinical analysis fields to Solr indexed fields
        cvToCvsFieldMap = new HashMap<>();
        cvToCvsFieldMap.put("discussion", Arrays.asList("discussionAuthor", "discussionDate", "discussionText"));
        cvToCvsFieldMap.put("discussion.author", Arrays.asList("discussionAuthor"));
        cvToCvsFieldMap.put("discussion.date", Arrays.asList("discussionDate"));
        cvToCvsFieldMap.put("discussion.text", Arrays.asList("discussionText"));
        cvToCvsFieldMap.put("confidence.value", Arrays.asList("confidenceValue"));
        cvToCvsFieldMap.put("confidence.author", Arrays.asList("confidenceAuthor"));
        cvToCvsFieldMap.put("confidence.date", Arrays.asList("confidenceDate"));
        cvToCvsFieldMap.put("tags", Arrays.asList("tags"));
        cvToCvsFieldMap.put("status", Arrays.asList("status"));
    }

    private String parseInterpretationInfo(String key, String string) {
        // TODO: parse intInfo
        return "";
    }
}
