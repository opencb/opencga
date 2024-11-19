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

import java.util.*;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;

public class ClinicalVariantEvidenceQueryParser extends ClinicalQueryParser {

    // Map from clinical variant fields (keys) to Solr indexed fields (values)
    public static Map<String, List<String>> cveToCvesFieldMap;

    public ClinicalVariantEvidenceQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
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
        join = "{!join from=id to=cvId fromIndex=" + getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant evidences filters
        filters = clinicalVariantEvidenceFilters(query);
        addCommonFilters(query, filters);
        addStringFilters(filters, solrQuery);

        // Log queries
        logQueries(query, queryOptions, solrQuery, "Clinical variant evidence");

        // Return Solr query
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
        Set<String> cvesFields = getCvesInclude(queryOptions);
        solrQuery.setFields(StringUtils.join(cvesFields, ","));
    }

    private Set<String> getCvesInclude(QueryOptions queryOptions) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            return getCvesIncludeFromInclude(queryOptions.getAsStringList(QueryOptions.INCLUDE));
        }
        if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            return getCvesIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
        }
        return Collections.singleton("json");
    }

    private Set<String> getCvesIncludeFromInclude(List<String> cveFields) {
        Set<String> cvsFields = new HashSet<>();
        for (String cveField : cveFields) {
            if (cveToCvesFieldMap.containsKey(cveField)) {
                // This field is stored in a Solr indexed field
                cvsFields.addAll(cveToCvesFieldMap.get(cveField));
            } else {
                return Collections.singleton("json");
            }
        }
        return cvsFields;
    }

    private Set<String> getCvesIncludeFromExclude(List<String> caFields) {
        return Collections.singleton("json");
    }

    static {
        // Map from clinical analysis fields to Solr indexed fields
        cveToCvesFieldMap = new HashMap<>();
        cveToCvesFieldMap.put("genomicFeature.geneName", Arrays.asList("geneName"));
        cveToCvesFieldMap.put("panelId", Arrays.asList("panelId"));
        cveToCvesFieldMap.put("modeOfInheritances", Arrays.asList("mois"));
        cveToCvesFieldMap.put("classification.acmg", Arrays.asList("acmgs"));
        cveToCvesFieldMap.put("classification.tier", Arrays.asList("tier"));
        cveToCvesFieldMap.put("classification.clinicalSignificance", Arrays.asList("clinicalSignificance"));
    }
}
