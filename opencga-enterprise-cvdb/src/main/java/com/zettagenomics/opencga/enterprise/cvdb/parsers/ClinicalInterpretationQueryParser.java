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
import com.zettagenomics.opencga.enterprise.cvdb.CvdbUtils;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalIncludeHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;

import java.util.*;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;

public class ClinicalInterpretationQueryParser extends ClinicalQueryParser {

    // Map from clinical interpretation fields (keys) to Solr indexed fields (values)
    public static Map<String, List<String>> ciToCisFieldMap;

    public ClinicalInterpretationQueryParser(String collectionPrefix, SearchIndexMetadata indexMetadata) {
        super(collectionPrefix, indexMetadata);
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
        join = "{!join from=id to=caId fromIndex=" + CvdbUtils.getCollectionName(collectionPrefix, projectId,
                CLINICAL_ANALYSES_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical interpretation filters
        filters = clinicalInterpretationFilters(query);
        addCommonFilters(query, filters);
        addStringFilters(filters, solrQuery);

        // Clinical variant filters
        filters = clinicalVariantFilters(query);
        join = "{!join from=ciId to=id fromIndex=" + CvdbUtils.getCollectionName(collectionPrefix, projectId,
                CLINICAL_VARIANTS_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant evidences filters
        filters = clinicalVariantEvidenceFilters(query);
        join = "{!join from=ciId to=id fromIndex=" + CvdbUtils.getCollectionName(collectionPrefix, projectId,
                CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX) + "}";
        addStringFilters(filters, join, solrQuery);

        // Log queries
        logQueries(query, queryOptions, solrQuery, "Clinical interpretation");

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
        Set<String> cisFields = getCisInclude(queryOptions);
        solrQuery.setFields(StringUtils.join(cisFields, ","));
    }

    private Set<String> getCisInclude(QueryOptions queryOptions) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            return getCisIncludeFromInclude(queryOptions.getAsStringList(QueryOptions.INCLUDE));
        }
        if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            return getCisIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
        }
        return Collections.singleton("maxJson");
    }

    private Set<String> getCisIncludeFromInclude(List<String> caFields) {
        if (caFields.contains(ClinicalIncludeHandler.INTERNAL_INCLUDE_MINIMUM_JSON)) {
            Collections.singleton("minJson");
        } else if (caFields.contains(ClinicalIncludeHandler.INTERNAL_INCLUDE_MEDIUM_JSON)) {
            return Collections.singleton("mediumJson");
        }

        boolean useMinJson = false;
        boolean useMediumJson = false;
        Set<String> casFields = new HashSet<>();
        for (String caField : caFields) {
            if (ciToCisFieldMap.containsKey(caField)) {
                // This field is stored in a Solr indexed field
                casFields.addAll(ciToCisFieldMap.get(caField));
            } else if (needsMaxJson(caField)) {
                // This field is stored only the maximum JSON field
                return Collections.singleton("maxJson");
            } else if (needsMediumJson(caField)) {
                // This field can be retrieved from the medium JSON field
                useMediumJson = true;
            } else {
                // Otherwise, use the minimum JSON field
                useMinJson = true;
            }
        }
        if (useMediumJson) {
            return Collections.singleton("mediumJson");
        } else if (useMinJson) {
            return Collections.singleton("minJson");
        }
        return casFields;
    }

    private Set<String> getCisIncludeFromExclude(List<String> ciFields) {
        if (ciFields.contains("panels")) {
            return Collections.singleton("mediumJson");
        }
        return Collections.singleton("maxJson");
    }

    private boolean needsMaxJson(String ciField) {
        // Checking fields for using the maximum JSON:
        //     panels.variants | genes | strs | regions
        return (ciField.equals("panels")
                || ciField.startsWith("panels.variants")
                || ciField.startsWith("panels.genes")
                || ciField.startsWith("panels.strs")
                || ciField.startsWith("panels.regions"));
    }

    private boolean needsMediumJson(String caField) {
        // Checking fields for using the medium JSON:
        //    primaryFindings.annotation
        //    secondaryFindings.annotation
        return (caField.equals("primaryFindings")
                || caField.equals("secondaryFindings")
                || caField.startsWith("primaryFindings.annotation")
                || caField.startsWith("secondaryFindings.annotation"));
    }

    static {
        // Map from clinical interpretation fields to Solr indexed fields
        ciToCisFieldMap = new HashMap<>();
        ciToCisFieldMap.put("id", Arrays.asList("id"));
        ciToCisFieldMap.put("description", Arrays.asList("description"));
        ciToCisFieldMap.put("panels.id", Arrays.asList("panelIds"));
        ciToCisFieldMap.put("analyst", Arrays.asList("analystId", "analystName", "analystEmail", "analystAssignedBy", "analystDate"));
        ciToCisFieldMap.put("analyst.id", Arrays.asList("analystId"));
        ciToCisFieldMap.put("analyst.name", Arrays.asList("analystName"));
        ciToCisFieldMap.put("analyst.email", Arrays.asList("analystEmail"));
        ciToCisFieldMap.put("analyst.assignedBy", Arrays.asList("analystAssignedBy"));
        ciToCisFieldMap.put("analyst.date", Arrays.asList("analystDate"));
        ciToCisFieldMap.put("method", Arrays.asList("methodName", "methodVersion", "methodCommit", "methodDependencies"));
        ciToCisFieldMap.put("method.name", Arrays.asList("methodName"));
        ciToCisFieldMap.put("method.version", Arrays.asList("methodVersion"));
        ciToCisFieldMap.put("method.commit", Arrays.asList("methodCommit"));
        ciToCisFieldMap.put("method.dependencies", Arrays.asList("methodDependencies"));
        ciToCisFieldMap.put("comments", Arrays.asList("comments"));
        ciToCisFieldMap.put("locked", Arrays.asList("locked"));
        ciToCisFieldMap.put("status", Arrays.asList("statusId", "statusName", "statusDescription", "statusDate"));
        ciToCisFieldMap.put("status.id", Arrays.asList("statusId"));
        ciToCisFieldMap.put("status.name", Arrays.asList("statusName"));
        ciToCisFieldMap.put("status.description", Arrays.asList("statusDescription"));
        ciToCisFieldMap.put("status.date", Arrays.asList("statusDate"));
        ciToCisFieldMap.put("creationDate", Arrays.asList("creationDate"));
        ciToCisFieldMap.put("modificationDate", Arrays.asList("modificationDate"));
        ciToCisFieldMap.put("version", Arrays.asList("version"));
    }
}
