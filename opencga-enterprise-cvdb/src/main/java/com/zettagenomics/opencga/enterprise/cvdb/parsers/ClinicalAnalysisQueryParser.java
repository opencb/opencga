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

public class ClinicalAnalysisQueryParser extends ClinicalQueryParser {

    // Map from clinical analysis fields (keys) to Solr indexed fields (values)
    public static Map<String, String> caToCasFieldMap;

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
        return Collections.singleton("maxJson");
    }

    private Set<String> getCasIncludeFromInclude(List<String> caFields) {
        if (caFields.contains("min")) {
            Collections.singleton("minJson");
        } else if (caFields.contains("medium")) {
            return Collections.singleton("mediumJson");
        }

        boolean useMinJson = false;
        boolean useMediumJson = false;
        Set<String> casFields = new HashSet<>();
        for (String caField : caFields) {
            if (caToCasFieldMap.containsKey(caField)) {
                // This field is stored in a Solr indexed field
                casFields.add(caToCasFieldMap.get(caField));
            } else if (needsMaxJson(caField)) {
                // This field is stored only the maxJson field
                return Collections.singleton("maxJson");
            } else if (containnedInMinJson(caField)) {
                // This field can be retrieved from the minJson field
                useMinJson = true;
            } else {
                // Otherwise, use the medium JSON field
                useMediumJson = true;
            }
        }
        if (useMediumJson) {
            return Collections.singleton("mediumJson");
        } else if (useMinJson) {
            return Collections.singleton("minJson");
        }
        return casFields;
    }

    private Set<String> getCasIncludeFromExclude(List<String> caFields) {
        if (caFields.contains("panels")
                && caFields.contains("interpretation.panels")
                && caFields.contains("secondaryInterpretations.panels")) {
            return Collections.singleton("mediumJson");
        }
        return Collections.singleton("maxJson");
    }

    private boolean needsMaxJson(String caField) {
        // Checking fields for using the maximum JSON:
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


    private boolean containnedInMinJson(String caField) {
        // Checking fields for using the minimum JSON
        return (caField.equals("disorder.id")
                || caField.equals("disorder.name")
                || caField.equals("disorder.description")
                || caField.equals("disorder.source")
                || caField.equals("disorder.url")
                || caField.equals("files.id")
                || caField.equals("files.name")
                || caField.equals("proband.id")
                || caField.equals("proband.sex")
                || caField.equals("proband.samples.id")
                || caField.equals("family.id")
                || caField.equals("family.name")
                || caField.equals("family.members.id")
                || caField.equals("family.members.sex")
                || caField.equals("family.members.samples.id")
                || caField.equals("family.panels.id")
                || caField.equals("family.panels.name")
                || caField.equals("family.panels.source")
                || caField.equals("family.panels.stats")
                || caField.equals("panels.id")
                || caField.equals("panels.name")
                || caField.equals("panels.source")
                || caField.equals("panels.stats")
                || caField.equals("interpretation.id")
                || caField.equals("interpretation.method")
                || caField.equals("interpretation.stats")
                || caField.startsWith("consent")
                || caField.startsWith("analyst")
                || caField.startsWith("analysts")
                || caField.startsWith("report")
                || caField.startsWith("request")
                || caField.startsWith("responsible")
                || caField.startsWith("priority")
                || caField.startsWith("flags")
                || caField.equals("creationDate")
                || caField.equals("modificationDate")
                || caField.equals("dueDate")
                || caField.equals("release")
                || caField.startsWith("qualityControl")
                || caField.startsWith("comments")
                || caField.startsWith("audit")
                || caField.startsWith("internal")
                || caField.startsWith("attributes")
                || caField.startsWith("status"));
    }

    static {
        // Map from clinical analysis fields to Solr indexed fields
        caToCasFieldMap = new HashMap<>();
        caToCasFieldMap.put("id", "id");
        caToCasFieldMap.put("description", "description");
        caToCasFieldMap.put("type", "type");
        caToCasFieldMap.put("disorder.id", "disorderId");
        caToCasFieldMap.put("files.name", "fileNames");
        caToCasFieldMap.put("proband.id", "probandId");
        caToCasFieldMap.put("family.id", "familyId");
        caToCasFieldMap.put("family.phenotypes.name", "familyPhenotypeNames");
        caToCasFieldMap.put("family.members.id", "familyMemberIds");
        caToCasFieldMap.put("panels.id", "panelIds");
        caToCasFieldMap.put("report.discussion.text", "report");
        caToCasFieldMap.put("status.id", "status");
        caToCasFieldMap.put("locked", "locked");
    }
}
