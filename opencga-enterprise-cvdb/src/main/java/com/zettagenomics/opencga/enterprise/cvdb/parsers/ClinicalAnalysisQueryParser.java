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

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalIncludeHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.util.*;

public class ClinicalAnalysisQueryParser extends ClinicalQueryParser {

    // Map from clinical analysis fields (keys) to Solr indexed fields (values)
    public static Map<String, String> caToCasFieldMap;

    public ClinicalAnalysisQueryParser(String collectionPrefix, VariantStorageMetadataManager variantStorageMetadataManager) {
        super(collectionPrefix, variantStorageMetadataManager);
    }

    @Override
    public SolrQuery parse(Query query, QueryOptions queryOptions) throws CvdbException {
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
        join = "{!join from=caId to=id fromIndex=" + ciCollectionName + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant filters
        filters = clinicalVariantFilters(query);
        join = "{!join from=caId to=id fromIndex=" + cvCollectionName + "}";
        addStringFilters(filters, join, solrQuery);

        // Clinical variant evidences filters
        filters = clinicalVariantEvidenceFilters(query);
        join = "{!join from=caId to=id fromIndex=" + cveCollectionName + "}";
        addStringFilters(filters, join, solrQuery);

        // Viewers
        addViewerFilter(query, "id", solrQuery);

        // Log queries
        logQueries(query, queryOptions, solrQuery, "Clinical analysis");

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
        if (caFields.contains(ClinicalIncludeHandler.INTERNAL_INCLUDE_MINIMUM_JSON)) {
            Collections.singleton("minJson");
        } else if (caFields.contains(ClinicalIncludeHandler.INTERNAL_INCLUDE_MEDIUM_JSON)) {
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

    private Set<String> getCasIncludeFromExclude(List<String> caFields) {
        if (caFields.contains("panels")
                && caFields.contains("interpretation.panels")
                && caFields.contains("secondaryInterpretations.panels")) {
            if ((caFields.contains("interpretation") || caFields.contains("interpretation.primaryFindings")
                    || caFields.contains("interpretation.primaryFindings.annotation"))
                    && (caFields.contains("secondaryInterpretations") || caFields.contains("secondaryInterpretations.primaryFindings")
                    || caFields.contains("secondaryInterpretations.primaryFindings.annotation"))) {
                return Collections.singleton("minJson");
            } else {
                return Collections.singleton("mediumJson");
            }
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

    private boolean needsMediumJson(String caField) {
        // Checking fields for using the medium JSON:
        //     interpretation.primaryFindings.annotation
        //     interpretation.panels.variants | genes | strs | regions
        //     secondaryInterpretations.panels.variants | genes | strs | regions
        return (caField.equals("interpretation")
                || caField.equals("interpretation.primaryFindings")
                || caField.equals("interpretation.secondaryFindings")
                || caField.startsWith("interpretation.primaryFindings.annotation")
                || caField.startsWith("interpretation.secondaryFindings.annotation")
                || caField.equals("secondaryInterpretations")
                || caField.equals("secondaryInterpretations.primaryFindings")
                || caField.equals("secondaryInterpretations.secondaryFindings")
                || caField.startsWith("secondaryInterpretations.primaryFindings.annotation")
                || caField.startsWith("secondaryInterpretations.secondaryFindings.annotation"));
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
        caToCasFieldMap.put("proband.disorder.id", "probandDisorderIds");
        caToCasFieldMap.put("proband.phenotype.id", "probandPhenotypeIds");
        caToCasFieldMap.put("family.id", "familyId");
        caToCasFieldMap.put("family.phenotypes.name", "familyPhenotypeNames");
        caToCasFieldMap.put("family.members.id", "familyMemberIds");
        caToCasFieldMap.put("panels.id", "panelIds");
        caToCasFieldMap.put("report.discussion.text", "report");
        caToCasFieldMap.put("status.id", "status");
        caToCasFieldMap.put("locked", "locked");
    }
}
