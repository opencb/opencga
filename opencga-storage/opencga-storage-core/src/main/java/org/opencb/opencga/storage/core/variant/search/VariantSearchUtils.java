/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.variant.search;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

/**
 * Created on 06/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSearchUtils {

    public static final String FIELD_SEPARATOR = "__";

    public static final Set<QueryParam> UNSUPPORTED_QUERY_PARAMS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(VariantQueryParam.FILE,
                    VariantQueryParam.FILTER,
                    VariantQueryParam.QUAL,
                    VariantQueryParam.SAMPLE_DATA,
                    VariantQueryParam.FILE_DATA,
                    VariantQueryParam.GENOTYPE,
                    VariantQueryParam.SAMPLE,
                    SAMPLE_MENDELIAN_ERROR,
                    SAMPLE_DE_NOVO,
                    SAMPLE_DE_NOVO_STRICT,
                    SAMPLE_COMPOUND_HETEROZYGOUS,
                    VariantQueryParam.COHORT,
                    VariantQueryParam.STATS_MGF,
                    VariantQueryParam.MISSING_ALLELES,
                    VariantQueryParam.MISSING_GENOTYPES,
                    VariantQueryParam.ANNOT_DRUG)));

    public static final Set<QueryParam> UNSUPPORTED_MODIFIERS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(VariantQueryParam.INCLUDE_FILE,
                    VariantQueryParam.INCLUDE_SAMPLE,
                    VariantQueryParam.INCLUDE_SAMPLE_ID,
//                    VariantQueryParam.INCLUDE_STUDY,
                    VariantQueryParam.UNKNOWN_GENOTYPE,
                    VariantQueryParam.INCLUDE_SAMPLE_DATA,
                    VariantQueryParam.INCLUDE_GENOTYPE,
                    VariantQueryParam.SAMPLE_METADATA,
                    VariantQueryParam.SAMPLE_LIMIT,
                    VariantQueryParam.SAMPLE_SKIP
                    // RETURNED_COHORTS
            )));

    private static final List<VariantField> UNSUPPORTED_VARIANT_FIELDS =
            Arrays.asList(
                    VariantField.STUDIES,
                    VariantField.ANNOTATION);

    private static final Set<String> ACCEPTED_FORMAT_FILTERS = Collections.singleton("DP");

    public static boolean isQueryCovered(Query query) {
        for (QueryParam nonCoveredParam : UNSUPPORTED_QUERY_PARAMS) {
            if (isValidParam(query, nonCoveredParam)) {
                return false;
            }
        }
        return true;
    }

    public static Collection<VariantQueryParam> coveredParams(Query query) {
        Set<VariantQueryParam> params = validParams(query);
        return coveredParams(params);
    }

    public static List<VariantQueryParam> coveredParams(Collection<VariantQueryParam> params) {
        List<VariantQueryParam> coveredParams = new ArrayList<>();

        for (VariantQueryParam param : params) {
            if (!UNSUPPORTED_QUERY_PARAMS.contains(param) && !UNSUPPORTED_MODIFIERS.contains(param)) {
                coveredParams.add(param);
            }
        }
        return coveredParams;
    }

    public static Collection<VariantQueryParam> uncoveredParams(Query query) {
        Set<VariantQueryParam> params = validParams(query);
        return uncoveredParams(params);
    }

    public static List<VariantQueryParam> uncoveredParams(Collection<VariantQueryParam> params) {
        List<VariantQueryParam> uncoveredParams = new ArrayList<>();

        for (VariantQueryParam param : params) {
            if (UNSUPPORTED_QUERY_PARAMS.contains(param) || UNSUPPORTED_MODIFIERS.contains(param)) {
                uncoveredParams.add(param);
            }
        }
        return uncoveredParams;
    }

    public static boolean isIncludeCovered(QueryOptions options) {
        Set<VariantField> returnedFields = VariantField.getIncludeFields(options);
        for (VariantField unsupportedVariantField : UNSUPPORTED_VARIANT_FIELDS) {
            if (returnedFields.contains(unsupportedVariantField)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Copy the given query and remove all uncovered params.
     *
     * @param query Query
     * @return      Query for the search engine
     */
    public static Query getSearchEngineQuery(Query query) {
        Collection<VariantQueryParam> uncoveredParams = uncoveredParams(query);
        Query searchEngineQuery = new Query(query);
        for (VariantQueryParam uncoveredParam : uncoveredParams) {
            searchEngineQuery.remove(uncoveredParam.key());
        }
        searchEngineQuery.put(INCLUDE_SAMPLE.key(), NONE);
        searchEngineQuery.put(INCLUDE_FILE.key(), NONE);
        searchEngineQuery.put(INCLUDE_SAMPLE_DATA.key(), NONE);
        searchEngineQuery.put(INCLUDE_GENOTYPE.key(), false);
        return searchEngineQuery;
    }

    public static Query getEngineQuery(Query query, QueryOptions options, VariantStorageMetadataManager scm) {
        Collection<VariantQueryParam> uncoveredParams = uncoveredParams(query);
        Query engineQuery = new Query();
        for (VariantQueryParam uncoveredParam : uncoveredParams) {
            engineQuery.put(uncoveredParam.key(), query.get(uncoveredParam.key()));
        }
        // Make sure that all modifiers are present in the engine query
        for (VariantQueryParam modifierParam : MODIFIER_QUERY_PARAMS) {
            engineQuery.putIfNotNull(modifierParam.key(), query.get(modifierParam.key()));
        }
        for (QueryParam modifierParam : MODIFIER_INTERNAL_QUERY_PARAMS) {
            engineQuery.putIfNotNull(modifierParam.key(), query.get(modifierParam.key()));
        }
        // Despite STUDIES is a covered filter, it has to be in the underlying
        // query to be used as defaultStudy
        if (isValidParam(query, STUDY)) {
            if (!uncoveredParams.isEmpty()) {
                // This will set the default study, if needed
                engineQuery.put(STUDY.key(), query.get(STUDY.key()));
            } else if (!isValidParam(query, INCLUDE_STUDY)) {
                // If returned studies is not defined, we need to define it with the values from STUDIES
                List<Integer> studies = VariantQueryProjectionParser.getIncludeStudies(query, options, scm);
                engineQuery.put(INCLUDE_STUDY.key(), studies);
            }
        }
        return engineQuery;
    }

}
