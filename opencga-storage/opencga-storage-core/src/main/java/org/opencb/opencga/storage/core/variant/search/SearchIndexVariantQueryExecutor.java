package org.opencb.opencga.storage.core.variant.search;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.search.solr.SolrNativeIterator;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.MODIFIER_QUERY_PARAMS;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.*;
import static org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager.SEARCH_ENGINE_ID;

/**
 * Query executor that joins results from {@link VariantSearchManager} and the underlying {@link VariantDBAdaptor}.
 *
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SearchIndexVariantQueryExecutor extends AbstractSearchIndexVariantQueryExecutor {

    private Logger logger = LoggerFactory.getLogger(SearchIndexVariantQueryExecutor.class);
    private boolean intersectActive;
    private boolean intersectAlways;
    private int intersectParamsThreshold;

    public SearchIndexVariantQueryExecutor(VariantDBAdaptor dbAdaptor, VariantSearchManager searchManager,
                                           String storageEngineId, String dbName, StorageConfiguration configuration,
                                           ObjectMap options) {
        super(dbAdaptor, searchManager, storageEngineId, dbName, configuration, options);
        intersectActive = getOptions().getBoolean(INTERSECT_ACTIVE.key(), INTERSECT_ACTIVE.defaultValue());
        intersectAlways = getOptions().getBoolean(INTERSECT_ALWAYS.key(), INTERSECT_ALWAYS.defaultValue());
        intersectParamsThreshold = getOptions().getInt(INTERSECT_PARAMS_THRESHOLD.key(), INTERSECT_PARAMS_THRESHOLD.defaultValue());
    }

    public SearchIndexVariantQueryExecutor setIntersectActive(boolean intersectActive) {
        this.intersectActive = intersectActive;
        return this;
    }

    public SearchIndexVariantQueryExecutor setIntersectAlways(boolean intersectAlways) {
        this.intersectAlways = intersectAlways;
        return this;
    }

    public SearchIndexVariantQueryExecutor setIntersectParamsThreshold(int intersectParamsThreshold) {
        this.intersectParamsThreshold = intersectParamsThreshold;
        return this;
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) throws StorageEngineException {
        return doQuerySearchManager(query, options) || doIntersectWithSearch(query, options);
    }

    @Override
    public DataResult<Long> count(Query query) {
        try {
            StopWatch watch = StopWatch.createStarted();
            long count = searchManager.count(dbName, query);
            int time = (int) watch.getTime(TimeUnit.MILLISECONDS);
            return new DataResult<>(time, Collections.emptyList(), 1, Collections.singletonList(count), 1);
        } catch (IOException | VariantSearchException e) {
            throw new VariantQueryException("Error querying Solr", e);
        }
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) {
        if (options == null) {
            options = QueryOptions.empty();
        }

        if (doQuerySearchManager(query, options)) {
            try {
                if (iterator) {
                    return searchManager.iterator(dbName, query, options);
                } else {
                    return searchManager.query(dbName, query, options);
                }
            } catch (IOException | VariantSearchException e) {
                throw new VariantQueryException("Error querying Solr", e);
            }
        } else {
            // Intersect Solr+Engine

            int limit = options.getInt(QueryOptions.LIMIT, Integer.MAX_VALUE);
            int skip = options.getInt(QueryOptions.SKIP, 0);
            boolean pagination = !iterator || skip > 0;

            Iterator<?> variantsIterator;
            Number numTotalResults = null;
            AtomicLong searchCount = null;
            Boolean approxCount = null;
            Integer approxCountSamplingSize = null;

            Query searchEngineQuery = getSearchEngineQuery(query);
            Query engineQuery = getEngineQuery(query, options, getMetadataManager());

            // Do not count for iterator
            if (!iterator) {
                if (isQueryCovered(query)) {
                    // If the query is fully covered, the numTotalResults from solr is correct.
                    searchCount = new AtomicLong();
                    numTotalResults = searchCount;
                    // Skip count in storage. We already know the numTotalResults
                    options.put(QueryOptions.COUNT, false);
                    approxCount = false;
                } else if (options.getBoolean(APPROXIMATE_COUNT.key()) || options.getBoolean(QueryOptions.COUNT)) {
                    options.put(QueryOptions.COUNT, false);
                    VariantQueryResult<Long> result = approximateCount(query, options);
                    numTotalResults = result.first();
                    approxCount = result.getApproximateCount();
                    approxCountSamplingSize = result.getApproximateCountSamplingSize();
                }
            }

            if (pagination) {
                if (isQueryCovered(query)) {
                    // We can use limit+skip directly in solr
                    variantsIterator = variantIdIteratorFromSearch(searchEngineQuery, limit, skip, searchCount);

                    // Remove limit and skip from Options for storage. The Search Engine already knows the pagination.
                    options = new QueryOptions(options);
                    options.remove(QueryOptions.LIMIT);
                    options.remove(QueryOptions.SKIP);
                } else {
                    logger.debug("Client side pagination. limit : {} , skip : {}", limit, skip);
                    // Can't limit+skip only from solr. Need to limit+skip also in client side
                    variantsIterator = variantIdIteratorFromSearch(searchEngineQuery);
                }
            } else {
                variantsIterator = variantIdIteratorFromSearch(searchEngineQuery, Integer.MAX_VALUE, 0, searchCount);
            }

            logger.debug("Intersect query " + engineQuery.toJson() + " options " + options.toJson());
            if (iterator) {
                return dbAdaptor.iterator(variantsIterator, engineQuery, options);
            } else {
                setDefaultTimeout(options);
                VariantQueryResult<Variant> queryResult = dbAdaptor.get(variantsIterator, engineQuery, options);
                if (numTotalResults != null) {
                    queryResult.setApproximateCount(approxCount);
                    queryResult.setApproximateCountSamplingSize(approxCountSamplingSize);
                    queryResult.setNumMatches(numTotalResults.longValue());
                }
                queryResult.setSource(SEARCH_ENGINE_ID + '+' + getStorageEngineId());
                return queryResult;
            }
        }
    }

    public VariantQueryResult<Long> approximateCount(Query query, QueryOptions options) {
        long count;
        boolean approxCount = true;
        int sampling = 0;
        StopWatch watch = StopWatch.createStarted();
        try {
            if (doQuerySearchManager(query, new QueryOptions(QueryOptions.COUNT, true))) {
                approxCount = false;
                count = searchManager.count(dbName, query);
            } else {
                sampling = options.getInt(APPROXIMATE_COUNT_SAMPLING_SIZE.key(),
                        getOptions().getInt(APPROXIMATE_COUNT_SAMPLING_SIZE.key(), APPROXIMATE_COUNT_SAMPLING_SIZE.defaultValue()));
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, sampling);

                Query searchEngineQuery = getSearchEngineQuery(query);
                Query engineQuery = getEngineQuery(query, options, getMetadataManager());

                VariantQueryResult<VariantSearchModel> nativeResult = searchManager
                        .nativeQuery(dbName, searchEngineQuery, queryOptions);
                List<String> variantIds = nativeResult.getResults().stream().map(VariantSearchModel::getId).collect(Collectors.toList());
                // Adjust numSamples if the results from SearchManager is smaller than numSamples
                // If this happens, the count is not approximated
                if (variantIds.size() < sampling) {
                    approxCount = false;
                    sampling = variantIds.size();
                }
                long numSearchResults = nativeResult.getNumTotalResults();

                long numResults;
                if (variantIds.isEmpty()) {
                    // Do not count if empty. It will not apply the filter and count through the whole database.
                    numResults = 0;
                } else {
                    engineQuery.put(VariantQueryUtils.ID_INTERSECT.key(), variantIds);
                    numResults = dbAdaptor.count(engineQuery).first();
                }
                logger.debug("NumResults: {}, NumSearchResults: {}, NumSamples: {}", numResults, numSearchResults, sampling);
                if (approxCount) {
                    count = (long) ((numResults / (float) sampling) * numSearchResults);
                } else {
                    count = numResults;
                }
            }
        } catch (IOException | VariantSearchException e) {
            throw new VariantQueryException("Error querying Solr", e);
        }
        int time = (int) watch.getTime(TimeUnit.MILLISECONDS);
        return new VariantQueryResult<>(time, 1, 1, Collections.emptyList(), Collections.singletonList(count), null,
                SEARCH_ENGINE_ID + '+' + getStorageEngineId(), approxCount, approxCount ? sampling : null, null);
    }

    /**
     * Decide if a query should be resolved using SearchManager or not.
     *
     * @param query     Query
     * @param options   QueryOptions
     * @return          true if should resolve only with SearchManager
     */
    public boolean doQuerySearchManager(Query query, QueryOptions options) {
        if (VariantStorageEngine.UseSearchIndex.from(options).equals(VariantStorageEngine.UseSearchIndex.NO)) {
            return false;
        } // else, YES or AUTO
        if (isQueryCovered(query) && isIncludeCovered(options)) {
            if (searchActiveAndAlive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Decide if a query should be resolved intersecting with SearchManager or not.
     *
     * @param query       Query
     * @param options     QueryOptions
     * @return            true if should intersect
     */
    public boolean doIntersectWithSearch(Query query, QueryOptions options) {
        VariantStorageEngine.UseSearchIndex useSearchIndex = VariantStorageEngine.UseSearchIndex.from(options);

        final boolean intersect;
        boolean active = searchActiveAndAlive();
        if (useSearchIndex.equals(VariantStorageEngine.UseSearchIndex.NO)) {
            // useSearchIndex = NO
            intersect = false;
        } else if (!intersectActive) {
            // If intersect is not active, do not intersect.
            intersect = false;
        } else if (intersectAlways) {
            // If always intersect, intersect if available
            intersect = active;
        } else if (!active) {
            intersect = false;
        } else if (useSearchIndex.equals(VariantStorageEngine.UseSearchIndex.YES)
                || VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_TRAIT)) {
            intersect = true;
        } else {
            if (options.getBoolean(QueryOptions.COUNT)) {
                intersect = true;
            } else {
                // TODO: Improve this heuristic
                // Count only real params
                Collection<VariantQueryParam> coveredParams = coveredParams(query);
                coveredParams.removeAll(MODIFIER_QUERY_PARAMS);
                intersect = coveredParams.size() >= intersectParamsThreshold;
            }
        }

        if (!intersect) {
            if (useSearchIndex.equals(VariantStorageEngine.UseSearchIndex.YES)) {
                throw new VariantQueryException("Unable to use search index. SearchEngine is not available");
            } else if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_TRAIT)) {
                throw VariantQueryException.unsupportedVariantQueryFilter(VariantQueryParam.ANNOT_TRAIT, getStorageEngineId(),
                        "Search engine is required.");
            }
        }
        return intersect;
    }

    protected Iterator<String> variantIdIteratorFromSearch(Query query) {
        return variantIdIteratorFromSearch(query, Integer.MAX_VALUE, 0, null);
    }

    protected Iterator<String> variantIdIteratorFromSearch(Query query, int limit, int skip, AtomicLong numTotalResults) {
        Iterator<String> variantsIterator;
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.LIMIT, limit)
                .append(QueryOptions.SKIP, skip)
                .append(QueryOptions.INCLUDE, VariantField.ID.fieldName());
        try {
            // Do not iterate for small queries
            if (limit < 10000) {
                VariantQueryResult<VariantSearchModel> nativeResult = searchManager.nativeQuery(dbName, query, queryOptions);
                if (numTotalResults != null) {
                    numTotalResults.set(nativeResult.getNumMatches());
                }
                variantsIterator = nativeResult.getResults()
                        .stream()
                        .map(VariantSearchModel::getId)
                        .iterator();
            } else {
                SolrNativeIterator nativeIterator = searchManager.nativeIterator(dbName, query, queryOptions);
                if (numTotalResults != null) {
                    numTotalResults.set(nativeIterator.getNumFound());
                }
                variantsIterator = Iterators.transform(nativeIterator, VariantSearchModel::getId);
            }
        } catch (VariantSearchException | IOException e) {
            throw new VariantQueryException("Error querying " + VariantSearchManager.SEARCH_ENGINE_ID, e);
        }
        return variantsIterator;
    }

}
