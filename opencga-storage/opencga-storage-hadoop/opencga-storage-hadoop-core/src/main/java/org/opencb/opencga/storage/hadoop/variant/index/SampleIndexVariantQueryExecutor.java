package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIteratorWithCounts;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.AbstractTwoPhasedVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ID;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.addSamplesMetadataIfRequested;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexVariantQueryExecutor extends AbstractTwoPhasedVariantQueryExecutor {

    public static final String SAMPLE_INDEX_INTERSECT = "sample_index_intersect";
    public static final String SAMPLE_INDEX_TABLE_SOURCE = "sample_index_table";
    public static final int DEFAULT_SAMPLING_SIZE = 200;
    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final VariantHadoopDBAdaptor dbAdaptor;
    private Logger logger = LoggerFactory.getLogger(SampleIndexVariantQueryExecutor.class);

    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
            .namingPattern("sample-index-async-count-%s")
            .build());

    public SampleIndexVariantQueryExecutor(VariantHadoopDBAdaptor dbAdaptor, SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                           String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options, "Sample Index Table");
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        if (options.getBoolean(SAMPLE_INDEX_INTERSECT, true)) {
            return SampleIndexQueryParser.validSampleIndexQuery(query);
        }
        return false;
    }

    @Override
    public DataResult<Long> count(Query query) {
        throw new UnsupportedOperationException("Count not implemented in " + getClass());
    }

    @Override
    protected long primaryCount(Query query, QueryOptions options) {
        SampleIndexQuery sampleIndexQuery = sampleIndexDBAdaptor.parseSampleIndexQuery(query);
        return sampleIndexDBAdaptor.count(sampleIndexQuery);
    }

    /**
     * Intersect result of SampleIndexTable and full phoenix query.
     * Use {@link org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator}.
     *
     * @param inputQuery Query
     * @param options    Options
     * @param iterator   Shall the resulting object be an iterator instead of a DataResult
     * @return           DataResult or Iterator with the variants that matches the query
     */
    @Override
    protected Object getOrIterator(Query inputQuery, QueryOptions options, boolean iterator) {
        Query query = new Query(inputQuery);
        SampleIndexQuery sampleIndexQuery = sampleIndexDBAdaptor.parseSampleIndexQuery(query);

        return getOrIterator(query, options, iterator, sampleIndexQuery);
    }

    protected Object getOrIterator(Query query, QueryOptions inputOptions, boolean iterator, SampleIndexQuery sampleIndexQuery) {
        logger.info("HBase SampleIndex intersect");
        Future<Long> asyncCountFuture;
        boolean asyncCount;
        if (shouldGetApproximateCount(inputOptions, iterator) && queryFiltersCovered(query)) {
            asyncCount = true;
            asyncCountFuture = THREAD_POOL.submit(() -> {
                StopWatch stopWatch = StopWatch.createStarted();
                long count = sampleIndexDBAdaptor.count(sampleIndexQuery);
                logger.info("Async count took " + TimeUtils.durationToString(stopWatch));
                return count;
            });
        } else {
            asyncCount = false;
            asyncCountFuture = null;
        }

        QueryOptions limitLessOptions = new QueryOptions(inputOptions);
        limitLessOptions.remove(QueryOptions.LIMIT);
        limitLessOptions.remove(QueryOptions.SKIP);
        VariantDBIteratorWithCounts variants = new VariantDBIteratorWithCounts(
                sampleIndexDBAdaptor.iterator(sampleIndexQuery, limitLessOptions));

        int batchSize = inputOptions.getInt("multiIteratorBatchSize", 200);
        if (iterator) {
            // SampleIndex iterator will be closed when closing the variants iterator
            return dbAdaptor.iterator(variants, query, inputOptions, batchSize);
        } else {
            int skip = getSkip(inputOptions);
            int limit = getLimit(inputOptions);
            int samplingSize = asyncCount ? 0 : getSamplingSize(inputOptions, DEFAULT_SAMPLING_SIZE, iterator);
            int tmpLimit = Math.max(limit, samplingSize);

            QueryOptions options = new QueryOptions(inputOptions);
            // Ensure results are sorted and it's not counting from variants dbAdaptor
            options.put(QueryOptions.SORT, true);
            options.put(QueryOptions.COUNT, false);
            options.put(QueryOptions.LIMIT, tmpLimit);

            MultiVariantDBIterator variantDBIterator = dbAdaptor.iterator(
                    new org.opencb.opencga.storage.core.variant.adaptors.iterators.DelegatedVariantDBIterator(variants) {
                        @Override
                        public void close() throws Exception {
                            // Do not close this iterator! We'll need to keep iterating to get the approximate count
                        }
                    }, query, options, batchSize);
            VariantQueryResult<Variant> result =
                    addSamplesMetadataIfRequested(variantDBIterator.toDataResult(), query, options, getMetadataManager());

            if (result.getNumResults() < tmpLimit) {
                // Not an approximate count!
                result.setApproximateCount(false);
                result.setNumMatches(result.getNumResults() + skip);
            } else if (asyncCount) {
                result.setApproximateCount(false);
                try {
                    result.setNumMatches(asyncCountFuture.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw VariantQueryException.internalException(e);
                }
            } else {
                // Approximate count
                QueryOptions numTotalResultsOptions = new QueryOptions(options);
                // Recover COUNT value from inputOptions
                numTotalResultsOptions.put(QueryOptions.COUNT,
                        inputOptions.get(QueryOptions.COUNT));
                numTotalResultsOptions.put(VariantStorageOptions.APPROXIMATE_COUNT.key(),
                        inputOptions.get(VariantStorageOptions.APPROXIMATE_COUNT.key()));
                setNumTotalResults(variantDBIterator, variants, result, sampleIndexQuery, query, numTotalResultsOptions);
            }

            // Ensure limit
            if (result.getNumResults() > limit) {
                result.setResults(result.getResults().subList(0, limit));
                result.setNumResults(limit);
            }

            result.setSource(getStorageEngineId() + " + " + SAMPLE_INDEX_TABLE_SOURCE);

            try {
                variants.close();
            } catch (Exception e) {
                throw VariantQueryException.internalException(e);
            }
            return result;
        }
    }

    protected void setNumTotalResults(
            MultiVariantDBIterator variantDBIterator, VariantDBIteratorWithCounts variants, VariantQueryResult<Variant> result,
            SampleIndexQuery sampleIndexQuery, Query query, QueryOptions options) {
        query = new Query(query);
        query.put(REGION.key(), sampleIndexQuery.getAllRegions());
        query.put(ID.key(), sampleIndexQuery.getAllVariants());
        setNumTotalResults(variants, result, query, options, variantDBIterator.getNumVariantsFromPrimary(), result.getNumResults());
    }

    private boolean queryFiltersCovered(Query query) {
        // Check if the query is fully covered
        Set<VariantQueryParam> params = VariantQueryUtils.validParams(query, true);
        params.remove(VariantQueryParam.STUDY);

        return params.isEmpty();
    }


}
