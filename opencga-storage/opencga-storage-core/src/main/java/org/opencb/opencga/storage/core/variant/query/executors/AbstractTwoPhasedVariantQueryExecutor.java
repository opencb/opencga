package org.opencb.opencga.storage.core.variant.query.executors;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIteratorWithCounts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.APPROXIMATE_COUNT;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.isValidParam;

/**
 * Execute a variant query in two phases.
 *
 * Query first a primary source (e.g. an index) and then apply a transformation
 * (e.g. post-filter, or fetch from the variants storage) to obtain the final result.
 *
 * Created by jacobo on 25/04/19.
 */
public abstract class AbstractTwoPhasedVariantQueryExecutor extends VariantQueryExecutor {

    public static final float MAGIC_NUMBER = 12.5F; // Magic number! Proportion of variants from chr1 and the whole genome
    public static final int CHR1_LENGTH = 249250621;
    public static final int CHR1_COUNT_THRESHOLD = 9500; // Less than 2 * MultiVariantDBIterator.VariantQueryIterator.MAX_BATCH_SIZE
    private final String primarySource;

    private Logger logger = LoggerFactory.getLogger(AbstractTwoPhasedVariantQueryExecutor.class);

    public AbstractTwoPhasedVariantQueryExecutor(VariantStorageMetadataManager metadataManager, String storageEngineId,
                                                 ObjectMap options, String primarySource) {
        super(metadataManager, storageEngineId, options);
        this.primarySource = primarySource;
    }

    /**
     * Count number of results from the primary.
     * @param query   Query
     * @param options Options
     * @return        Number of variants in the primary source.
     */
    protected abstract long primaryCount(Query query, QueryOptions options);

    protected final void setNumTotalResults(VariantDBIteratorWithCounts variantsFromPrimary, VariantQueryResult<Variant> result,
                                            Query query, QueryOptions options) {
        setNumTotalResults(variantsFromPrimary, result, query, options,
                variantsFromPrimary.getCount(), result.getNumResults());
    }

    /**
     * Set the approximate number of total results.
     *
     * @param variantsFromPrimary    Variants from primary source. Usually, a fast index source.
     * @param result                 VariantQueryResult to modify
     * @param query                  Query being executed
     * @param options                Options of the query
     * @param numVariantsFromPrimary Number of variants read from the primary source
     * @param numResults             Final number of results
     */
    protected void setNumTotalResults(VariantDBIteratorWithCounts variantsFromPrimary, VariantQueryResult<Variant> result,
                                            Query query, QueryOptions options,
                                            int numVariantsFromPrimary, int numResults) {
        // TODO: Allow exact count with "approximateCount=false"
        if (shouldGetApproximateCount(options)) {
            int limit = options.getInt(QueryOptions.LIMIT, -1);
            int skip = options.getInt(QueryOptions.SKIP, 0);
            if (limit >= 0 && limit > numResults) {
                if (skip > 0 && numResults == 0) {
                    // Skip could be greater than numTotalResults. Approximate count
                    result.setApproximateCount(true);
                } else {
                    // Less results than limit. Count is not approximated
                    result.setApproximateCount(false);
                }
                result.setNumMatches(numResults + skip);
                return;
            }

            long totalCount;
            if (variantsFromPrimary.hasNext()) {
                if (!isValidParam(query, REGION)) {
                    totalCount = estimateTotalCount(variantsFromPrimary, query);
//                } else if (sampleIndexDBAdaptor.isFastCount(sampleIndexQuery) && sampleIndexQuery.getSamplesMap().size() == 1) {
//                    StopWatch stopWatch = StopWatch.createStarted();
//                    Map.Entry<String, List<String>> entry = sampleIndexQuery.getSamplesMap().entrySet().iterator().next();
//                    totalCount = sampleIndexDBAdaptor.count(sampleIndexQuery, entry.getKey());
//                    logger.info("Count variants from sample index table : " + TimeUtils.durationToString(stopWatch));
                } else {
                    StopWatch stopWatch = StopWatch.createStarted();
                    Iterators.getLast(variantsFromPrimary);
                    totalCount = variantsFromPrimary.getCount();
                    logger.info("Drain variants from " + primarySource + " : " + TimeUtils.durationToString(stopWatch));
                }
            } else {
                logger.info("Variants from " + primarySource + " exhausted");
                totalCount = variantsFromPrimary.getCount();
            }
            long approxCount;
            logger.info("numResults = " + numResults);
            logger.info("numResultsFromPrimary = " + numVariantsFromPrimary);
            logger.info("totalCountFromPrimary = " + totalCount);

            // Multiply first to avoid loss of precision
            approxCount = totalCount * numResults / numVariantsFromPrimary;

            logger.info("approxCount = " + approxCount);
            result.setApproximateCount(true);
            result.setNumTotalResults(approxCount);
            result.setApproximateCountSamplingSize(numVariantsFromPrimary);
        }
    }

    protected boolean shouldGetApproximateCount(QueryOptions options) {
        return shouldGetApproximateCount(options, false);
    }

    protected boolean shouldGetApproximateCount(QueryOptions options, boolean iterator) {
        return !iterator && (options.getBoolean(QueryOptions.COUNT, false) || options.getBoolean(APPROXIMATE_COUNT.key(), false));
    }

    protected final boolean shouldGetExactCount(QueryOptions options) {
        return options.getBoolean(QueryOptions.COUNT, false) && !options.getBoolean(VariantStorageOptions.APPROXIMATE_COUNT.key(), false);
    }

    protected int getLimit(QueryOptions options) {
        return options.getInt(QueryOptions.LIMIT);
    }

    protected int getSkip(QueryOptions options) {
        return Math.max(0, options.getInt(QueryOptions.SKIP));
    }

    protected int getSamplingSize(QueryOptions inputOptions, int defaultSamplingSize, boolean iterator) {
        int samplingSize;
        if (shouldGetApproximateCount(inputOptions, iterator)) {
            samplingSize = inputOptions.getInt(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), defaultSamplingSize);
        } else {
            samplingSize = 0;
        }
        return samplingSize;
    }

    private long estimateTotalCount(VariantDBIteratorWithCounts variantsFromPrimary, Query query) {
        long totalCount;
        long chr1Count;
        StopWatch stopWatch = StopWatch.createStarted();
        if (variantsFromPrimary.getChromosomeCount("1") != null) {
            // Iterate until the chr1 is exhausted
            int i = 0;
            chr1Count = variantsFromPrimary.getChromosomeCount("1");
            boolean partial = false;
            Variant next = null;
            while ("1".equals(variantsFromPrimary.getCurrentChromosome()) && variantsFromPrimary.hasNext()) {
                next = variantsFromPrimary.next();
                i++;
                chr1Count++;
                if (chr1Count > CHR1_COUNT_THRESHOLD) {
                    if (next.getChromosome().equals("1")) {
                        partial = true;
                    }
                    break;
                }
            }
            if (partial) {
                chr1Count = ((long) (((float) chr1Count) / next.getStart() * CHR1_LENGTH));
            }
            if (i != 0) {
                if (partial) {
                    logger.info("Partial count variants from chr1, up to " + CHR1_COUNT_THRESHOLD + ", using the same iterator over the "
                            + primarySource + " : Read " + i + " extra variants in " + TimeUtils.durationToString(stopWatch));
                } else {
                    logger.info("Count variants from chr1 using the same iterator over the " + primarySource + " : "
                            + "Read " + i + " extra variants in " + TimeUtils.durationToString(stopWatch));
                }
            }
        } else {
            query.put(REGION.key(), "1");
            chr1Count = primaryCount(query, new QueryOptions());
            logger.info("Count variants from chr1 in " + primarySource + " : " + TimeUtils.durationToString(stopWatch));
        }

        logger.info("chr1 count = " + chr1Count);
        totalCount = (long) (chr1Count * MAGIC_NUMBER);
        return totalCount;
    }
}
