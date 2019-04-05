package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.Iterators;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIteratorWithCounts;
import org.opencb.opencga.storage.core.variant.query.VariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.APPROXIMATE_COUNT;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.addSamplesMetadataIfRequested;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexVariantQueryExecutor extends VariantQueryExecutor {

    public static final String SAMPLE_INDEX_INTERSECT = "sample_index_intersect";
    public static final String SAMPLE_INDEX_TABLE_SOURCE = "sample_index_table";
    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private Logger logger = LoggerFactory.getLogger(SampleIndexVariantQueryExecutor.class);

    public SampleIndexVariantQueryExecutor(VariantHadoopDBAdaptor dbAdaptor, SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                           String storageEngineId, ObjectMap options) {
        super(dbAdaptor, storageEngineId, options);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        if (options.getBoolean(SAMPLE_INDEX_INTERSECT, true)) {
            if (options.getBoolean(QueryOptions.COUNT, false)) {
                // TODO: Support count
                return false;
            } else {
                return SampleIndexQueryParser.validSampleIndexQuery(query);
            }
        }
        return false;
    }

    @Override
    public QueryResult<Long> count(Query query) {
        throw new UnsupportedOperationException("Count not implemented in " + getClass());
    }

    @Override
    protected VariantHadoopDBAdaptor getDBAdaptor() {
        return (VariantHadoopDBAdaptor) super.getDBAdaptor();
    }

    /**
     * Intersect result of SampleIndexTable and full phoenix query.
     * Use {@link org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator}.
     *
     * @param inputQuery Query
     * @param options    Options
     * @param iterator   Shall the resulting object be an iterator instead of a QueryResult
     * @return           QueryResult or Iterator with the variants that matches the query
     * @throws StorageEngineException StorageEngineException
     */
    @Override
    protected Object getOrIterator(Query inputQuery, QueryOptions options, boolean iterator)
            throws StorageEngineException {
        Query query = new Query(inputQuery);
        SampleIndexQuery sampleIndexQuery = SampleIndexQueryParser.parseSampleIndexQuery(query, getMetadataManager());

        if (isFullyCoveredQuery(query, options)) {
            logger.info("HBase SampleIndex, skip variants table");
            return getOrIteratorFullyCovered(options, iterator, query, sampleIndexQuery);
        } else {
            logger.info("HBase SampleIndex intersect");
            return getOrIteratorIntersect(sampleIndexQuery, query, options, iterator);
        }

    }

    private Object getOrIteratorFullyCovered(QueryOptions options, boolean iterator, Query query, SampleIndexQuery sampleIndexQuery) {
        VariantDBIterator variantIterator = sampleIndexDBAdaptor.iterator(sampleIndexQuery);
        if (iterator) {
            return variantIterator;
        } else {
            variantIterator.toQueryResult();
            VariantQueryResult<Variant> result =
                    addSamplesMetadataIfRequested(variantIterator.toQueryResult(), query, options, getMetadataManager());
//                if (!options.getBoolean(QueryOptions.SKIP_COUNT, true) || options.getBoolean(APPROXIMATE_COUNT.key(), false)) {
//
//                }
            result.setSource(SAMPLE_INDEX_TABLE_SOURCE);
            return result;
        }
    }

    private Object getOrIteratorIntersect(SampleIndexQuery sampleIndexQuery, Query query, QueryOptions options, boolean iterator) {
        VariantDBIteratorWithCounts variants = new VariantDBIteratorWithCounts(sampleIndexDBAdaptor.iterator(sampleIndexQuery));

        int batchSize = options.getInt("multiIteratorBatchSize", 200);
        if (iterator) {
            // SampleIndex iterator will be closed when closing the variants iterator
            return dbAdaptor.iterator(variants, query, options, batchSize);
        } else {
            MultiVariantDBIterator variantDBIterator = dbAdaptor.iterator(
                    new org.opencb.opencga.storage.core.variant.adaptors.iterators.DelegatedVariantDBIterator(variants) {
                        @Override
                        public void close() throws Exception {
                            // Do not close this iterator! We'll need to keep iterating to get the approximate count
                        }
                    }, query, options, batchSize);
            VariantQueryResult<Variant> result =
                    addSamplesMetadataIfRequested(variantDBIterator.toQueryResult(), query, options, getMetadataManager());
            // TODO: Allow exact count with "approximateCount=false"
            if (!options.getBoolean(QueryOptions.SKIP_COUNT, true) || options.getBoolean(APPROXIMATE_COUNT.key(), false)) {
                int sampling = variants.getCount();
                int limit = options.getInt(QueryOptions.LIMIT, 0);
                int skip = options.getInt(QueryOptions.SKIP, 0);
                if (limit > 0 && limit > result.getNumResults()) {
                    if (skip > 0 && result.getNumResults() == 0) {
                        // Skip could be greater than numTotalResults. Approximate count
                        result.setApproximateCount(true);
                    } else {
                        // Less results than limit. Count is not approximated
                        result.setApproximateCount(false);
                    }
                    result.setNumTotalResults(result.getNumResults() + skip);
                } else if (variants.hasNext()) {
                    long totalCount;
                    if (CollectionUtils.isEmpty(sampleIndexQuery.getRegions())) {
                        int chr1Count;
                        StopWatch stopWatch = StopWatch.createStarted();
                        if (variants.getChromosomeCount("1") != null) {
                            // Iterate until the chr1 is exhausted
                            int i = 0;
                            while ("1".equals(variants.getCurrentChromosome()) && variants.hasNext()) {
                                variants.next();
                                i++;
                            }
                            chr1Count = variants.getChromosomeCount("1");
                            if (i != 0) {
                                logger.info("Count variants from chr1 using the same iterator over the Sample Index Table : "
                                        + "Read " + i + " extra variants in " + TimeUtils.durationToString(stopWatch));
                            }
                        } else {
                            query.put(REGION.key(), "1");
                            SampleIndexQuery sampleIndexQueryChr1 =
                                    SampleIndexQueryParser.parseSampleIndexQuery(query, getMetadataManager());
                            chr1Count = Iterators.size(sampleIndexDBAdaptor.iterator(sampleIndexQueryChr1));
                            logger.info("Count variants from chr1 in Sample Index Table : " + TimeUtils.durationToString(stopWatch));
                        }
                        float magicNumber = 12.5F; // Magic number! Proportion of variants from chr1 and the whole genome

                        logger.info("chr1 count = " + chr1Count);
                        totalCount = (int) (chr1Count * magicNumber);
                    } else if (sampleIndexDBAdaptor.isFastCount(sampleIndexQuery) && sampleIndexQuery.getSamplesMap().size() == 1) {
                        StopWatch stopWatch = StopWatch.createStarted();
                        Map.Entry<String, List<String>> entry = sampleIndexQuery.getSamplesMap().entrySet().iterator().next();
                        totalCount = sampleIndexDBAdaptor.count(sampleIndexQuery, entry.getKey());
                        logger.info("Count variants from sample index table : " + TimeUtils.durationToString(stopWatch));
                    } else {
                        StopWatch stopWatch = StopWatch.createStarted();
                        Iterators.getLast(variants);
                        totalCount = variants.getCount();
                        logger.info("Drain variants from sample index table : " + TimeUtils.durationToString(stopWatch));
                    }
                    long approxCount;
                    logger.info("totalCount = " + totalCount);
                    logger.info("result.getNumResults() = " + result.getNumResults());
                    logger.info("numQueries = " + variantDBIterator.getNumQueries());
                    if (variantDBIterator.getNumQueries() == 1) {
                        // Just one query with limit, index was accurate enough
                        approxCount = totalCount;
                    } else {
                        // Multiply first to avoid loss of precision
                        approxCount = totalCount * result.getNumResults() / sampling;
                        logger.info("sampling = " + sampling);
                    }
                    logger.info("approxCount = " + approxCount);
                    result.setApproximateCount(true);
                    result.setNumTotalResults(approxCount);
                    result.setApproximateCountSamplingSize(sampling);
                } else {
                    logger.info("Genotype index Iterator exhausted");
                    logger.info("sampling = " + sampling);
                    result.setApproximateCount(sampling != result.getNumResults());
                    result.setNumTotalResults(sampling);
                }
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

    private boolean isFullyCoveredQuery(Query query, QueryOptions options) {

        // Check if the included files are fully covered
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);
        if (includeFields.contains(VariantField.ANNOTATION) || includeFields.contains(VariantField.STUDIES)) {
            return false;
        }

        // Check if the query is fully covered
        Set<VariantQueryParam> params = VariantQueryUtils.validParams(query, true);
        params.remove(VariantQueryParam.STUDY);

        return params.isEmpty();
    }


}
