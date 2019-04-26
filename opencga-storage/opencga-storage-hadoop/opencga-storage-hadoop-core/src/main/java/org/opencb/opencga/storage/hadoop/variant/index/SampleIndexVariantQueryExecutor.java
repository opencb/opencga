package org.opencb.opencga.storage.hadoop.variant.index;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIteratorWithCounts;
import org.opencb.opencga.storage.core.variant.query.AbstractTwoPhasedVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.addSamplesMetadataIfRequested;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexVariantQueryExecutor extends AbstractTwoPhasedVariantQueryExecutor {

    public static final String SAMPLE_INDEX_INTERSECT = "sample_index_intersect";
    public static final String SAMPLE_INDEX_TABLE_SOURCE = "sample_index_table";
    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final VariantHadoopDBAdaptor dbAdaptor;
    private Logger logger = LoggerFactory.getLogger(SampleIndexVariantQueryExecutor.class);

    public SampleIndexVariantQueryExecutor(VariantHadoopDBAdaptor dbAdaptor, SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                           String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options, "Sample Index Table");
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.dbAdaptor = dbAdaptor;
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
    protected long primaryCount(Query query, QueryOptions options) {
        SampleIndexQuery sampleIndexQuery = SampleIndexQueryParser.parseSampleIndexQuery(query, getMetadataManager());
        return sampleIndexDBAdaptor.count(sampleIndexQuery);
    }

    /**
     * Intersect result of SampleIndexTable and full phoenix query.
     * Use {@link org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator}.
     *
     * @param inputQuery Query
     * @param options    Options
     * @param iterator   Shall the resulting object be an iterator instead of a QueryResult
     * @return           QueryResult or Iterator with the variants that matches the query
     */
    @Override
    protected Object getOrIterator(Query inputQuery, QueryOptions options, boolean iterator) {
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
            setNumTotalResults(variantDBIterator, variants, result, sampleIndexQuery, query, options);
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
        query.put(REGION.key(), sampleIndexQuery.getRegions());
        setNumTotalResults(variants, result, query, options, variantDBIterator.getNumQueries());
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
