package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.Iterators;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.UnionMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIteratorWithCounts;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.CompoundHeterozygousQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.BiFunction;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

/**
 * Created by jacobo on 26/04/19.
 */
public class SampleIndexCompoundHeterozygousQueryExecutor extends CompoundHeterozygousQueryExecutor {

    private static Logger logger = LoggerFactory.getLogger(SampleIndexCompoundHeterozygousQueryExecutor.class);
    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final VariantHadoopDBAdaptor dbAdaptor;

    public SampleIndexCompoundHeterozygousQueryExecutor(
            VariantStorageMetadataManager metadataManager, String storageEngineId, ObjectMap options, VariantIterable iterable,
            SampleIndexDBAdaptor sampleIndexDBAdaptor, VariantHadoopDBAdaptor dbAdaptor) {
        super(metadataManager, storageEngineId, options, iterable);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    protected long primaryCount(Query query, QueryOptions options) {
        // Assume that filter from secondary index is good enough for the approximate count.
        Trio trio = getCompHetTrio(query);
        return Iterators.size(getRawIterator(trio.getChild(), trio.getFather(), trio.getMother(), query, new QueryOptions()
                .append(QueryOptions.INCLUDE, VariantField.ID.fieldName()), sampleIndexDBAdaptor));
    }

    @Override
    protected void setNumTotalResults(VariantDBIteratorWithCounts variantsFromPrimary, VariantQueryResult<Variant> result,
                                      Query query, QueryOptions inputOptions, int numVariantsFromPrimary, int numResults) {
        // Obtain underlying sampleIndex iterator, which is faster than the variants iterator to calculate an approximate count
        VariantDBIteratorWithCounts sampleIndexIterator;
        VariantDBIterator delegated = variantsFromPrimary.getDelegated();
        if (delegated instanceof ExposedMultiVariantDBIterator) {
            VariantDBIterator fastIterator1 = ((ExposedMultiVariantDBIterator) delegated).getIterator();
            sampleIndexIterator = (VariantDBIteratorWithCounts) fastIterator1;
            numVariantsFromPrimary = ((ExposedMultiVariantDBIterator) delegated).getNumVariantsFromPrimary();
        } else {
            sampleIndexIterator = variantsFromPrimary;
        }
        super.setNumTotalResults(sampleIndexIterator, result, query, inputOptions, numVariantsFromPrimary, numResults);
    }

    private static class ExposedMultiVariantDBIterator extends MultiVariantDBIterator {

        ExposedMultiVariantDBIterator(
                VariantDBIterator variantsIterator, int batchSize, Query query, QueryOptions options,
                BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory) {
            super(variantsIterator, batchSize, query, options, iteratorFactory);
        }

        public VariantDBIterator getIterator() {
            return ((VariantDBIterator) variantsIterator);
        }
    }

    @Override
    protected VariantDBIterator getRawIterator(String proband, String father, String mother,
                                               Query query, QueryOptions options, VariantIterable iterable) {
        if (father.equals(MISSING_SAMPLE) || mother.equals(MISSING_SAMPLE)) {
            // Single parent iterator
            String parent = father.equals(MISSING_SAMPLE) ? mother : father;

            query = new Query(query)
                    .append(VariantQueryParam.GENOTYPE.key(), proband + IS + HET + AND + parent + IS + REF + OR + HET)
                    .append(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS.key(), null); // Remove CH filter

            // Single sampleIndex iterator
            VariantDBIterator indexIterator = sampleIndexDBAdaptor.iterator(
                    query, new QueryOptions(options).append(QueryOptions.SORT, true));

            // Join the sampleIndex iterator with the variants index in an ExposedMultiVariantIterator
            return exposedMultiVariantIterator(query, options, indexIterator);
        } else {
            Query baseQuery = new Query(query)
                    .append(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS.key(), null) // Remove CH filter
                    .append(VariantQueryParam.GENOTYPE.key(), null);

            // Multi parent iterator
            Query query1 = new Query(baseQuery)
                    .append(VariantQueryParam.GENOTYPE.key(), proband + IS + HET + AND + father + IS + HET + AND + mother + IS + REF);
            Query query2 = new Query(baseQuery)
                    .append(VariantQueryParam.GENOTYPE.key(), proband + IS + HET + AND + father + IS + REF + AND + mother + IS + HET);

            VariantDBIterator iterator1 = sampleIndexDBAdaptor.iterator(query1, new QueryOptions(options).append(QueryOptions.SORT, true));
            VariantDBIterator iterator2 = sampleIndexDBAdaptor.iterator(query2, new QueryOptions(options).append(QueryOptions.SORT, true));

            Query variantsQuery;
            if (!query1.equals(query2)) {
                logger.warn("Unexpected : Different set of params from query1 and query2");
                logger.info("proband: {}", proband);
                logger.info("Query1: {}", query1.toJson());
                logger.info("Query2: {}", query2.toJson());
                variantsQuery = new Query(baseQuery)
                        .append(VariantQueryParam.GENOTYPE.key(), proband + IS + HET + AND + father + IS + HET + AND + mother + IS + HET);
                // Dummy query to remove covered filters
                sampleIndexDBAdaptor.parseSampleIndexQuery(variantsQuery);
            } else {
                variantsQuery = new Query(query1);
            }

            // Union of two iterators from the SampleIndex
            UnionMultiVariantKeyIterator multiParentIterator = new UnionMultiVariantKeyIterator(Arrays.asList(iterator1, iterator2));
            // Join the sampleIndex iterator with the variants index in an ExposedMultiVariantIterator
            return exposedMultiVariantIterator(variantsQuery, options, multiParentIterator);
        }
    }

    private ExposedMultiVariantDBIterator exposedMultiVariantIterator(Query query, QueryOptions options, VariantDBIterator iterator) {
        int samplingSize = options.getInt(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), DEFAULT_SAMPLING_SIZE);
        return new ExposedMultiVariantDBIterator(
                new VariantDBIteratorWithCounts(iterator),
                ((int) (samplingSize * 1.4)),
                query, new QueryOptions(options).append(QueryOptions.SORT, true), dbAdaptor::iterator
        );
    }
}
