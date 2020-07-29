package org.opencb.opencga.storage.hadoop.variant.adaptors;

import com.google.common.collect.Iterators;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.query.ParsedQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.ALTERNATE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.REFERENCE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser.isSupportedQueryParam;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseColumnIntersectVariantQueryExecutor extends VariantQueryExecutor {
    public static final String HBASE_COLUMN_INTERSECT = "hbase_column_intersect";
    private static final boolean ACTIVE_BY_DEFAULT = false;
    private final VariantDBAdaptor dbAdaptor;

    private Logger logger = LoggerFactory.getLogger(HBaseColumnIntersectVariantQueryExecutor.class);

    public HBaseColumnIntersectVariantQueryExecutor(VariantDBAdaptor dbAdaptor, String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {

        if (!options.getBoolean(HBASE_COLUMN_INTERSECT, ACTIVE_BY_DEFAULT)) {
            // HBase column intersect not active
            return false;
        }

        if (options.getBoolean(QueryOptions.COUNT, false) || options.getBoolean(VariantStorageOptions.APPROXIMATE_COUNT.key(), false)) {
            // Do not use for count
            return false;
        }

        if (isValidParam(query, SAMPLE) && isSupportedQueryParam(query, SAMPLE)) {
            if (VariantQueryUtils.checkOperator(query.getString(SAMPLE.key())) != QueryOperation.OR) {
                // Valid sample filter
                return true;
            }
        }
        if (isValidParam(query, GENOTYPE) && isSupportedQueryParam(query, GENOTYPE)) {
            QueryOperation queryOperation = parseGenotypeFilter(query.getString(GENOTYPE.key()), new HashMap<>());
            if (queryOperation != QueryOperation.OR) {
                // Valid genotype filter
                return true;
            }
        }
        if (isValidParam(query, FILE) && isSupportedQueryParam(query, FILE)) {
            // Valid file filter
            return true;
        }

        return false;
    }

    @Override
    public DataResult<Long> count(Query query) {
        throw new UnsupportedOperationException("Count not implemented in " + getClass());
    }

    /**
     * Intersect result of column hbase scan and full phoenix query.
     * Use {@link org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator}.
     *
     * @param query    Query
     * @param options  Options
     * @param iterator Shall the resulting object be an iterator instead of a DataResult
     * @return DataResult or Iterator with the variants that matches the query
     */
    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) {
        logger.info("HBase column intersect");

        // Build the query with only one query filter -> Single HBase column filter
        //
        // We want to take profit of getting all the values from one column pretty fast
        // If we add more columns, even to reduce the number of results, we will scan more rows,
        // which is what ultimately we want to reduce.
        // Only add more columns if we want rows with ANY of them. e.g: files=file1;file2
        // TODO: Make number of filters configurable?
        Query scanQuery = new Query();
        QueryOptions scanOptions = new QueryOptions(VariantHadoopDBAdaptor.NATIVE, true)
                .append(QueryOptions.INCLUDE, Arrays.asList(CHROMOSOME, START, END, REFERENCE, ALTERNATE));

        scanQuery.putIfNotNull(STUDY.key(), query.get(STUDY.key()));
        if (query.getBoolean(VARIANTS_TO_INDEX.key(), false)) {
            scanQuery.put(VARIANTS_TO_INDEX.key(), true);
        } else if (isValidParam(query, SAMPLE)) {
            // At any case, filter only by first sample
            // TODO: Use sample with less variants?

            scanQuery.putIfNotNull(SAMPLE.key(), splitValue(query, SAMPLE).getValues().get(0));
        } else if (isValidParam(query, GENOTYPE)) {
            // Get the genotype sample with fewer genotype filters (i.e., the most strict filter)

            HashMap<Object, List<String>> map = new HashMap<>();
            parseGenotypeFilter(query.getString(GENOTYPE.key()), map);
            Map.Entry<Object, List<String>> currentEntry = null;
            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
                if (currentEntry == null || currentEntry.getValue().size() > entry.getValue().size()) {
                    currentEntry = entry;
                }
            }

            scanQuery.putIfNotNull(GENOTYPE.key(),
                    currentEntry.getKey() + ":" + currentEntry.getValue().stream().collect(Collectors.joining(",")));
        } else if (isValidParam(query, FILE)) {
            ParsedQuery<String> pair = splitValue(query, FILE);
            if (pair.getOperation() == QueryOperation.OR) {
                // Because we want all the variants with ANY of this files, use ALL files to filter
                scanQuery.putIfNotNull(FILE.key(), query.get(FILE.key()));
            } else {
                // Filter only by one file
                scanQuery.putIfNotNull(FILE.key(), pair.getValues().get(0));
            }

        }
        if (isValidParam(query, REGION)) {
            scanQuery.put(REGION.key(), query.get(REGION.key()));
            query.remove(REGION.key());
        }

        Iterator<String> variants = Iterators.transform(dbAdaptor.iterator(scanQuery, scanOptions), Variant::toString);

        int batchSize = options.getInt("multiIteratorBatchSize", 100);
        if (iterator) {
            return dbAdaptor.iterator(variants, query, options, batchSize);
        } else {
            VariantQueryResult<Variant> result = dbAdaptor.get(variants, query, options);
            result.setSource(getStorageEngineId() + " + " + getStorageEngineId());
            return result;
        }
    }


}
