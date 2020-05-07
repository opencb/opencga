package org.opencb.opencga.storage.core.variant.query.executors;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.Collections;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.QUERY_DEFAULT_TIMEOUT;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.QUERY_MAX_TIMEOUT;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantQueryExecutor implements VariantIterable {

    protected final VariantStorageMetadataManager metadataManager;
    protected final String storageEngineId;
    private final ObjectMap options;

    public VariantQueryExecutor(VariantStorageMetadataManager metadataManager, String storageEngineId, ObjectMap options) {
        this.metadataManager = metadataManager;
        this.storageEngineId = storageEngineId;
        this.options = options;
    }

    public final VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        try {
            return (VariantQueryResult<Variant>) getOrIterator(query, options, false);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public final VariantDBIterator iterator(Query query, QueryOptions options) {
        try {
//            query = parser.preProcessQuery(query, options);
            return (VariantDBIterator) getOrIterator(query, options, true);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    public void setDefaultTimeout(QueryOptions options) {
        setDefaultTimeout(options, getOptions());
    }

    public static void setDefaultTimeout(QueryOptions queryOptions, ObjectMap config) {
        int defaultTimeout = config.getInt(QUERY_DEFAULT_TIMEOUT.key(), QUERY_DEFAULT_TIMEOUT.defaultValue());
        int maxTimeout = config.getInt(QUERY_MAX_TIMEOUT.key(), QUERY_MAX_TIMEOUT.defaultValue());
        int timeout = queryOptions.getInt(QueryOptions.TIMEOUT, defaultTimeout);
        if (timeout > maxTimeout) {
            throw new VariantQueryException("Invalid timeout '" + timeout + "'. Max timeout is " + maxTimeout);
        } else if (timeout < 0) {
            throw new VariantQueryException("Invalid timeout '" + timeout + "'. Timeout must be positive");
        }
        queryOptions.put(QueryOptions.TIMEOUT, timeout);
    }

    /**
     * Determine if this VariantQueryExecutor can run the given query.
     * @param query    Query to execute
     * @param options  Options for the query
     * @return         True if this variant query executor is valid for the query
     * @throws StorageEngineException if there is an error
     */
    public abstract boolean canUseThisExecutor(Query query, QueryOptions options) throws StorageEngineException;

    public DataResult<Long> count(Query query) {
        VariantQueryResult<Variant> result = get(query, new QueryOptions(QueryOptions.COUNT, true).append(QueryOptions.LIMIT, 0));
        return new DataResult<>(
                result.getTime(),
                result.getEvents(),
                1,
                Collections.singletonList(result.getNumMatches()),
                result.getNumMatches(),
                result.getAttributes());
    }

    public VariantQueryResult<Long> approximateCount(Query query, QueryOptions options) {
        return new VariantQueryResult<>(count(query), null);
    }

    protected abstract Object getOrIterator(Query query, QueryOptions options, boolean iterator) throws StorageEngineException;

    protected VariantStorageMetadataManager getMetadataManager() {
        return metadataManager;
    }

    protected String getStorageEngineId() {
        return storageEngineId;
    }

    protected ObjectMap getOptions() {
        return options;
    }
}
