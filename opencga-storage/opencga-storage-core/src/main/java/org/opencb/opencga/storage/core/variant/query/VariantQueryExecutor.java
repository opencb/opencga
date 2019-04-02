package org.opencb.opencga.storage.core.variant.query;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.DEFAULT_TIMEOUT;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.MAX_TIMEOUT;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.addDefaultLimit;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.addDefaultSampleLimit;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantQueryExecutor implements VariantIterable {

    protected final VariantDBAdaptor dbAdaptor;
    protected final String storageEngineId;
    protected final ObjectMap options;

    public VariantQueryExecutor(VariantDBAdaptor dbAdaptor, String storageEngineId, ObjectMap options) {
        this.dbAdaptor = dbAdaptor;
        this.storageEngineId = storageEngineId;
        this.options = options;
    }

    public final VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        try {
            addDefaultLimit(options, getOptions());
            addDefaultSampleLimit(query, getOptions());

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
        int defaultTimeout = config.getInt(DEFAULT_TIMEOUT.key(), DEFAULT_TIMEOUT.defaultValue());
        int maxTimeout = config.getInt(MAX_TIMEOUT.key(), MAX_TIMEOUT.defaultValue());
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

    public abstract QueryResult<Long> count(Query query);

    public VariantQueryResult<Long> approximateCount(Query query, QueryOptions options) throws StorageEngineException {
        return new VariantQueryResult<>(count(query), null);
    }

    protected abstract Object getOrIterator(Query query, QueryOptions options, boolean iterator) throws StorageEngineException;

    protected VariantDBAdaptor getDBAdaptor() {
        return dbAdaptor;
    }

    protected VariantStorageMetadataManager getMetadataManager() {
        return dbAdaptor.getMetadataManager();
    }

    protected String getStorageEngineId() {
        return storageEngineId;
    }

    protected ObjectMap getOptions() {
        return options;
    }
}
