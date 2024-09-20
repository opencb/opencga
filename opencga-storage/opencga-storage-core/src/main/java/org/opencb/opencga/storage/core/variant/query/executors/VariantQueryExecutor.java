package org.opencb.opencga.storage.core.variant.query.executors;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.query.VariantQuerySource;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.QUERY_DEFAULT_TIMEOUT;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.QUERY_MAX_TIMEOUT;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantQueryExecutor {

    protected final VariantStorageMetadataManager metadataManager;
    protected final String storageEngineId;
    private final ObjectMap options;

    public VariantQueryExecutor(VariantStorageMetadataManager metadataManager, String storageEngineId, ObjectMap options) {
        this.metadataManager = metadataManager;
        this.storageEngineId = storageEngineId;
        this.options = options;
    }

    public final VariantQueryResult<Variant> get(ParsedVariantQuery query) {
        try {
            return (VariantQueryResult<Variant>) getOrIterator(query, false);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    public final VariantDBIterator iterator(ParsedVariantQuery variantQuery) {
        try {
            return (VariantDBIterator) getOrIterator(variantQuery, true);
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
     * @param variantQuery    Query to execute
     * @return         True if this variant query executor is valid for the query
     * @throws StorageEngineException if there is an error
     */
    public final boolean canUseThisExecutor(ParsedVariantQuery variantQuery) throws StorageEngineException {
        boolean canUseThisExecutor = canUseThisExecutor(variantQuery, variantQuery.getInputOptions());
        if (canUseThisExecutor) {
            if (variantQuery.getSource().isSecondary()) {
                // Querying for a secondary index source. This executor can only be used if the source is the same
                if (getSource() != variantQuery.getSource()) {
                    canUseThisExecutor = false;
                }
            }
        }
        return canUseThisExecutor;
    }

    /**
     * Internal method to determine if this VariantQueryExecutor can run the given query.
     * @param variantQuery    Query to execute
     * @param options  Options for the query
     * @return         True if this variant query executor is valid for the query
     * @throws StorageEngineException if there is an error
     */
    protected abstract boolean canUseThisExecutor(ParsedVariantQuery variantQuery, QueryOptions options) throws StorageEngineException;

    protected abstract Object getOrIterator(ParsedVariantQuery variantQuery, boolean iterator) throws StorageEngineException;

    protected VariantStorageMetadataManager getMetadataManager() {
        return metadataManager;
    }

    protected String getStorageEngineId() {
        return storageEngineId;
    }

    protected VariantQuerySource getSource() {
        return VariantQuerySource.VARIANT_INDEX;
    }

    protected ObjectMap getOptions() {
        return options;
    }
}
