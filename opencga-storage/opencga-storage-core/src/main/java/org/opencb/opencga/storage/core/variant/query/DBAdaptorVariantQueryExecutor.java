package org.opencb.opencga.storage.core.variant.query;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

/**
 * Simplest implementation of the VariantQueryExecutor.
 * Will run the query using directly the {@link VariantDBAdaptor}.
 *
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DBAdaptorVariantQueryExecutor extends VariantQueryExecutor {

    public DBAdaptorVariantQueryExecutor(VariantDBAdaptor dbAdaptor, String storageEngineId, ObjectMap options) {
        super(dbAdaptor, storageEngineId, options);
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) throws StorageEngineException {
        if (iterator) {
            return dbAdaptor.iterator(query, options);
        } else {
            return dbAdaptor.get(query, options);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) {
        return dbAdaptor.count(query);
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        // Always can use this executor
        return true;
    }
}
