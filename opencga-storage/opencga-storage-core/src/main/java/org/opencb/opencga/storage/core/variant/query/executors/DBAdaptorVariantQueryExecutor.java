package org.opencb.opencga.storage.core.variant.query.executors;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.core.response.VariantQueryResult;
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

    private final VariantDBAdaptor dbAdaptor;

    public DBAdaptorVariantQueryExecutor(VariantDBAdaptor dbAdaptor, String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) {
        if (iterator) {
            return dbAdaptor.iterator(query, options);
        } else {
            VariantQueryResult<Variant> result = dbAdaptor.get(query, options);
            if (result.getSource() == null || result.getSource().isEmpty()) {
                result.setSource(storageEngineId);
            }
            return result;
        }
    }

    @Override
    public DataResult<Long> count(Query query) {
        return dbAdaptor.count(query);
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        // Always can use this executor
        return true;
    }
}
