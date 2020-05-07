package org.opencb.opencga.storage.mongodb.variant.query;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.DBAdaptorVariantQueryExecutor;

import java.util.Set;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.MODIFIER_QUERY_PARAMS;

/**
 * Special executor to be used if the query contains only filters for REGION and (optionally) STUDY. [#837]
 *
 * Created on 02/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class RegionVariantQueryExecutor extends DBAdaptorVariantQueryExecutor {

    public RegionVariantQueryExecutor(VariantDBAdaptor dbAdaptor, String storageEngineId, ObjectMap options) {
        super(dbAdaptor, storageEngineId, options);
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        if (VariantStorageEngine.UseSearchIndex.from(options).equals(VariantStorageEngine.UseSearchIndex.YES)) {
            // Query search index is mandatory. Can not use this executor.
            return false;
        }
        if (options.getBoolean(QueryOptions.COUNT, false)) {
            // Should not require total count
            return false;
        }

        // Get set of valid params. Remove Modifier params
        Set<VariantQueryParam> queryParams = VariantQueryUtils.validParams(query);
        queryParams.removeAll(MODIFIER_QUERY_PARAMS);

        // REGION + [ STUDY ]
        if (queryParams.contains(REGION) // Has region
                // Optionally, has study
                && (queryParams.size() == 1 || queryParams.size() == 2 && queryParams.contains(VariantQueryParam.STUDY))) {
            return true;
        }

        return false;
    }
}
