package org.opencb.opencga.client.rest.analysis;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;

import java.io.IOException;

/**
 * Created by pfurio on 23/11/16.
 */
public class VariantClient extends AbstractParentClient {

    private static final String VARIANT_URL = "analysis/variant";

    public VariantClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    public QueryResponse<Job> index(String fileIds, ObjectMap params) throws CatalogException, IOException {
        params.append("file", fileIds);
        return execute(VARIANT_URL, "index", params, GET, Job.class);
    }

    public QueryResponse<Variant> query(ObjectMap bodyParams, QueryOptions options) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(options);
        params.putIfNotNull("body", bodyParams);
        return execute(VARIANT_URL, "query", params, POST, Variant.class);
    }

    public QueryResponse<Long> count(ObjectMap bodyParams, QueryOptions options) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(options);
        params.putIfNotNull("body", bodyParams);
        return execute(VARIANT_URL, "query", params, POST, Long.class);
    }

    public QueryResponse<ObjectMap> genericQuery(ObjectMap bodyParams, QueryOptions options) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(options);
        params.putIfNotNull("body", bodyParams);
        return execute(VARIANT_URL, "query", params, POST, ObjectMap.class);
    }

}
