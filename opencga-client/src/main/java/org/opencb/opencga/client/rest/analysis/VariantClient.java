/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.client.rest.analysis;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.results.VariantQueryResult;

import java.io.IOException;
import java.util.List;

/**
 * Created by pfurio on 23/11/16.
 */
public class VariantClient extends AbstractParentClient {

    private static final String VARIANT_URL = "analysis/variant";

    public static class QueryResponseMixing<T> {
        @JsonDeserialize(contentAs = VariantQueryResult.class)
        private List<QueryResult<T>> response;
    }

    public VariantClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
        jsonObjectMapper.addMixIn(QueryResponse.class, QueryResponseMixing.class);
    }

    public QueryResponse<Job> index(String fileIds, ObjectMap params) throws CatalogException, IOException {
        params.append("file", fileIds);
        return execute(VARIANT_URL, "index", params, GET, Job.class);
    }

    public QueryResponse<Variant> query(ObjectMap params, QueryOptions options) throws CatalogException, IOException {
        if (options != null) {
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, Variant.class);
    }

    public VariantQueryResult<Variant> query2(ObjectMap params, QueryOptions options) throws CatalogException, IOException {
        if (options != null) {
            params.putAll(options);
        }
        return executeVariantQuery(VARIANT_URL, "query", params, GET, Variant.class);
    }

//    public VariantQueryResult<Variant> queryResult(ObjectMap params, QueryOptions options) throws CatalogException, IOException {
//        return ((VariantQueryResult<Variant>) query(params, options).getResponse().get(0));
//    }

    public QueryResponse<Long> count(ObjectMap params, QueryOptions options) throws CatalogException, IOException {
        if (options != null) {
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, Long.class);
    }

    public QueryResponse<ObjectMap> genericQuery(ObjectMap params, QueryOptions options) throws CatalogException, IOException {
        if (options != null) {
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, ObjectMap.class);
    }

}
