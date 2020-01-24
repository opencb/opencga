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

package org.opencb.opencga.storage.server.rest;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

//import org.opencb.opencga.storage.core.variant.adaptors.CatalogVariantDBAdaptor;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>.
 */
@Path("/variants")
public class VariantRestWebService extends GenericRestWebService {

    public static final int LIMIT_DEFAULT = 1000;
    public static final int LIMIT_MAX = 5000;

    public VariantRestWebService(@PathParam("version") String version, @Context UriInfo uriInfo,
                                 @Context HttpServletRequest httpServletRequest, @Context ServletContext context) throws IOException {
        super(version, uriInfo, httpServletRequest, context);
    }

    @GET
    @Path("/query")
    @Produces("application/json")
    public Response fetch(@QueryParam("storageEngine") String storageEngine,
                          @QueryParam("dbName") String dbName,
                          @QueryParam("region") String regionsCVS
    ) {
        try {
            Query query = getVariantQuery(params);
            VariantQueryResult<Variant> queryResult = StorageEngineFactory.get().getVariantStorageEngine(storageEngine,
                    dbName).get(query, queryOptions);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            e.printStackTrace();
            return createErrorResponse(e.toString());
        }
    }

    private Query getVariantQuery(MultivaluedMap<String, ?> params) {
        Query query = new Query();

        for (VariantQueryParam queryParams : VariantQueryParam.values()) {
            if (params.containsKey(queryParams.key())) {
                query.put(queryParams.key(), params.get(queryParams.key()));
            }
        }
        return query;
    }

    public static class VariantFetcher {

        public static DataResult getVariants(String storageEngine, String dbName, QueryOptions options)
                throws StorageEngineException {
            VariantDBAdaptor dbAdaptor = StorageEngineFactory.get().getVariantStorageEngine(storageEngine, dbName).getDBAdaptor();

            Query query = new Query();
            for (VariantQueryParam acceptedValue : VariantQueryParam.values()) {
                if (options.get(acceptedValue.key()) != null) {
                    query.put(acceptedValue.key(), options.get(acceptedValue.key()));
                }
            }
            options.add("query", query);
//            for (String acceptedValue : Arrays.asList("merge", "exclude", "include", "skip", "limit")) {
//                addQueryParam(queryOptions, acceptedValue);
//            }

            if (options.getInt("limit", Integer.MAX_VALUE) > LIMIT_MAX) {
                options.put("limit", Math.max(options.getInt("limit", LIMIT_DEFAULT), LIMIT_MAX));
            }

            return dbAdaptor.get(query, options);
        }

    }
//
//    private void addQueryParam(ObjectMap map, String acceptedValue) {
//        if (params.containsKey(acceptedValue)) {
//            List<String> values = params.get(acceptedValue);
//            String csv = values.get(0);
//            for (int i = 1; i < values.size(); i++) {
//                csv += "," + values.get(i);
//            }
//            map.put(acceptedValue, csv);
//        }
//    }

}
