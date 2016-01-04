/*
 * Copyright 2015 OpenCB
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

import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
    @Path("/fetch")
    @Produces("application/json")
    public Response fetch(@QueryParam("storageEngine") String storageEngine,
                          @QueryParam("dbName") String dbName,
                          @QueryParam("region") String regionsCVS,
                          @QueryParam("histogram") @DefaultValue("false") boolean histogram,
                          @QueryParam("histogram_interval") @DefaultValue("2000") int interval
    ) {
        try {
            QueryResult queryResult = VariantFetcher.getVariants(storageEngine, dbName, histogram, interval, queryOptions);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            e.printStackTrace();
            return createErrorResponse(e.toString());
        }
    }

    public static class VariantFetcher {

        public static QueryResult getVariants(String storageEngine, String dbName, boolean histogram, int interval, QueryOptions options)
                throws StorageManagerException, ClassNotFoundException, IllegalAccessException, InstantiationException {
            VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager(storageEngine).getDBAdaptor(dbName);

            Query query = new Query();
            for (VariantQueryParams acceptedValue : VariantQueryParams.values()) {
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

            // Parse the provided regions. The total size of all regions together
            // can't excede 1 million positions
            List<Region> regions = Region.parseRegions(options.getString(VariantQueryParams.REGION.key()));
            regions = regions == null ? Collections.emptyList() : regions;
            int regionsSize = regions.stream().reduce(0, (size, r) -> size += r.getEnd() - r.getStart(), (a, b) -> a + b);

            QueryResult queryResult;
            if (histogram) {
                if (regions.size() != 1) {
                    throw new IllegalArgumentException("Sorry, histogram functionality only works with a single region");
                } else {
                    if (interval > 0) {
                        options.put("interval", interval);
                    }
                    queryResult = dbAdaptor.getFrequency(query, regions.get(0), interval);
                }
            } else {
                queryResult = dbAdaptor.get(query, options);
            }
            //            else if (regionsSize <= 1000000) {
            //                if (regions.size() == 0) {
            //                    if (!queryOptions.containsKey("id") && !queryOptions.containsKey("gene")) {
            //                        return createErrorResponse("Some positional filer is needed, like region, gene or id.");
            //                    } else {
            //                        return createOkResponse(variants.get(query, queryOptions));
            //                    }
            //                } else {
            //                    return createOkResponse(variants.get(query, queryOptions));
            //                }
            //            } else {
            //                return createErrorResponse("The total size of all regions provided can't exceed 1 million positions. "
            //                        + "If you want to browse a larger number of positions, please provide the parameter
            // 'histogram=true'");
            //            }

            return queryResult;
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
