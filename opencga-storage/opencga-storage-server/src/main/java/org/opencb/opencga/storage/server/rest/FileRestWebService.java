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
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Created by jacobo on 14/11/14.
 */
@Path("/files")
public class FileRestWebService extends GenericRestWebService {

    public FileRestWebService(@PathParam("version") String version, @Context UriInfo uriInfo,
                              @Context HttpServletRequest httpServletRequest, @Context ServletContext context) throws IOException {
        super(version, uriInfo, httpServletRequest, context);
        params = uriInfo.getQueryParameters();
    }

    @GET
    @Path("/{fileId}/info")
    @Produces("application/json")
    public Response info(@PathParam(value = "fileId") @DefaultValue("") String fileId) {
        return createErrorResponse("Unimplemented!");
    }

    @GET
    @Path("/{fileId}/index")
    @Produces("application/json")
    public Response index(@PathParam(value = "fileId") @DefaultValue("") String fileId) {
        return createErrorResponse("Unimplemented!");
    }

    @GET
    @Path("/{fileId}/fetch")
    @Produces("application/json")
    public Response fetch(@PathParam("fileId") @DefaultValue("") String fileId,
                          @QueryParam("storageEngine") String storageEngine,
                          @QueryParam("dbName") String dbName,
                          @QueryParam("bioformat") @DefaultValue("") String bioformat,
                          @QueryParam("region") String region,

                          @QueryParam("path") @DefaultValue("") String path,
                          @QueryParam("view_as_pairs") @DefaultValue("false") boolean viewAsPairs,
                          @QueryParam("include_coverage") @DefaultValue("true") boolean includeCoverage,
                          @QueryParam("process_differences") @DefaultValue("true") boolean processDifferences,
                          @QueryParam("histogram") @DefaultValue("false") boolean histogram,
                          @QueryParam("interval") @DefaultValue("2000") int interval) {

        try {
            switch (bioformat) {
                case "vcf":
                    queryOptions.add(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileId);
                    return createOkResponse(VariantRestWebService.VariantFetcher.getVariants(storageEngine, dbName, histogram, interval,
                            queryOptions));
                case "bam":
                    AlignmentStorageManager sm = StorageManagerFactory.get().getAlignmentStorageManager(storageEngine);
                    ObjectMap params = new ObjectMap();
                    AlignmentDBAdaptor dbAdaptor = sm.getDBAdaptor(dbName);

                    QueryOptions options = new QueryOptions();
                    if (path != null && !path.isEmpty()) {
//                        String rootDir = OpenCGAStorageService.getInstance().getProperties().getProperty("OPENCGA.STORAGE.ROOTDIR",
//                                "/home/cafetero/opencga/catalog/users/jcoll/projects/1/1/");
                        String rootDir = "";
                        options.put(AlignmentDBAdaptor.QO_BAM_PATH, Paths.get(rootDir, path.replace(":", "/")).toString());
                    }
                    options.put(AlignmentDBAdaptor.QO_FILE_ID, fileId);
                    options.put(AlignmentDBAdaptor.QO_VIEW_AS_PAIRS, viewAsPairs);
                    options.put(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, includeCoverage);
                    options.put(AlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, processDifferences);
                    options.put(AlignmentDBAdaptor.QO_INTERVAL_SIZE, interval);
                    options.put(AlignmentDBAdaptor.QO_HISTOGRAM, histogram);
//                    options.put(AlignmentDBAdaptor.QO_COVERAGE_CHUNK_SIZE, chunkSize);


                    QueryResult queryResult;
                    if (histogram) {
                        queryResult = dbAdaptor.getAllIntervalFrequencies(new Region(region), options);
                    } else {
                        queryResult = dbAdaptor.getAllAlignmentsByRegion(Arrays.asList(new Region(region)), options);
                    }

                    return createOkResponse(queryResult);
                default:
                    return createErrorResponse("Unknown bioformat " + bioformat);

            }
        } catch (Exception e) {
            e.printStackTrace();
            return createErrorResponse(e.toString());
        }
    }
}
