/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.server.rest.ga4gh;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.ga4gh.methods.SearchReadsRequest;
import org.ga4gh.methods.SearchReadsResponse;
import org.ga4gh.methods.SearchVariantsRequest;
import org.ga4gh.methods.SearchVariantsResponse;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Variant;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * Created on 09/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */

@Path("/{version}/ga4gh")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "GA4GH", position = 13, description = "Global Alliance for Genomics & Health RESTful API")
public class Ga4ghWSServer extends OpenCGAWSServer {

    public Ga4ghWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    /* =================    VARIANTS     ===================*/

    @POST
    @Path("/variants/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Description", position = 1, notes = "Notes")
    public Response searchVariants(SearchVariantsRequest request) {
        String method = "ga4gh/variants/search";
        try {

            if (request.getVariantSetId() == null || request.getVariantSetId().isEmpty()) {
                return createErrorResponse(method, "Required referenceName or referenceId");
            }
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.append(STUDIES.key(), request.getVariantSetId());

//        queryOptions.append(, request.getVariantName()); //TODO
            if (request.getCallSetIds() != null) {
                queryOptions.append(RETURNED_SAMPLES.key(), request.getCallSetIds());
            }

            CharSequence chr = null;
            if (request.getReferenceName() != null) {
                chr = request.getReferenceName();
            }
            if (chr == null) {
                return createErrorResponse(method, "Required referenceName or referenceId");
            }
            if (request.getStart() == null || request.getStart() < 0) {
                return createErrorResponse(method, "Required start position");
            }
            if (request.getEnd() == null || request.getEnd() < 0) {
                return createErrorResponse(method, "Required end position");
            }
            long delta = request.getEnd() - request.getStart();
            if (delta < 0/* || delta > 20000*/) {
                return createErrorResponse(method, "End must be behind the start");
            }
            queryOptions.append(REGION.key(), new Region(chr.toString(), request.getStart().intValue(), request.getEnd().intValue()));

            if (request.getPageSize() == null || request.getPageSize() <= 0 || request.getPageSize() > 4000) {
                this.queryOptions.add(QueryOptions.LIMIT, 1000);
            } else {
                this.queryOptions.add(QueryOptions.LIMIT, request.getPageSize());
            }

            int page = 0;
            if (request.getPageToken() != null) {
                try {
                    page = Integer.parseInt(request.getPageToken().toString());
                    this.queryOptions.put("skip", this.queryOptions.getInt("limit") * page);
                } catch (Exception e) {
                    return createErrorResponse(method, "Invalid page token \"" + request.getPageToken() + "\"");
                }
            }
            // Get all query options
            SearchVariantsResponse response = new SearchVariantsResponse();
            Query query = VariantStorageManager.getVariantQuery(queryOptions);

            List<Variant> variants = variantManager.get(query, queryOptions, sessionId, Variant.class).getResult();
            response.setNextPageToken(Integer.toString(++page));
            response.setVariants(variants);
            return buildResponse(Response.ok(response.toString(), MediaType.APPLICATION_JSON_TYPE));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    /* =================    ALIGNMENTS     ===================*/

    @POST
    @Path("/reads/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Description", position = 1, notes = "Notes")
    public Response searchAlignments(SearchReadsRequest request) {
        String method = "ga4gh/reads/search";
        try {
            if (request.getReadGroupIds() == null || request.getReadGroupIds().size() == 0) {
                return createErrorResponse(method, "Required at least one group id.");
            }

            if (request.getReadGroupIds().size() > 1) {
                return createErrorResponse(method, "Several read group ids yet not supported.");
            }

            if (request.getReferenceId() == null || request.getReferenceId().isEmpty()) {
                return createErrorResponse(method, "Required reference id");
            }

            if (request.getStart() == null || request.getStart() <= 0)  {
                return createErrorResponse(method, "Required start position");
            }

            if (request.getEnd() == null || request.getEnd() <= 0)  {
                return createErrorResponse(method, "Required end position");
            }

            Query query = new Query();
            query.put(AlignmentDBAdaptor.QueryParams.REGION.key(),
                    request.getReferenceId() + ":" + request.getStart().intValue() + "-" + request.getEnd().intValue());

            this.queryOptions.put(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), true);

            if (request.getPageSize() == null || request.getPageSize() <= 0 || request.getPageSize() > 4000) {
                this.queryOptions.put(AlignmentDBAdaptor.QueryParams.LIMIT.key(), 1000);
            } else {
                this.queryOptions.put(AlignmentDBAdaptor.QueryParams.LIMIT.key(), request.getPageSize());
            }

            int page = 0;
            if (request.getPageToken() != null) {
                try {
                    page = Integer.parseInt(request.getPageToken().toString());
                    this.queryOptions.put(AlignmentDBAdaptor.QueryParams.SKIP.key(),
                            this.queryOptions.getInt(AlignmentDBAdaptor.QueryParams.LIMIT.key()) * page);
                } catch (Exception e) {
                    return createErrorResponse(method, "Invalid page token \"" + request.getPageToken() + "\"");
                }
            }

            SearchReadsResponse response = new SearchReadsResponse();

            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            QueryResult<ReadAlignment> queryResult = alignmentStorageManager
                    .query("", request.getReadGroupIds().get(0), query, queryOptions, sessionId);

            response.setAlignments(queryResult.getResult());
            response.setNextPageToken(Integer.toString(++page));

            return buildResponse(Response.ok(response.toString(), MediaType.APPLICATION_JSON_TYPE));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
