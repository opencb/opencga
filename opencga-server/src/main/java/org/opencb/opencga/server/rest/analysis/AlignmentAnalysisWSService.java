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

package org.opencb.opencga.server.rest.analysis;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{version}/analysis/alignment")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Alignment", position = 4, description = "Methods for working with 'files' endpoint")
public class AlignmentAnalysisWSService extends AnalysisWSService {

    public AlignmentAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public AlignmentAnalysisWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/index")
    @ApiOperation(value = "Index alignment files", position = 14, response = QueryResponse.class)
    public Response index(@ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                              @QueryParam(value = "file") String fileIdStr,
                          @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyId,
                          @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                          @ApiParam("Boolean indicating that only the transform step will be run") @DefaultValue("false") @QueryParam("transform") boolean transform,
                          @ApiParam("Boolean indicating that only the load step will be run") @DefaultValue("false") @QueryParam("load") boolean load) {
        if (StringUtils.isNotEmpty(studyId)) {
            studyStr = studyId;
        }

        Map<String, String> params = new LinkedHashMap<>();
//        addParamIfNotNull(params, "studyId", studyId);
        addParamIfTrue(params, "transform", transform);
        addParamIfTrue(params, "load", load);

        logger.info("ObjectMap: {}", params);

        try {
            QueryResult queryResult = catalogManager.getFileManager().index(fileIdStr, studyStr, "BAM", params, sessionId);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Fetch alignments from a BAM file", position = 15, response = ReadAlignment[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAlignments(@ApiParam(value = "Id of the alignment file in catalog", required = true) @QueryParam("file")
                                          String fileIdStr,
                                  @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyId,
                                  @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                      @QueryParam("study") String studyStr,
                                  @ApiParam(value = "Comma separated list of regions 'chr:start-end'", required = false) @QueryParam
                                          ("region") String region,
                                  @ApiParam(value = "Minimum mapping quality", required = false) @QueryParam("minMapQ") Integer minMapQ,
                                  @ApiParam(value = "Only alignments completely contained within boundaries of region", required = false)
                                  @QueryParam("contained") Boolean contained,
                                  @ApiParam(value = "Force SAM MD optional field to be set with the alignments", required = false)
                                  @QueryParam("mdField") Boolean mdField,
                                  @ApiParam(value = "Compress the nucleotide qualities by using 8 quality levels", required = false)
                                  @QueryParam("binQualities") Boolean binQualities) {
        try {
            if (StringUtils.isNotEmpty(studyId)) {
                studyStr = studyId;
            }

            Query query = new Query();
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.LIMIT.key(), limit);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.SKIP.key(), skip);
            queryOptions.putIfNotNull("count", count);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), contained);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.MD_FIELD.key(), mdField);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.BIN_QUALITIES.key(), binQualities);

            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            if (StringUtils.isNotEmpty(region)) {
                String[] regionList = region.split(",");
                List<QueryResult<ReadAlignment>> queryResultList = new ArrayList<>(regionList.length);
                for (String regionAux : regionList) {
                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), regionAux);
                    queryResultList.add(alignmentStorageManager.query(studyStr, fileIdStr, query, queryOptions, sessionId));
                }
                return createOkResponse(queryResultList);
            } else {
                return createOkResponse(alignmentStorageManager.query(studyStr, fileIdStr, query, queryOptions, sessionId));
            }

        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetch the stats of an alignment file", position = 15, response = AlignmentGlobalStats.class)
    public Response getStats(@ApiParam(value = "Id of the alignment file in catalog", required = true) @QueryParam("file")
                                          String fileIdStr,
                             @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyId,
                             @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                             @ApiParam(value = "Comma separated list of regions 'chr:start-end'", required = false) @QueryParam("region") String region,
                             @ApiParam(value = "Minimum mapping quality", required = false) @QueryParam("minMapQ") Integer minMapQ,
                             @ApiParam(value = "Only alignments completely contained within boundaries of region", required = false)
                                 @QueryParam("contained") Boolean contained) {
        try {
            if (StringUtils.isNotEmpty(studyId)) {
                studyStr = studyId;
            }

            Query query = new Query();
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), contained);

            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            if (StringUtils.isNotEmpty(region)) {
                String[] regionList = region.split(",");
                List<QueryResult<AlignmentGlobalStats>> queryResultList = new ArrayList<>(regionList.length);
                for (String regionAux : regionList) {
                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), regionAux);
                    queryResultList.add(alignmentStorageManager.stats(studyStr, fileIdStr, query, queryOptions, sessionId));
                }
                return createOkResponse(queryResultList);
            } else {
                return createOkResponse(alignmentStorageManager.stats(studyStr, fileIdStr, query, queryOptions, sessionId));
            }
//            String userId = catalogManager.getUserManager().getId(sessionId);
//            Long fileId = catalogManager.getFileManager().getId(userId, fileIdStr);
//
//            Query query = new Query();
//            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), region);
//            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);
//
//            QueryOptions queryOptions = new QueryOptions();
//            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), contained);
//
//            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
//            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, options, sessionId);
//
//            if (fileQueryResult != null && fileQueryResult.getNumResults() != 1) {
//                // This should never happen
//                throw new CatalogException("Critical error: File " + fileId + " could not be found in catalog.");
//            }
//            String path = fileQueryResult.first().getUri().getRawPath();
//
//            AlignmentStorageEngine alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager();
//            AlignmentGlobalStats stats = alignmentStorageManager.getDBAdaptor().stats(path, query, queryOptions);
//            QueryResult<AlignmentGlobalStats> queryResult = new QueryResult<>("get stats", -1, 1, 1, "", "", Arrays.asList(stats));
//            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/coverage")
    @ApiOperation(value = "Fetch the coverage of an alignment file", position = 15, response = RegionCoverage.class)
    public Response getCoverage(@ApiParam(value = "Id of the alignment file in catalog", required = true) @QueryParam("file")
                                     String fileIdStr,
                                @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyId,
                                @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                                @ApiParam(value = "Comma separated list of regions 'chr:start-end'", required = false) @QueryParam("region")
                                            String region,
                                @ApiParam(value = "Minimum mapping quality", required = false) @QueryParam("minMapQ") Integer minMapQ,
                                @ApiParam(value = "Window size", required = false, defaultValue = "1") @QueryParam("windowSize")
                                    Integer windowSize,
                                @ApiParam(value = "Only alignments completely contained within boundaries of region", required = false)
                                    @QueryParam("contained") Boolean contained) {
        try {
            if (StringUtils.isNotEmpty(studyId)) {
                studyStr = studyId;
            }

            Query query = new Query();
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), contained);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.WINDOW_SIZE.key(), windowSize);

            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            if (StringUtils.isNotEmpty(region)) {
                String[] regionList = region.split(",");
                List<QueryResult<RegionCoverage>> queryResultList = new ArrayList<>(regionList.length);
                for (String regionAux : regionList) {
                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), regionAux);
                    queryResultList.add(alignmentStorageManager.coverage(studyStr, fileIdStr, query, queryOptions, sessionId));
                }
                return createOkResponse(queryResultList);
            } else {
                return createOkResponse(alignmentStorageManager.coverage(studyStr, fileIdStr, query, queryOptions, sessionId));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
