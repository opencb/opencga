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
@Path("/{apiVersion}/analysis/alignment")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Alignment", position = 4, description = "Methods for working with 'files' endpoint")
public class AlignmentAnalysisWSService extends AnalysisWSService {

    public AlignmentAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public AlignmentAnalysisWSService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);
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
            List<String> idList = getIdList(fileIdStr);
            QueryResult queryResult = catalogManager.getFileManager().index(idList, studyStr, "BAM", params, sessionId);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Fetch alignments from a BAM file", position = 15, response = ReadAlignment[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Return total number of results", defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response getAlignments(@ApiParam(value = "File ID or name in Catalog", required = true) @QueryParam("file") String fileIdStr,
                                  @ApiParam(value = "Study [[user@]project:]study where study and project can be either the Id or alias") @QueryParam("study") String studyStr,
                                  @ApiParam(value = "Comma-separated list of regions 'chr:start-end'", required = true) @QueryParam ("region") String regions,
                                  @ApiParam(value = "Minimum mapping quality") @QueryParam("minMapQ") Integer minMapQ,
                                  @ApiParam(value = "Maximum number of mismatches") @QueryParam("maxNM") Integer maxNM,
                                  @ApiParam(value = "Maximum number of hits") @QueryParam("maxNH") Integer maxNH,
                                  @ApiParam(value = "Return only properly paired alignments") @QueryParam("properlyPaired") @DefaultValue("false") Boolean properlyPaired,
                                  @ApiParam(value = "Skip unmapped alignments") @QueryParam("skipUnmapped") @DefaultValue("false") Boolean unmapped,
                                  @ApiParam(value = "Skip duplicated alignments") @QueryParam("skipDuplicated") @DefaultValue("false") Boolean duplicated,
                                  @ApiParam(value = "Return alignments contained within boundaries of region") @DefaultValue("false") @QueryParam("contained") Boolean contained,
                                  @ApiParam(value = "Force SAM MD optional field to be set with the alignments") @DefaultValue("false") @QueryParam("mdField") Boolean mdField,
                                  @ApiParam(value = "Compress the nucleotide qualities by using 8 quality levels") @QueryParam("binQualities") @DefaultValue("false") Boolean binQualities) {
        try {
            Query query = new Query();
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_NM.key(), maxNM);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_NH.key(), maxNH);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.PROPERLY_PAIRED.key(), properlyPaired);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.SKIP_UNMAPPED.key(), unmapped);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.SKIP_DUPLICATED.key(), duplicated);

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), contained);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.MD_FIELD.key(), mdField);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.BIN_QUALITIES.key(), binQualities);
            queryOptions.putIfNotNull(QueryOptions.LIMIT, limit);
            queryOptions.putIfNotNull(QueryOptions.SKIP, skip);
            queryOptions.putIfNotNull(QueryOptions.COUNT, count);

            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
            if (StringUtils.isNotEmpty(regions)) {
                String[] regionList = regions.split(",");
                List<QueryResult<ReadAlignment>> queryResultList = new ArrayList<>(regionList.length);
                for (String region: regionList) {
                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), region);
                    QueryResult<ReadAlignment> queryResult = alignmentStorageManager.query(studyStr, fileIdStr, query, queryOptions, sessionId);
                    queryResultList.add(queryResult);
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
    @Path("/coverage")
    @ApiOperation(value = "Fetch the coverage of an alignment file", position = 15, response = RegionCoverage.class)
    public Response getCoverage(@ApiParam(value = "File ID or name in Catalog", required = true) @QueryParam("file") String fileIdStr,
                                @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
                                @ApiParam(value = "Comma-separated list of regions 'chr:start-end'", required = true) @QueryParam("region") String regions,
                                @ApiParam(value = "Window size", defaultValue = "1") @QueryParam("windowSize") Integer windowSize) {
        try {
            Query query = new Query();
//            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);

            QueryOptions queryOptions = new QueryOptions();
//            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), contained);
            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.WINDOW_SIZE.key(), windowSize);

            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
            if (StringUtils.isNotEmpty(regions)) {
                String[] regionList = regions.split(",");
                List<QueryResult<RegionCoverage>> queryResultList = new ArrayList<>(regionList.length);
                for (String region : regionList) {
                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), region);
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

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetch the stats of an alignment file", position = 15, response = AlignmentGlobalStats.class)
    public Response getStats(@ApiParam(value = "Id of the alignment file in catalog", required = true) @QueryParam("file")
                                     String fileIdStr,
                             @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyId,
                             @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                             @QueryParam("study") String studyStr,
                             @ApiParam(value = "Comma separated list of regions 'chr:start-end'") @QueryParam("region") String region,
                             @ApiParam(value = "Minimum mapping quality") @QueryParam("minMapQ") Integer minMapQ,
                             @ApiParam(value = "Only alignments completely contained within boundaries of region")
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

}
