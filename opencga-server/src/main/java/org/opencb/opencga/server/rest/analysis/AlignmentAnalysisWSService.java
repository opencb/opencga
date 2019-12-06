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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.exceptions.AlignmentCoverageException;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.wrappers.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.OpenCGAResult;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.core.api.ParamConstants.*;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/analysis/alignment")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Alignment", description = "Methods for working with 'files' endpoint")
public class AlignmentAnalysisWSService extends AnalysisWSService {

    public AlignmentAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public AlignmentAnalysisWSService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/index")
    @ApiOperation(value = "Index alignment files", response = DataResponse.class)
    public Response index(@ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                          @QueryParam(value = "file") String fileIdStr,
                          @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyId,
                          @ApiParam(value = STUDY_DESCRIPTION)
                          @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                          @ApiParam("Output directory id") @QueryParam("outDir") String outDirStr,
                          @ApiParam("Compute coverage") @DefaultValue("false") @QueryParam("coverage") boolean coverage) {

        if (StringUtils.isNotEmpty(studyId)) {
            studyStr = studyId;
        }

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("file", fileIdStr);
        addParamIfTrue(params, "coverage", coverage);
        addParamIfNotNull(params, "outdir", outDirStr);

        logger.info("ObjectMap: {}", params);

        try {
            OpenCGAResult<Job> queryResult = catalogManager.getJobManager().submit(studyStr, "alignment", "index", Enums.Priority.HIGH,
                    params, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Fetch alignments from a BAM file", response = ReadAlignment[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Max number of results to be returned", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = "Number of results to skip", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Return total number of results", defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response query(@ApiParam(value = "File ID or name in Catalog", required = true) @QueryParam("file") String fileIdStr,
                          @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                          @ApiParam(value = "Comma-separated list of regions 'chr:start-end'", required = true) @QueryParam("region") String regions,
                          @ApiParam(value = "Minimum mapping quality") @QueryParam("minMapQ") Integer minMapQ,
                          @ApiParam(value = "Maximum number of mismatches") @QueryParam("maxNM") Integer maxNM,
                          @ApiParam(value = "Maximum number of hits") @QueryParam("maxNH") Integer maxNH,
                          @ApiParam(value = "Return only properly paired alignments") @QueryParam("properlyPaired") @DefaultValue("false") Boolean properlyPaired,
                          @ApiParam(value = "Maximum insert size") @QueryParam("maxInsertSize") Integer maxInsertSize,
                          @ApiParam(value = "Skip unmapped alignments") @QueryParam("skipUnmapped") @DefaultValue("false") Boolean unmapped,
                          @ApiParam(value = "Skip duplicated alignments") @QueryParam("skipDuplicated") @DefaultValue("false") Boolean duplicated,
                          @ApiParam(value = "Return alignments contained within boundaries of region") @DefaultValue("false") @QueryParam("contained") Boolean contained,
                          @ApiParam(value = "Force SAM MD optional field to be set with the alignments") @DefaultValue("false") @QueryParam("mdField") Boolean mdField,
                          @ApiParam(value = "Compress the nucleotide qualities by using 8 quality levels") @QueryParam("binQualities") @DefaultValue("false") Boolean binQualities) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            Query query = new Query();
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_NM.key(), maxNM);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_NH.key(), maxNH);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.PROPERLY_PAIRED.key(), properlyPaired);
            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_INSERT_SIZE.key(), maxInsertSize);
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
                DataResult<ReadAlignment> dataResult = DataResult.empty();
                for (String region : regionList) {
                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), region);
                    DataResult<ReadAlignment> queryResult = alignmentStorageManager.query(studyStr, fileIdStr, query, queryOptions, token);
                    dataResult.append(queryResult);
                }
                return createOkResponse(dataResult);
            } else {
                return createErrorResponse("query", "Missing region, no region provided");
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/coverage/run")
    @ApiOperation(value = "Compute coverage for a list of alignment files", response = Job.class)
    public Response coverageRun(@ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                                @QueryParam(value = "file") String fileIdStr,
                                @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyId,
                                @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                                @ApiParam("Output directory id") @QueryParam("outDir") String outDirStr) {

        if (StringUtils.isNotEmpty(studyId)) {
            studyStr = studyId;
        }

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("file", fileIdStr);
        addParamIfNotNull(params, "outdir", outDirStr);

        logger.info("ObjectMap: {}", params);


        try {
            OpenCGAResult<Job> queryResult = catalogManager.getJobManager().submit(studyStr, "alignment", "coverage-run", Enums.Priority.HIGH, params,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/coverage/query")
    @ApiOperation(value = "Fetch the coverage of an alignment file", response = RegionCoverage.class)
    public Response coverageQuery(
            @ApiParam(value = "File ID or name in Catalog", required = true) @QueryParam("file") String fileIdStr,
            @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of regions 'chr:start-end'") @QueryParam("region") String regionStr,
            @ApiParam(value = "Comma separated list of genes") @QueryParam("gene") String geneStr,
            @ApiParam(value = "Gene offset (to extend the gene region at up and downstream") @DefaultValue("500") @QueryParam("geneOffset") int geneOffset,
            @ApiParam(value = "Only exons") @QueryParam("onlyExons") @DefaultValue("false") Boolean onlyExons,
            @ApiParam(value = "Exon offset (to extend the exon region at up and downstream") @DefaultValue("50") @QueryParam("exonOffset") int exonOffset,
            @ApiParam(value = "Range of coverage values to be reported. Minimum and maximum values are separated by '-', e.g.: 20-40 (for coverage values greater or equal to 20 and less or equal to 40). A single value means to report coverage values greater or equal to that value.") @QueryParam("threshold") String threshold,
            @ApiParam(value = "Window size (if a threshold is provided, window size must be 1)") @DefaultValue("1") @QueryParam("windowSize") int windowSize) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            List<Region> regionList = new ArrayList<>();

            // Parse regions from region parameter
            if (StringUtils.isNotEmpty(regionStr)) {
                regionList.addAll(Region.parseRegions(regionStr));
            }

            // Get regions from genes/exons parameters
            if (StringUtils.isNotEmpty(geneStr)) {
                regionList = getRegionsFromGenes(geneStr, geneOffset, onlyExons, exonOffset, regionList, studyStr);
            }

            if (CollectionUtils.isNotEmpty(regionList)) {
                DataResult<RegionCoverage> dataResult = DataResult.empty();
                if (StringUtils.isEmpty(threshold)) {
                    for (Region region : regionList) {
                        DataResult<RegionCoverage> coverage = alignmentStorageManager.coverage(studyStr, fileIdStr, region, windowSize, token);
                        if (coverage.getResults().size() > 0) {
                            dataResult.append(coverage);
                        }
                    }
                } else {
                    // Report regions for a given coverage range
                    String[] split = threshold.split("-");
                    int minCoverage;
                    int maxCoverage;
                    try {
                        if (split.length == 1) {
                            minCoverage = Integer.parseInt(split[0]);
                            maxCoverage = Integer.MAX_VALUE;
                        } else if (split.length == 2) {
                            minCoverage = Integer.parseInt(split[0]);
                            maxCoverage = Integer.parseInt(split[1]);
                        } else {
                            return createErrorResponse(new AlignmentCoverageException("Invalid threshold: " + threshold
                                    + ". Valid ranges include minimum and maximum values, e.g.: 20-60"));
                        }
                    } catch (NumberFormatException e) {
                        return createErrorResponse(new AlignmentCoverageException("Invalid threshold: " + threshold
                                + ". Valid ranges include minimum and maximum values, e.g.: 20-60"));
                    }
                    if (minCoverage > maxCoverage) {
                        return createErrorResponse(new AlignmentCoverageException("Invalid threshold: " + threshold
                                + ". The maximum value must be greater or equal to the minimum value, e.g.: 20-60"));
                    }

                    if (windowSize != 1) {
                        return createErrorResponse(new AlignmentCoverageException("Invalid window size: " + windowSize
                                + ". Window size must be 1 when retrieving coverage with a given threshold"));
                    }

                    for (Region region : regionList) {
                        DataResult<RegionCoverage> coverage = alignmentStorageManager.coverage(studyStr, fileIdStr, region, minCoverage,
                                maxCoverage, token);
                        if (coverage.getResults().size() > 0) {
                            dataResult.append(coverage);
                        }
                    }
                }

                return createOkResponse(dataResult);
            } else {
                return createErrorResponse("coverage", "Missing region, no region provides");
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/coverage/log2Ratio")
    @ApiOperation(value = "Compute log2 coverage ratio from file #1 and file #2", response = RegionCoverage.class)
    public Response coverageLog2Ratio(@ApiParam(value = "File #1 (e.g., somatic file ID or name in Catalog)", required = true) @QueryParam("file1") String somaticFileIdStr,
                                      @ApiParam(value = "File #2 (e.g., germline file ID or name in Catalog)", required = true) @QueryParam("file2") String germlineFileIdStr,
                                      @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                      @ApiParam(value = "Comma separated list of regions 'chr:start-end'") @QueryParam("region") String regionStr,
                                      @ApiParam(value = "Comma separated list of genes") @QueryParam("gene") String geneStr,
                                      @ApiParam(value = "Gene offset (to extend the gene region at up and downstream") @DefaultValue("500") @QueryParam("geneOffset") int geneOffset,
                                      @ApiParam(value = "Only exons") @QueryParam("onlyExons") @DefaultValue("false") Boolean onlyExons,
                                      @ApiParam(value = "Exon offset (to extend the exon region at up and downstream") @DefaultValue("50") @QueryParam("exonOffset") int exonOffset,
                                      @ApiParam(value = "Window size") @DefaultValue("1") @QueryParam("windowSize") int windowSize) {
        try {
            ParamUtils.checkIsSingleID(somaticFileIdStr);
            ParamUtils.checkIsSingleID(germlineFileIdStr);
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            List<Region> regionList = new ArrayList<>();

            // Parse regions from region parameter
            if (StringUtils.isNotEmpty(regionStr)) {
                regionList.addAll(Region.parseRegions(regionStr));
            }

            // Get regions from genes/exons parameters
            if (StringUtils.isNotEmpty(geneStr)) {
                regionList = getRegionsFromGenes(geneStr, geneOffset, onlyExons, exonOffset, regionList, studyStr);
            }

            if (CollectionUtils.isNotEmpty(regionList)) {
                // Getting total counts for file #1: somatic file
                DataResult<Long> somaticResult = alignmentStorageManager.getTotalCounts(studyStr, somaticFileIdStr, token);
                if (CollectionUtils.isEmpty(somaticResult.getResults()) || somaticResult.getResults().get(0) == 0) {
                    return createErrorResponse("log2CoverageRatio", "Impossible get total counts for file " + somaticFileIdStr);
                }
                long somaticTotalCounts = somaticResult.getResults().get(0);

                // Getting total counts for file #2: germline file
                DataResult<Long> germlineResult = alignmentStorageManager.getTotalCounts(studyStr, germlineFileIdStr, token);
                if (CollectionUtils.isEmpty(germlineResult.getResults()) || germlineResult.getResults().get(0) == 0) {
                    return createErrorResponse("log2CoverageRatio", "Impossible get total counts for file " + germlineFileIdStr);
                }
                long germlineTotalCounts = germlineResult.getResults().get(0);

                // Compute log2 coverage ratio for each region given
                DataResult<RegionCoverage> dataResult = DataResult.empty();
                for (Region region : regionList) {
                    DataResult<RegionCoverage> somaticCoverage = alignmentStorageManager.coverage(studyStr, somaticFileIdStr, region, windowSize, token);
                    DataResult<RegionCoverage> germlineCoverage = alignmentStorageManager.coverage(studyStr, germlineFileIdStr, region, windowSize, token);
                    if (somaticCoverage.getResults().size() == 1 && germlineCoverage.getResults().size() == 1) {
                        try {
                            StopWatch watch = StopWatch.createStarted();
                            RegionCoverage coverage = BamUtils.log2CoverageRatio(somaticCoverage.getResults().get(0), somaticTotalCounts,
                                    germlineCoverage.getResults().get(0), germlineTotalCounts);
                            int dbTime = somaticResult.getTime() + somaticCoverage.getTime()
                                    + germlineResult.getTime() + germlineCoverage.getTime() + ((int) watch.getTime());
                            dataResult.append(new DataResult<>(dbTime, Collections.emptyList(), 1, Collections.singletonList(coverage), 1));
                        } catch (AlignmentCoverageException e) {
                            logger.error("log2CoverageRatio: " + e.getMessage() + ": somatic file = " + somaticFileIdStr + ", germline file = " + germlineFileIdStr + ", region = " + region.toString());
                        }
                    } else {
                        logger.error("log2CoverageRatio: something wrong happened: somatic file = " + somaticFileIdStr + ", germline file = " + germlineFileIdStr + ", region = " + region.toString());
                    }
                }
                return createOkResponse(dataResult);
            } else {
                return createErrorResponse("log2CoverageRatio", "Missing region, no region provides");
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/lowCoverage")
    @ApiOperation(value = "Fetch regions with a low coverage", hidden = true, response = RegionCoverage.class)
    public Response lowCoverage(
            @ApiParam(value = "File id or name in Catalog", required = true) @QueryParam("file") String fileIdStr,
            @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of regions 'chr:start-end'") @QueryParam("region") String regionStr,
            @ApiParam(value = "Comma separated list of genes") @QueryParam("gene") String geneStr,
            @ApiParam(value = "Gene offset (to extend the gene region at up and downstream") @DefaultValue("500") @QueryParam("geneOffset") int geneOffset,
            @ApiParam(value = "Only exons") @QueryParam("onlyExons") @DefaultValue("false") Boolean onlyExons,
            @ApiParam(value = "Exon offset (to extend the exon region at up and downstream") @DefaultValue("50") @QueryParam("exonOffset") int exonOffset,
            @ApiParam(value = "Number of reads under which a region will will be considered low covered") @DefaultValue("20") @QueryParam("minCoverage") int minCoverage) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
            List<Region> regionList = new ArrayList<>();

            // Parse regions from region parameter
            if (StringUtils.isNotEmpty(regionStr)) {
                regionList.addAll(Region.parseRegions(regionStr));
            }

            // Get regions from genes/exons parameters
            if (StringUtils.isNotEmpty(geneStr)) {
                regionList = getRegionsFromGenes(geneStr, geneOffset, onlyExons, exonOffset, regionList, studyStr);
            }

            if (CollectionUtils.isNotEmpty(regionList)) {
                // Compute low coverage regions from the given input regions
                DataResult<RegionCoverage> dataResult = DataResult.empty();
                for (Region region : regionList) {
                    dataResult.append(alignmentStorageManager.getLowCoverageRegions(studyStr, fileIdStr, region, minCoverage, token));
                }
                return createOkResponse(dataResult);
            } else {
                return createErrorResponse("lowCoveredRegions", "Missing regions or genes");
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    //-------------------------------------------------------------------------
    // STATS: run, info and query
    //-------------------------------------------------------------------------

    @POST
    @Path("/stats/run")
    @ApiOperation(value = "Compute stats for a list of alignment files", response = Job.class)
    public Response statsRun(@ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                             @QueryParam(value = "inputFile") String inputFile,
                             @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                             @QueryParam("study") String study) {

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("inputFile", inputFile);

        logger.info("ObjectMap: {}", params);

        try {
            OpenCGAResult<Job> queryResult = catalogManager.getJobManager().submit(study, "alignment", "stats-run",
                    Enums.Priority.HIGH, params, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats/info")
    @ApiOperation(value = "Fetch the stats of an alignment file", response = AlignmentGlobalStats.class)
    public Response statsInfo(@ApiParam(value = "Id of the alignment file in catalog", required = true) @QueryParam("inputFile")
                                      String inputFile,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String study) {

        try {
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
            return createOkResponse(alignmentStorageManager.statsInfo(study, inputFile, token));
        } catch (AnalysisException e) {
            return createErrorResponse(e);
        }

//        try {
//            Query query = new Query();
//            query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), minMapQ);
//
//            QueryOptions queryOptions = new QueryOptions();
//            queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), contained);
//
//            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
//            alignmentStorageManager.statsInfo(study, inputFile, token);
//
//            if (StringUtils.isNotEmpty(region)) {
//                String[] regionList = region.split(",");
//                DataResult<AlignmentGlobalStats> dataResult = DataResult.empty();
//                for (String regionAux : regionList) {
//                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), regionAux);
//                    dataResult.append(alignmentStorageManager.stats(studyStr, fileIdStr, query, queryOptions, token));
//                }
//                return createOkResponse(dataResult);
//            } else {
//                return createOkResponse(alignmentStorageManager.stats(studyStr, fileIdStr, query, queryOptions, token));
//            }
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
    }

    @GET
    @Path("/stats/query")
    @ApiOperation(value = "Fetch alignment files according to their stats", response = File.class)
    public Response statsQuery(@ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
                               @ApiParam(value = RAW_TOTAL_SEQUENCES_DESCRIPTION) @QueryParam(RAW_TOTAL_SEQUENCES) String rawTotalSequences,
                               @ApiParam(value = FILTERED_SEQUENCES_DESCRIPTION) @QueryParam(FILTERED_SEQUENCES) String filteredSequences,
                               @ApiParam(value = READS_MAPPED_DESCRIPTION) @QueryParam(READS_MAPPED) String readsMapped,
                               @ApiParam(value = READS_MAPPED_AND_PAIRED_DESCRIPTION) @QueryParam(READS_MAPPED_AND_PAIRED) String readsMappedAndPaired,
                               @ApiParam(value = READS_UNMAPPED_DESCRIPTION) @QueryParam(READS_UNMAPPED) String readsUnmapped,
                               @ApiParam(value = READS_PROPERLY_PAIRED_DESCRIPTION) @QueryParam(READS_PROPERLY_PAIRED) String readsProperlyPaired,
                               @ApiParam(value = READS_PAIRED_DESCRIPTION) @QueryParam(READS_PAIRED) String readsPaired,
                               @ApiParam(value = READS_DUPLICATED_DESCRIPTION) @QueryParam(READS_DUPLICATED) String readsDuplicated,
                               @ApiParam(value = READS_MQ0_DESCRIPTION) @QueryParam(READS_MQ0) String readsMQ0,
                               @ApiParam(value = READS_QC_FAILED_DESCRIPTION) @QueryParam(READS_QC_FAILED) String readsQCFailed,
                               @ApiParam(value = NON_PRIMARY_ALIGNMENTS_DESCRIPTION) @QueryParam(NON_PRIMARY_ALIGNMENTS) String nonPrimaryAlignments,
                               @ApiParam(value = MISMATCHES_DESCRIPTION) @QueryParam(MISMATCHES) String mismatches,
                               @ApiParam(value = ERROR_RATE_DESCRIPTION) @QueryParam(ERROR_RATE) String errorRate,
                               @ApiParam(value = AVERAGE_LENGTH_DESCRIPTION) @QueryParam(AVERAGE_LENGTH) String averageLength,
                               @ApiParam(value = AVERAGE_FIRST_FRAGMENT_LENGTH_DESCRIPTION) @QueryParam(AVERAGE_FIRST_FRAGMENT_LENGTH) String averageFirstFragmentLength,
                               @ApiParam(value = AVERAGE_LAST_FRAGMENT_LENGTH_DESCRIPTION) @QueryParam(AVERAGE_LAST_FRAGMENT_LENGTH) String averageLastFragmentLength,
                               @ApiParam(value = AVERAGE_QUALITY_DESCRIPTION) @QueryParam(AVERAGE_QUALITY) String averageQuality,
                               @ApiParam(value = INSERT_SIZE_AVERAGE_DESCRIPTION) @QueryParam(INSERT_SIZE_AVERAGE) String insertSizeAverage,
                               @ApiParam(value = INSERT_SIZE_STANDARD_DEVIATION_DESCRIPTION) @QueryParam(INSERT_SIZE_STANDARD_DEVIATION) String insertSizeStandardDeviation,
                               @ApiParam(value = PAIRS_WITH_OTHER_ORIENTATION_DESCRIPTION) @QueryParam(PAIRS_WITH_OTHER_ORIENTATION) String pairsWithOtherOrientation,
                               @ApiParam(value = PAIRS_ON_DIFFERENT_CHROMOSOMES_DESCRIPTION) @QueryParam(PAIRS_ON_DIFFERENT_CHROMOSOMES) String pairsOnDifferentChromosomes,
                               @ApiParam(value = PERCENTAGE_OF_PROPERLY_PAIRED_READS_DESCRIPTION) @QueryParam(PERCENTAGE_OF_PROPERLY_PAIRED_READS) String percentageOfProperlyPairedReads) {

        Query query = new Query();
        query.putIfNotNull(RAW_TOTAL_SEQUENCES, rawTotalSequences);
        query.putIfNotNull(FILTERED_SEQUENCES, filteredSequences);
        query.putIfNotNull(READS_MAPPED, readsMapped);
        query.putIfNotNull(READS_MAPPED_AND_PAIRED, readsMappedAndPaired);
        query.putIfNotNull(READS_UNMAPPED, readsUnmapped);
        query.putIfNotNull(READS_PROPERLY_PAIRED, readsProperlyPaired);
        query.putIfNotNull(READS_PAIRED, readsPaired);
        query.putIfNotNull(READS_DUPLICATED, readsDuplicated);
        query.putIfNotNull(READS_MQ0, readsMQ0);
        query.putIfNotNull(READS_QC_FAILED, readsQCFailed);
        query.putIfNotNull(NON_PRIMARY_ALIGNMENTS, nonPrimaryAlignments);
        query.putIfNotNull(MISMATCHES, mismatches);
        query.putIfNotNull(ERROR_RATE, errorRate);
        query.putIfNotNull(AVERAGE_LENGTH, averageLength);
        query.putIfNotNull(AVERAGE_FIRST_FRAGMENT_LENGTH, averageFirstFragmentLength);
        query.putIfNotNull(AVERAGE_LAST_FRAGMENT_LENGTH, averageLastFragmentLength);
        query.putIfNotNull(AVERAGE_QUALITY, averageQuality);
        query.putIfNotNull(INSERT_SIZE_AVERAGE, insertSizeAverage);
        query.putIfNotNull(INSERT_SIZE_STANDARD_DEVIATION, insertSizeStandardDeviation);
        query.putIfNotNull(PAIRS_WITH_OTHER_ORIENTATION, pairsWithOtherOrientation);
        query.putIfNotNull(PAIRS_ON_DIFFERENT_CHROMOSOMES, pairsOnDifferentChromosomes);
        query.putIfNotNull(PERCENTAGE_OF_PROPERLY_PAIRED_READS, percentageOfProperlyPairedReads);

        try {
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
            return createOkResponse(alignmentStorageManager.statsQuery(study, query, QueryOptions.empty(), token));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S
    //-------------------------------------------------------------------------

    // BWA

    public static class BwaRunParams extends RestBodyParams {
        public BwaRunParams() {
        }

        public BwaRunParams(String command, String fastaFile, String indexBaseFile, String fastq1File, String fastq2File, String samFile,
                            String outdir, Map<String, String> bwaParams) {
            this.command = command;
            this.fastaFile = fastaFile;
            this.indexBaseFile = indexBaseFile;
            this.fastq1File = fastq1File;
            this.fastq2File = fastq2File;
            this.samFile = samFile;
            this.outdir = outdir;
            this.bwaParams = bwaParams;
        }

        public String command;       // Valid values: index or mem
        public String fastaFile;     //  Fasta file
        public String indexBaseFile; // Index base file
        public String fastq1File;    // FastQ #1 file
        public String fastq2File;    // FastQ #2 file
        public String samFile;       // SAM file
        public String outdir;
        public Map<String, String> bwaParams;
    }

    @POST
    @Path("/bwa/run")
    @ApiOperation(value = BwaWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response bwaRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            AlignmentAnalysisWSService.BwaRunParams params) {
        return submitJob("alignment", BwaWrapperAnalysis.ID, study, params, jobName, jobDescription, jobTags);
    }

    // Samtools

    public static class SamtoolsRunParams extends RestBodyParams {
        public SamtoolsRunParams() {
        }

        public SamtoolsRunParams(String command, String inputFile, String outputFile, String outdir, Map<String, String> samtoolsParams) {
            this.command = command;
            this.inputFile = inputFile;
            this.outputFile = outputFile;
            this.outdir = outdir;
            this.samtoolsParams = samtoolsParams;
        }

        public String command;      // Valid values: view, index, sort, stats
        public String inputFile;    // Input file
        public String outputFile;   // Output file
        public String outdir;
        public Map<String, String> samtoolsParams;
    }

    @POST
    @Path("/samtools/run")
    @ApiOperation(value = SamtoolsWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response samtoolsRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            AlignmentAnalysisWSService.SamtoolsRunParams params) {
        return submitJob("alignment", SamtoolsWrapperAnalysis.ID, study, params, jobName, jobDescription, jobTags);
    }

    // Deeptools

    public static class DeeptoolsRunParams extends RestBodyParams {
        public DeeptoolsRunParams() {
        }

        public DeeptoolsRunParams(String executable, String bamFile, String coverageFile, String outdir,
                                  Map<String, String> deeptoolsParams) {
            this.executable = executable;
            this.bamFile = bamFile;
            this.coverageFile = coverageFile;
            this.outdir = outdir;
            this.deeptoolsParams = deeptoolsParams;
        }

        public String executable;     // Valid values: bamCoverage
        public String bamFile;        // BAM file
        public String coverageFile;   // Coverage file
        public String outdir;
        public Map<String, String> deeptoolsParams;
    }

    @POST
    @Path("/deeptools/run")
    @ApiOperation(value = DeeptoolsWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response deeptoolsRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            AlignmentAnalysisWSService.DeeptoolsRunParams params) {
        return submitJob("alignment", DeeptoolsWrapperAnalysis.ID, study, params, jobName, jobDescription, jobTags);
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private List<Region> getRegionsFromGenes(String geneStr, int geneOffset, boolean onlyExons, int exonOffset, List<Region> initialRegions,
                                             String studyStr)
            throws CatalogException, StorageEngineException, IOException {
        Map<String, Region> regionMap = new HashMap<>();

        // Process initial regions
        if (CollectionUtils.isNotEmpty(initialRegions)) {
            for (Region region : initialRegions) {
                updateRegionMap(region, regionMap);
            }
        }

        // Get species and assembly from catalog
        DataResult<Project> projectQueryResult = catalogManager.getProjectManager().get(
                new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyStr),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), token);
        if (projectQueryResult.getNumResults() != 1) {
            throw new CatalogException("Error getting species and assembly from catalog when computing coverage");
        }

        // Query CellBase to get gene coordinates and then apply the offset (up and downstream) to create a gene region
        String species = projectQueryResult.first().getOrganism().getScientificName();
        String assembly = projectQueryResult.first().getOrganism().getAssembly();
        CellBaseClient cellBaseClient = new CellBaseClient(storageEngineFactory.getVariantStorageEngine().getConfiguration().getCellbase()
                .toClientConfiguration());
        GeneClient geneClient = new GeneClient(species, assembly, cellBaseClient.getClientConfiguration());
        QueryResponse<Gene> response = geneClient.get(Arrays.asList(geneStr.split(",")), QueryOptions.empty());
        if (CollectionUtils.isNotEmpty(response.allResults())) {
            for (Gene gene : response.allResults()) {
                // Create region from gene coordinates
                Region region = null;
                if (onlyExons) {
                    if (geneOffset > 0) {
                        region = new Region(gene.getChromosome(), gene.getStart() - geneOffset, gene.getStart());
                        updateRegionMap(region, regionMap);
                        region = new Region(gene.getChromosome(), gene.getEnd(), gene.getEnd() + geneOffset);
                        updateRegionMap(region, regionMap);
                    }
                    if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
                        for (Transcript transcript : gene.getTranscripts()) {
                            if (CollectionUtils.isNotEmpty(transcript.getExons())) {
                                for (Exon exon : transcript.getExons()) {
                                    region = new Region(exon.getChromosome(), exon.getGenomicCodingStart() - exonOffset,
                                            exon.getGenomicCodingEnd() + exonOffset);
                                    updateRegionMap(region, regionMap);
                                }
                            }
                        }
                    }
                } else {
                    region = new Region(gene.getChromosome(), gene.getStart() - geneOffset, gene.getEnd() + geneOffset);
                    updateRegionMap(region, regionMap);
                }
            }
        }
        return new ArrayList<>(regionMap.values());
    }

    public void updateRegionMap(Region region, Map<String, Region> map) {
        if (!map.containsKey(region.toString())) {
            List<String> toRemove = new ArrayList<>();
            for (Region reg : map.values()) {
                // Check if the new region overlaps regions in the map
                if (region.overlaps(reg.getChromosome(), reg.getStart(), reg.getEnd())) {
                    // First, mark to remove the current region
                    toRemove.add(reg.toString());
                    // Second, extend the new region
                    region = new Region(reg.getChromosome(), Math.min(reg.getStart(), region.getStart()),
                            Math.max(reg.getEnd(), region.getEnd()));
                }
            }
            // Remove all marked regions
            for (String key : toRemove) {
                map.remove(key);
            }
            // Insert the new (or extended) region
            map.put(region.toString(), region);
        }
    }
}
