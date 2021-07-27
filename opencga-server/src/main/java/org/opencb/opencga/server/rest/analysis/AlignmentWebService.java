/*
 * Copyright 2015-2020 OpenCB
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.exceptions.AlignmentCoverageException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.alignment.AlignmentIndexOperation;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.alignment.qc.*;
import org.opencb.opencga.analysis.wrappers.bwa.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.picard.PicardWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.alignment.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
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
public class AlignmentWebService extends AnalysisWebService {

    public AlignmentWebService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public AlignmentWebService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);
    }

    //-------------------------------------------------------------------------
    // INDEX
    //-------------------------------------------------------------------------

    @POST
    @Path("/index/run")
    @ApiOperation(value = ALIGNMENT_INDEX_DESCRIPTION, response = Job.class)
    public Response indexRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = AlignmentIndexParams.DESCRIPTION, required = true) AlignmentIndexParams params) {
        return submitJob(AlignmentIndexOperation.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    //-------------------------------------------------------------------------
    // QUERY
    //-------------------------------------------------------------------------

    @GET
    @Path("/query")
    @ApiOperation(value = ALIGNMENT_QUERY_DESCRIPTION, response = ReadAlignment.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response query(@ApiParam(value = FILE_ID_DESCRIPTION, required = true) @QueryParam(FILE_ID_PARAM) String fileIdStr,
                          @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
                          @ApiParam(value = REGION_DESCRIPTION) @QueryParam(REGION_PARAM) String regionStr,
                          @ApiParam(value = GENE_DESCRIPTION) @QueryParam(GENE_PARAM) String geneStr,
                          @ApiParam(value = OFFSET_DESCRIPTION) @DefaultValue(OFFSET_DEFAULT) @QueryParam(OFFSET_PARAM) int offset,
                          @ApiParam(value = ONLY_EXONS_DESCRIPTION) @QueryParam(ONLY_EXONS_PARAM) @DefaultValue("false") Boolean onlyExons,
                          @ApiParam(value = MINIMUM_MAPPING_QUALITY_DESCRIPTION) @QueryParam(MINIMUM_MAPPING_QUALITY_PARAM) Integer minMappingQuality,
                          @ApiParam(value = MAXIMUM_NUMBER_MISMATCHES_DESCRIPTION) @QueryParam(MAXIMUM_NUMBER_MISMATCHES_PARAM) Integer maxNumMismatches,
                          @ApiParam(value = MAXIMUM_NUMBER_HITS_DESCRIPTION) @QueryParam(MAXIMUM_NUMBER_HITS_PARAM) Integer maxNumHits,
                          @ApiParam(value = PROPERLY_PAIRED_DESCRIPTION) @QueryParam(PROPERLY_PAIRED_PARAM) @DefaultValue("false") Boolean properlyPaired,
                          @ApiParam(value = MAXIMUM_INSERT_SIZE_DESCRIPTION) @QueryParam(MAXIMUM_INSERT_SIZE_PARAM) Integer maxInsertSize,
                          @ApiParam(value = SKIP_UNMAPPED_DESCRIPTION) @QueryParam(SKIP_UNMAPPED_PARAM) @DefaultValue("false") Boolean unmapped,
                          @ApiParam(value = SKIP_DUPLICATED_DESCRIPTION) @QueryParam(SKIP_DUPLICATED_PARAM) @DefaultValue("false") Boolean duplicated,
                          @ApiParam(value = REGION_CONTAINED_DESCRIPTION) @DefaultValue("false") @QueryParam(REGION_CONTAINED_PARAM) Boolean contained,
                          @ApiParam(value = FORCE_MD_FIELD_DESCRIPTION) @DefaultValue("false") @QueryParam(FORCE_MD_FIELD_PARAM) Boolean forceMDField,
                          @ApiParam(value = BIN_QUALITIES_DESCRIPTION) @QueryParam(BIN_QUALITIES_PARAM) @DefaultValue("false") Boolean binQualities,
                          @ApiParam(value = SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION) @DefaultValue("false") @QueryParam(SPLIT_RESULTS_INTO_REGIONS_PARAM) boolean splitResults) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);

            Query query = new Query();
            query.putIfNotNull(MINIMUM_MAPPING_QUALITY_PARAM, minMappingQuality);
            query.putIfNotNull(MAXIMUM_NUMBER_MISMATCHES_PARAM, maxNumMismatches);
            query.putIfNotNull(MAXIMUM_NUMBER_HITS_PARAM, maxNumHits);
            query.putIfNotNull(PROPERLY_PAIRED_PARAM, properlyPaired);
            query.putIfNotNull(MAXIMUM_INSERT_SIZE_PARAM, maxInsertSize);
            query.putIfNotNull(SKIP_UNMAPPED_PARAM, unmapped);
            query.putIfNotNull(SKIP_DUPLICATED_PARAM, duplicated);

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotNull(REGION_CONTAINED_PARAM, contained);
            queryOptions.putIfNotNull(FORCE_MD_FIELD_PARAM, forceMDField);
            queryOptions.putIfNotNull(BIN_QUALITIES_PARAM, binQualities);
            queryOptions.putIfNotNull(QueryOptions.LIMIT, limit);
            queryOptions.putIfNotNull(QueryOptions.SKIP, skip);
            //queryOptions.putIfNotNull(QueryOptions.COUNT, count);

            // Parse regions from region parameter
            List<Region> inputRegions = Region.parseRegions(regionStr);
            List<String> inputGenes = StringUtils.isEmpty(geneStr) ? new ArrayList<>() : Arrays.asList(geneStr.split(","));

            // Merge regions
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
            List<Region> regionList = alignmentStorageManager.mergeRegions(inputRegions, inputGenes, onlyExons, offset, study, token);

            List<OpenCGAResult> results = new ArrayList<>();
            for (Region region : regionList) {
                String queryRegion = region.toString();
                if (StringUtils.isNotEmpty(queryRegion)) {
                    query.putIfNotNull(REGION_PARAM, queryRegion);

                    if (count) {
                        results.add(alignmentStorageManager.count(study, fileIdStr, query, queryOptions, token));
                    } else {
                        results.add(alignmentStorageManager.query(study, fileIdStr, query, queryOptions, token));
                    }
                }
            }

            return createOkResponse(postProcessingResult(results, splitResults, regionList));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    //-------------------------------------------------------------------------
    // COVERAGE: index/run, query, ratio and stats
    //-------------------------------------------------------------------------

    @POST
    @Path("/coverage/index/run")
    @ApiOperation(value = "Compute coverage for a list of alignment files", response = Job.class)
    public Response coverageRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = CoverageIndexParams.DESCRIPTION, required = true) CoverageIndexParams params) {

        return null;
        //        logger.debug("ObjectMap: {}", params);
//
//        DeeptoolsWrapperParams deeptoolsParams = new DeeptoolsWrapperParams();
//        deeptoolsParams.setCommand("bamCoverage");
//        deeptoolsParams.setBamFile(params.getFile());
//
//        Map<String, String> bamCoverageParams = new HashMap<>();
//        bamCoverageParams.put("bs", String.valueOf(params.getWindowSize() < 1 ? 1 : params.getWindowSize()));
//        bamCoverageParams.put("of", "bigwig");
//        bamCoverageParams.put("minMappingQuality", "20");
//        deeptoolsParams.setDeeptoolsParams(bamCoverageParams);
//
//        logger.debug("ObjectMap (DeepTools) : {}", bamCoverageParams);
//
//        return submitJob(DeeptoolsWrapperAnalysis.ID, study, deeptoolsParams, jobName, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/coverage/query")
    @ApiOperation(value = ALIGNMENT_COVERAGE_QUERY_DESCRIPTION, response = RegionCoverage.class)
    public Response coverageQuery(
            @ApiParam(value = FILE_ID_DESCRIPTION, required = true) @QueryParam(FILE_ID_PARAM) String inputFile,
            @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = REGION_DESCRIPTION) @QueryParam(REGION_PARAM) String regionStr,
            @ApiParam(value = GENE_DESCRIPTION) @QueryParam(GENE_PARAM) String geneStr,
            @ApiParam(value = OFFSET_DESCRIPTION) @DefaultValue(OFFSET_DEFAULT) @QueryParam(OFFSET_PARAM) int offset,
            @ApiParam(value = ONLY_EXONS_DESCRIPTION) @QueryParam(ONLY_EXONS_PARAM) @DefaultValue("false") Boolean onlyExons,
            @ApiParam(value = COVERAGE_RANGE_DESCRIPTION) @QueryParam(COVERAGE_RANGE_PARAM) String range,
            @ApiParam(value = COVERAGE_WINDOW_SIZE_DESCRIPTION) @DefaultValue(COVERAGE_WINDOW_SIZE_DEFAULT) @QueryParam(COVERAGE_WINDOW_SIZE_PARAM) int windowSize,
            @ApiParam(value = SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION) @DefaultValue("false") @QueryParam(SPLIT_RESULTS_INTO_REGIONS_PARAM) boolean splitResults) {
        try {
            ParamUtils.checkIsSingleID(inputFile);
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            // Parse regions from region parameter
            List<Region> inputRegions = Region.parseRegions(regionStr);
            List<String> inputGenes = StringUtils.isEmpty(geneStr) ? new ArrayList<>() : Arrays.asList(geneStr.split(","));

            // Merge regions
            List<Region> regionList = alignmentStorageManager.mergeRegions(inputRegions, inputGenes, onlyExons, offset, study, token);

            if (CollectionUtils.isNotEmpty(regionList)) {
                List<OpenCGAResult> results = new ArrayList<>();
                if (StringUtils.isEmpty(range)) {
                    for (Region region : regionList) {
                        OpenCGAResult<RegionCoverage> coverage = alignmentStorageManager
                                .coverageQuery(study, inputFile, region, 0, Integer.MAX_VALUE, windowSize, token);
//                        if (coverage.getResults().size() > 0) {
                        results.add(coverage);
//                        }
                    }
                } else {
                    // Report regions for a given coverage range
                    int minCoverage;
                    int maxCoverage;
                    String[] split = range.split("-");
                    try {
                        if (split.length == 1) {
                            minCoverage = 0;
                            maxCoverage = Integer.parseInt(split[0]);
                        } else if (split.length == 2) {
                            minCoverage = Integer.parseInt(split[0]);
                            maxCoverage = Integer.parseInt(split[1]);
                        } else {
                            return createErrorResponse(new AlignmentCoverageException("Invalid coverage range: " + range
                                    + ". Valid ranges include minimum and maximum values, e.g.: 20-60"));
                        }
                    } catch (NumberFormatException e) {
                        return createErrorResponse(new AlignmentCoverageException("Invalid coverage range: " + range
                                + ". Valid ranges include minimum and maximum values, e.g.: 20-60"));
                    }
                    if (minCoverage > maxCoverage) {
                        return createErrorResponse(new AlignmentCoverageException("Invalid coverage range: " + range
                                + ". The maximum value must be greater or equal to the minimum value, e.g.: 20-60"));
                    }

                    if (windowSize != 1) {
                        return createErrorResponse(new AlignmentCoverageException("Invalid window size: " + windowSize
                                + ". Window size must be 1 when retrieving coverage with a given threshold"));
                    }

                    for (Region region : regionList) {
                        OpenCGAResult<RegionCoverage> coverage = alignmentStorageManager
                                .coverageQuery(study, inputFile, region, minCoverage, maxCoverage, windowSize, token);
//                        if (coverage.getResults().size() > 0) {
                        results.add(coverage);
//                        }
                    }
                }
                return createOkResponse(postProcessingResult(results, splitResults, regionList));
            } else {
                return createErrorResponse("coverage/query", "Missing region(s)");
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/coverage/ratio")
    @ApiOperation(value = ALIGNMENT_COVERAGE_RATIO_DESCRIPTION, response = RegionCoverage.class)
    public Response coverageRatio(@ApiParam(value = FILE_ID_1_DESCRIPTION, required = true) @QueryParam(FILE_ID_1_PARAM) String somaticFile,
                                  @ApiParam(value = FILE_ID_2_DESCRIPTION, required = true) @QueryParam(FILE_ID_2_PARAM) String germlineFile,
                                  @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
                                  @ApiParam(value = SKIP_LOG2_DESCRIPTION) @QueryParam(SKIP_LOG2_PARAM) @DefaultValue("false") Boolean skipLog2,
                                  @ApiParam(value = REGION_DESCRIPTION) @QueryParam(REGION_PARAM) String regionStr,
                                  @ApiParam(value = GENE_DESCRIPTION) @QueryParam(GENE_PARAM) String geneStr,
                                  @ApiParam(value = OFFSET_DESCRIPTION) @DefaultValue(OFFSET_DEFAULT) @QueryParam(OFFSET_PARAM) int offset,
                                  @ApiParam(value = ONLY_EXONS_DESCRIPTION) @QueryParam(ONLY_EXONS_PARAM) @DefaultValue("false") Boolean onlyExons,
                                  @ApiParam(value = COVERAGE_WINDOW_SIZE_DESCRIPTION) @DefaultValue("" + COVERAGE_WINDOW_SIZE_DEFAULT) @QueryParam(COVERAGE_WINDOW_SIZE_PARAM) int windowSize,
                                  @ApiParam(value = SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION) @DefaultValue("false") @QueryParam(SPLIT_RESULTS_INTO_REGIONS_PARAM) boolean splitResults) {
        try {
            ParamUtils.checkIsSingleID(somaticFile);
            ParamUtils.checkIsSingleID(germlineFile);
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            // Parse regions from region parameter
            List<Region> inputRegions = Region.parseRegions(regionStr);
            List<String> inputGenes = StringUtils.isEmpty(geneStr) ? new ArrayList<>() : Arrays.asList(geneStr.split(","));

            // Merge regions
            List<Region> regionList = alignmentStorageManager.mergeRegions(inputRegions, inputGenes, onlyExons, offset, study, token);

            if (CollectionUtils.isNotEmpty(regionList)) {
                // Getting total counts for file #1: somatic file
                OpenCGAResult<Long> somaticResult = alignmentStorageManager.getTotalCounts(study, somaticFile, token);
                if (CollectionUtils.isEmpty(somaticResult.getResults()) || somaticResult.getResults().get(0) == 0) {
                    return createErrorResponse("Coverage ratio", "Impossible get total counts for file " + somaticFile);
                }
                long somaticTotalCounts = somaticResult.getResults().get(0);

                // Getting total counts for file #2: germline file
                OpenCGAResult<Long> germlineResult = alignmentStorageManager.getTotalCounts(study, germlineFile, token);
                if (CollectionUtils.isEmpty(germlineResult.getResults()) || germlineResult.getResults().get(0) == 0) {
                    return createErrorResponse("Coverage ratio", "Impossible get total counts for file " + germlineFile);
                }
                long germlineTotalCounts = germlineResult.getResults().get(0);

                // Compute (log2) coverage ratio for each region given
                List<OpenCGAResult> results = new ArrayList<>();
                for (Region region : regionList) {
                    OpenCGAResult<RegionCoverage> somaticCoverage = alignmentStorageManager
                            .coverageQuery(study, somaticFile, region, 0, Integer.MAX_VALUE, windowSize, token);
                    OpenCGAResult<RegionCoverage> germlineCoverage = alignmentStorageManager
                            .coverageQuery(study, germlineFile, region, 0, Integer.MAX_VALUE, windowSize, token);
                    if (somaticCoverage.getResults().size() == 1 && germlineCoverage.getResults().size() == 1) {
                        try {
                            StopWatch watch = StopWatch.createStarted();
                            RegionCoverage coverage = BamUtils.coverageRatio(somaticCoverage.getResults().get(0), somaticTotalCounts,
                                    germlineCoverage.getResults().get(0), germlineTotalCounts, !skipLog2);
                            int dbTime = somaticResult.getTime() + somaticCoverage.getTime()
                                    + germlineResult.getTime() + germlineCoverage.getTime() + ((int) watch.getTime());
                            results.add(new OpenCGAResult<>(dbTime, Collections.emptyList(), 1, Collections.singletonList(coverage), 1));
                        } catch (AlignmentCoverageException e) {
                            logger.error("Coverage ratio: {}: somatic file = {}, germline file = {}, region = {}",
                                    e.getMessage(), somaticFile, germlineFile, region.toString());
                        }
                    } else {
                        logger.error("Coverage ratio: something wrong happened: somatic file = {}, germline file = {}, region = {}",
                                somaticFile, germlineFile, region.toString());
                    }
                }
                return createOkResponse(postProcessingResult(results, splitResults, regionList));
            } else {
                return createErrorResponse("Coverage ratio", "Missing region, no region provides");
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/coverage/stats")
    @ApiOperation(value = ALIGNMENT_COVERAGE_STATS_DESCRIPTION, response = GeneCoverageStats.class)
    public Response coverageQuery(
            @ApiParam(value = FILE_ID_DESCRIPTION, required = true) @QueryParam(FILE_ID_PARAM) String inputFile,
            @ApiParam(value = GENE_DESCRIPTION, required = true) @QueryParam(GENE_PARAM) String geneStr,
            @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = LOW_COVERAGE_REGION_THRESHOLD_DESCRIPTION) @DefaultValue(LOW_COVERAGE_REGION_THRESHOLD_DEFAULT) @QueryParam(LOW_COVERAGE_REGION_THRESHOLD_PARAM) int threshold
    ) {
        try {
            ParamUtils.checkIsSingleID(inputFile);
            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            // Gene list
            if (StringUtils.isEmpty(geneStr)) {
                createErrorResponse("Coverage stats", "Missing genes.");
            }
            List<String> inputGenes = Arrays.asList(geneStr.split(","));

            return createOkResponse(alignmentStorageManager.coverageStats(study, inputFile, inputGenes, threshold, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/coverage/qc/geneCoverageStats/run")
    @ApiOperation(value = ALIGNMENT_GENE_COVERAGE_STATS_DESCRIPTION, response = Job.class)
    public Response geneCoverageStatsRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = AlignmentGeneCoverageStatsParams.DESCRIPTION, required = true) AlignmentGeneCoverageStatsParams params) {

        return submitJob(AlignmentGeneCoverageStatsAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    //-------------------------------------------------------------------------
    // Quality control (QC): stats, flag stats, FastQC and HS metrics
    //-------------------------------------------------------------------------

    @POST
    @Path("/qc/run")
    @ApiOperation(value = ALIGNMENT_QC_DESCRIPTION, response = Job.class)
    public Response qcRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = AlignmentQcParams.DESCRIPTION, required = true) AlignmentQcParams params) {

        return submitJob(AlignmentQcAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S
    //-------------------------------------------------------------------------

    @POST
    @Path("/bwa/run")
    @ApiOperation(value = BwaWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response bwaRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = BwaWrapperParams.DESCRIPTION, required = true) BwaWrapperParams params) {
        return submitJob(BwaWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/samtools/run")
    @ApiOperation(value = SamtoolsWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response samtoolsRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = SamtoolsWrapperParams.DESCRIPTION, required = true) SamtoolsWrapperParams params) {
        return submitJob(SamtoolsWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/deeptools/run")
    @ApiOperation(value = DeeptoolsWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response deeptoolsRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = DeeptoolsWrapperParams.DESCRIPTION, required = true) DeeptoolsWrapperParams params) {
        return submitJob(DeeptoolsWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/fastqc/run")
    @ApiOperation(value = FastqcWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response fastqcRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = FastqcWrapperParams.DESCRIPTION, required = true) FastqcWrapperParams params) {
        return submitJob(FastqcWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/picard/run")
    @ApiOperation(value = PicardWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response picardRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = PicardWrapperParams.DESCRIPTION, required = true) PicardWrapperParams params) {
        return submitJob(PicardWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private OpenCGAResult postProcessingResult(List<OpenCGAResult> results, boolean splitResults, List<Region> regionList) {
        OpenCGAResult finalResult = OpenCGAResult.empty();
        if (results.size() == 1) {
            finalResult = results.get(0);
        } else if (results.size() > 1){
            if (splitResults) {
                // Keep results split
                int time = results.stream().mapToInt(OpenCGAResult::getTime).sum();
                finalResult = new OpenCGAResult(time, Collections.emptyList(), results.size(), results, results.size());
            } else {
                // Merge results
                for (OpenCGAResult result : results) {
                    finalResult.append(result);
                }
            }
        }

        // Add fetched regions in the final result attributes
        finalResult.getAttributes().put("opencga_fetched_regions", StringUtils.join(regionList, ","));
        return finalResult;
    }
}
