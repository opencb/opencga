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
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.exceptions.AlignmentCoverageException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.wrappers.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

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
public class AlignmentAnalysisWSService extends AnalysisWSService {

    public AlignmentAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public AlignmentAnalysisWSService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);
    }

    //-------------------------------------------------------------------------
    // INDEX
    //-------------------------------------------------------------------------

    @POST
    @Path("/index")
    @ApiOperation(value = ALIGNMENT_INDEX_DESCRIPTION, response = Job.class)
    public Response index(@ApiParam(value = FILE_ID_DESCRIPTION, required = true) @QueryParam(value = FILE_ID_PARAM) String inputFile,
                          @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(STUDY_PARAM) String study) {

        Map<String, Object> params = new LinkedHashMap<>();
        params.put(FILE_ID_PARAM, inputFile);

        logger.info("ObjectMap: {}", params);

        try {
            OpenCGAResult<Job> queryResult = catalogManager.getJobManager().submit(study, "alignment-index", Enums.Priority.HIGH,
                    params, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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
    // COVERAGE: run, query and ratio
    //-------------------------------------------------------------------------

    @POST
    @Path("/coverage/run")
    @ApiOperation(value = "Compute coverage for a list of alignment files", response = Job.class)
    public Response coverageRun(@ApiParam(value = FILE_ID_DESCRIPTION, required = true) @QueryParam(value = FILE_ID_PARAM) String file,
                                @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
                                @ApiParam(value = COVERAGE_WINDOW_SIZE_DESCRIPTION) @DefaultValue(COVERAGE_WINDOW_SIZE_DEFAULT) @QueryParam(COVERAGE_WINDOW_SIZE_PARAM) int windowSize) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put(FILE_ID_PARAM, file);
        params.put(COVERAGE_WINDOW_SIZE_PARAM, windowSize);
        logger.debug("ObjectMap: {}", params);
        try {
            OpenCGAResult<Job> queryResult = catalogManager.getJobManager()
                    .submit(study, "alignment-coverage-run", Enums.Priority.HIGH, params, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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

    //-------------------------------------------------------------------------
    // STATS: run, info and query
    //-------------------------------------------------------------------------
    @POST
    @Path("/stats/run")
    @ApiOperation(value = ALIGNMENT_STATS_DESCRIPTION, response = Job.class)
    public Response statsRun(@ApiParam(value = FILE_ID_DESCRIPTION, required = true) @QueryParam(value = FILE_ID_PARAM) String inputFile,
                             @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(STUDY_PARAM) String study) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put(FILE_ID_PARAM, inputFile);
        logger.debug("ObjectMap: {}", params);
        try {
            OpenCGAResult<Job> queryResult = catalogManager.getJobManager()
                    .submit(study, "alignment-stats-run", Enums.Priority.HIGH, params, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats/info")
    @ApiOperation(value = ALIGNMENT_STATS_INFO_DESCRIPTION, response = String.class)
    public Response statsInfo(@ApiParam(value = FILE_ID_DESCRIPTION, required = true) @QueryParam(FILE_ID_PARAM) String inputFile,
                              @ApiParam(value = STUDY_DESCRIPTION) @QueryParam(STUDY_PARAM) String study) {
        AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);
        try {
            return createOkResponse(alignmentStorageManager.statsInfo(study, inputFile, token));
        } catch (ToolException | StorageEngineException | CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats/query")
    @ApiOperation(value = ALIGNMENT_STATS_QUERY_DESCRIPTION, response = File.class)
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
    public static class BwaRunParams extends ToolParams {
        public BwaRunParams() {
        }

        public BwaRunParams(String command, String fastaFile, String indexBaseFile, String fastq1File, String fastq2File,
                            String samFilename, String outdir, Map<String, String> bwaParams) {
            this.command = command;
            this.fastaFile = fastaFile;
            this.indexBaseFile = indexBaseFile;
            this.fastq1File = fastq1File;
            this.fastq2File = fastq2File;
            this.samFilename = samFilename;
            this.outdir = outdir;
            this.bwaParams = bwaParams;
        }

        public String command;       // Valid values: index or mem
        public String fastaFile;     //  Fasta file
        public String indexBaseFile; // Index base file
        public String fastq1File;    // FastQ #1 file
        public String fastq2File;    // FastQ #2 file
        public String samFilename;   // SAM file name
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
        return submitJob(BwaWrapperAnalysis.ID, study, params, jobName, jobDescription, jobTags);
    }

    // Samtools
    public static class SamtoolsRunParams extends ToolParams {
        public SamtoolsRunParams() {
        }

        public SamtoolsRunParams(String command, String inputFile, String outputFilename, String referenceFile, String readGroupFile,
                                 String bedFile, String refSeqFile, String referenceNamesFile, String targetRegionFile,
                                 String readsNotSelectedFilename, String outdir, Map<String, String> samtoolsParams) {
            this.command = command;
            this.inputFile = inputFile;
            this.outputFilename = outputFilename;
            this.referenceFile = referenceFile;
            this.readGroupFile = readGroupFile;
            this.bedFile = bedFile;
            this.refSeqFile = refSeqFile;
            this.referenceNamesFile = referenceNamesFile;
            this.targetRegionFile = targetRegionFile;
            this.readsNotSelectedFilename = readsNotSelectedFilename;
            this.outdir = outdir;
            this.samtoolsParams = samtoolsParams;
        }

        public String command;          // Valid values: view, index, sort, stats
        public String inputFile;        // Input file
        public String outputFilename;   // Output filename
        public String referenceFile;
        public String readGroupFile;
        public String bedFile;
        public String refSeqFile;
        public String referenceNamesFile;
        public String targetRegionFile;
        public String readsNotSelectedFilename;
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
        return submitJob(SamtoolsWrapperAnalysis.ID, study, params, jobName, jobDescription, jobTags);
    }

    // Deeptools
    public static class DeeptoolsRunParams extends ToolParams {
        public DeeptoolsRunParams() {
        }

        public DeeptoolsRunParams(String command, String bamFile, String outdir, Map<String, String> deeptoolsParams) {
            this.command = command;
            this.bamFile = bamFile;
            this.outdir = outdir;
            this.deeptoolsParams = deeptoolsParams;
        }

        public String command;     // Valid values: bamCoverage
        public String bamFile;        // BAM file
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
        return submitJob(DeeptoolsWrapperAnalysis.ID, study, params, jobName, jobDescription, jobTags);
    }

    // FastQC
    public static class FastqcRunParams extends ToolParams {
        public FastqcRunParams() {
        }

        public FastqcRunParams(String file, String outdir, Map<String, String> fastqcParams) {
            this.file = file;
            this.outdir = outdir;
            this.fastqcParams = fastqcParams;
        }

        public String file;        // Input file
        public String outdir;
        public Map<String, String> fastqcParams;
    }

    @POST
    @Path("/fastqc/run")
    @ApiOperation(value = FastqcWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response fastqcRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            AlignmentAnalysisWSService.FastqcRunParams params) {
        return submitJob(FastqcWrapperAnalysis.ID, study, params, jobName, jobDescription, jobTags);
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
