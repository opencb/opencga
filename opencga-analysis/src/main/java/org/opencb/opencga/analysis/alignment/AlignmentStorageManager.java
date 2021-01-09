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

package org.opencb.opencga.analysis.alignment;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.models.alignment.*;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.analysis.models.FileInfo;
import org.opencb.opencga.analysis.models.StudyInfo;
import org.opencb.opencga.analysis.wrappers.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentStorageEngine;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.core.api.ParamConstants.*;
import static org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine.ALIGNMENT_STATS_VARIABLE_SET;

/**
 * Created by pfurio on 31/10/16.
 */
public class AlignmentStorageManager extends StorageManager {

    private AlignmentStorageEngine alignmentStorageEngine;
    private String jobId;

    private static final Map<String, String> statsMap = new HashMap<>();

    public AlignmentStorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);

        // TODO: Create this alignmentStorageEngine by reflection
        this.alignmentStorageEngine = new LocalAlignmentStorageEngine();

        initStatsMap();
    }

    public AlignmentStorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory, String jobId) {
        super(catalogManager, storageEngineFactory);

        // TODO: Create this alignmentStorageEngine by reflection
        this.alignmentStorageEngine = new LocalAlignmentStorageEngine();
        this.jobId = jobId;

        initStatsMap();
    }

    //-------------------------------------------------------------------------
    // INDEX
    //-------------------------------------------------------------------------

    public void index(String study, String inputFile, boolean overwrite, String outdir, String token) throws ToolException {
        ObjectMap params = new ObjectMap();

        AlignmentIndexOperation indexOperation = new AlignmentIndexOperation();
        indexOperation.setUp(null, catalogManager, storageEngineFactory, params, Paths.get(outdir), jobId, token);

        indexOperation.setStudy(study);
        indexOperation.setInputFile(inputFile);
        indexOperation.setOverwrite(overwrite);

        indexOperation.start();
    }

    //-------------------------------------------------------------------------
    // QUERY
    //-------------------------------------------------------------------------

    public OpenCGAResult<ReadAlignment> query(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());

        return alignmentStorageEngine.getDBAdaptor().get(studyInfo.getFileInfo().getPhysicalFilePath(), query, options);
    }

    //-------------------------------------------------------------------------

    public AlignmentIterator<ReadAlignment> iterator(String studyId, String fileId, Query query, QueryOptions options,
                                                     String sessionId) throws CatalogException, IOException, StorageEngineException {
        return iterator(studyId, fileId, query, options, sessionId, ReadAlignment.class);
    }

    //-------------------------------------------------------------------------

    public <T> AlignmentIterator<T> iterator(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId,
                                             Class<T> clazz) throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());

        return alignmentStorageEngine.getDBAdaptor().iterator(studyInfo.getFileInfo().getPhysicalFilePath(), query, options, clazz);
    }


    //-------------------------------------------------------------------------
    // STATS: run, info and query
    //-------------------------------------------------------------------------

    public void statsRun(String study, String inputFile, String outdir, String token) throws ToolException {
        ObjectMap params = new ObjectMap();
        params.put(SamtoolsWrapperAnalysis.INDEX_STATS_PARAM, true);

        SamtoolsWrapperAnalysis samtools = new SamtoolsWrapperAnalysis();
        samtools.setUp(null, catalogManager, storageEngineFactory, params, Paths.get(outdir), jobId, token);

        samtools.setStudy(study);
        samtools.setCommand("stats")
                .setInputFile(inputFile)
                .setOutputFilename(Paths.get(inputFile).getFileName() + ".stats.txt");

        samtools.start();
    }

    //-------------------------------------------------------------------------

    public OpenCGAResult<SamtoolsStats> statsInfo(String study, String inputFile, String token) throws ToolException, CatalogException {
        OpenCGAResult<File> fileResult;
        fileResult = catalogManager.getFileManager().get(study, inputFile, QueryOptions.empty(), token);

        if (fileResult.getNumResults() == 1) {
            for (AnnotationSet annotationSet : fileResult.getResults().get(0).getAnnotationSets()) {
                if ("opencga_alignment_stats".equals(annotationSet.getId())) {
                    StopWatch watch = StopWatch.createStarted();
                    SamtoolsStats stats = JacksonUtils.getDefaultObjectMapper().convertValue(annotationSet.getAnnotations(),
                            SamtoolsStats.class);
                    watch.stop();
                    return new OpenCGAResult<>(((int) watch.getTime()), Collections.emptyList(), 1, Collections.singletonList(stats), 1);
                }
            }
            throw new ToolException("Alignment stats not computed for " + inputFile);
        } else {
            throw new ToolException("Error accessing to the file: " + inputFile);
        }
    }

    //-------------------------------------------------------------------------

    public OpenCGAResult<File> statsQuery(String study, Query query, QueryOptions queryOptions, String token) throws CatalogException {
        Query searchQuery = new Query();
        List<String> filters = new ArrayList<>();
        query.keySet().forEach(k -> {
            if (statsMap.containsKey(k)) {
                filters.add(ALIGNMENT_STATS_VARIABLE_SET + ":" + statsMap.get(k) + query.get(k));
            }
        });
        searchQuery.put(Constants.ANNOTATION, StringUtils.join(filters, ";"));

        return catalogManager.getFileManager().search(study, searchQuery, queryOptions, token);
    }

    //-------------------------------------------------------------------------
    // COVERAGE: run, query and ratio
    //-------------------------------------------------------------------------

    public void coverageRun(String study, String inputFile, int windowSize, boolean overwrite, String outdir, String token) throws ToolException {
        ObjectMap params = new ObjectMap();
        params.put("of", "bigwig");
        params.put("bs", windowSize);
        params.put("overwrite", overwrite);

        DeeptoolsWrapperAnalysis deeptools = new DeeptoolsWrapperAnalysis();

        deeptools.setUp(null, catalogManager, storageEngineFactory, params, Paths.get(outdir), jobId, token);

        deeptools.setStudy(study);
        deeptools.setCommand("bamCoverage")
                .setBamFile(inputFile);

        deeptools.start();
    }

    //-------------------------------------------------------------------------

    public OpenCGAResult<RegionCoverage> coverageQuery(String studyIdStr, String fileIdStr, Region region, int minCoverage, int maxCoverage,
                                                       int windowSize, String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().coverageQuery(Paths.get(file.getUri()), region, minCoverage, maxCoverage, windowSize);
    }

    //-------------------------------------------------------------------------

    public OpenCGAResult<GeneCoverageStats> coverageStats(String studyIdStr, String fileIdStr, List<String> geneNames, int threshold, String token)
            throws Exception {
        StopWatch watch = StopWatch.createStarted();

        List<GeneCoverageStats> geneCoverageStatsList = new ArrayList<>();

        // Get file
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, token);
//        System.out.println("file = " + file.getUri());

        // Get species and assembly from catalog
        OpenCGAResult<Project> projectQueryResult = catalogManager.getProjectManager().get(
                new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyIdStr),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), token);
        if (projectQueryResult.getNumResults() != 1) {
            throw new CatalogException("Error getting species and assembly from catalog");
        }
        String species = projectQueryResult.first().getOrganism().getScientificName();
        String assembly = projectQueryResult.first().getOrganism().getAssembly();

        for (String geneName : geneNames) {

            // Init gene coverage stats
            GeneCoverageStats geneCoverageStats = new GeneCoverageStats();
            geneCoverageStats.setFile(file.getId());

            geneCoverageStats.setGeneName(geneName);
            if (CollectionUtils.isNotEmpty(file.getSampleIds())) {
                geneCoverageStats.setSampleId(file.getSampleIds().get(0));
            }

            // Get exon regions per transcript
//            Map<String, List<Region>> exonRegions = getExonRegionsPerTranscript(geneName, species, assembly);
//            for (Map.Entry<String, List<Region>> entry : exonRegions.entrySet()) {
//                System.out.println(entry.getKey() + " -> " + StringUtils.join(entry.getValue().toArray(), ","));
//            }


            // Query CellBase to get gene coordinates and then apply the offset (up and downstream) to create a gene region
            CellBaseClient cellBaseClient = new CellBaseClient(storageEngineFactory.getVariantStorageEngine().getConfiguration().getCellbase()
                    .toClientConfiguration());
            GeneClient geneClient = new GeneClient(species, assembly, cellBaseClient.getClientConfiguration());
            QueryResponse<Gene> response = geneClient.get(Collections.singletonList(geneName), QueryOptions.empty());
            Gene gene = response.firstResult();
            if (gene != null) {
                List<TranscriptCoverageStats> transcriptCoverageStatsList = new ArrayList<>();
                // Create region from gene coordinates
                if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
                    // Compute coverage stats per transcript
                    for (Transcript transcript : gene.getTranscripts()) {
                        TranscriptCoverageStats transcriptCoverageStats = new TranscriptCoverageStats();
                        transcriptCoverageStats.setId(transcript.getId());
                        transcriptCoverageStats.setName(transcript.getName());
                        transcriptCoverageStats.setBiotype(transcript.getBiotype());
                        transcriptCoverageStats.setChromosome(transcript.getChromosome());
                        transcriptCoverageStats.setStart(transcript.getStart());
                        transcriptCoverageStats.setEnd(transcript.getEnd());
                        transcriptCoverageStats.setLowCoverageThreshold(threshold);

                        // Trasscript length as a sum of exon lengths
                        int length = 0;
                        int numExons = 0;
                        final int bp = 5;
                        // Coverage depths: 1x, 5x, 10x, 15x, 20x, 25x, 30x, 40x, 50x, 60x, 75x, 100x
                        double[] depths = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                        // List of low coverage regions and exon stats
                        List<LowCoverageRegionStats> lowCoverageRegionStats = new ArrayList<>();
                        List<ExonCoverageStats> exonCoverageStats = new ArrayList<>();

                        if (CollectionUtils.isNotEmpty(transcript.getExons())) {
                            for (Exon exon : transcript.getExons()) {
                                if (exon.getStart() != 0 && exon.getEnd() != 0) {
                                    numExons++;
                                    Region region = new Region(exon.getChromosome(), exon.getStart() - bp, exon.getEnd() + bp);
                                    length += (region.size());

                                    OpenCGAResult<RegionCoverage> regionResult = alignmentStorageEngine.getDBAdaptor().coverageQuery(
                                            Paths.get(file.getUri()), region, 0, Integer.MAX_VALUE, 1);

                                    RegionCoverage regionCoverage = regionResult.first();

                                    // Exon stats (skipping +/- bp)
                                    RegionCoverageStats stats = computeExonStats(regionCoverage, bp);
                                    ExonCoverageStats exonStats = new ExonCoverageStats(exon.getId(), exon.getChromosome(), exon.getStart(),
                                            exon.getEnd(), stats.getAvg(), stats.getMin(), stats.getMax());
                                    exonCoverageStats.add(exonStats);

                                    // % depths
                                    if (regionCoverage != null) {
                                        for (double coverage : regionCoverage.getValues()) {
                                            if (coverage >= 1) {
                                                depths[0]++;
                                                if (coverage >= 5) {
                                                    depths[1]++;
                                                    if (coverage >= 10) {
                                                        depths[2]++;
                                                        if (coverage >= 15) {
                                                            depths[3]++;
                                                            if (coverage >= 20) {
                                                                depths[4]++;
                                                                if (coverage >= 25) {
                                                                    depths[5]++;
                                                                    if (coverage >= 30) {
                                                                        depths[6]++;
                                                                        if (coverage >= 40) {
                                                                            depths[7]++;
                                                                            if (coverage >= 50) {
                                                                                depths[8]++;
                                                                                if (coverage >= 60) {
                                                                                    depths[9]++;
                                                                                    if (coverage >= 75) {
                                                                                        depths[10]++;
                                                                                        if (coverage >= 100) {
                                                                                            depths[11]++;
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        // Get low coverage regions, from 0 to threshold depth
                                        List<RegionCoverage> filteredRegions = BamUtils.filterByCoverage(regionCoverage, 0, threshold + 1);
                                        for (RegionCoverage filteredRegion : filteredRegions) {
                                            if (filteredRegion.getValues() != null && filteredRegion.getValues().length > 0) {
                                                lowCoverageRegionStats.add(new LowCoverageRegionStats(filteredRegion.getChromosome(),
                                                        filteredRegion.getStart(), filteredRegion.getEnd(),
                                                        filteredRegion.getStats().getAvg(), filteredRegion.getStats().getMin()));
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Set transcript length taking into account to remove the extra bp
                        transcriptCoverageStats.setLength(length - (2 * bp * numExons));

                        // Update (%) depths but taking into account the extra bp
                        for (int i = 0; i < depths.length; i++) {
                            depths[i] = depths[i] / length * 100.0;
                        }
                        transcriptCoverageStats.setDepths(depths);
                        transcriptCoverageStats.setLowCoverageRegionStats(lowCoverageRegionStats);
                        transcriptCoverageStats.setExonStats(exonCoverageStats);

                        transcriptCoverageStatsList.add(transcriptCoverageStats);
                    }
                }

                geneCoverageStats.setStats(transcriptCoverageStatsList);
            }

            geneCoverageStatsList.add(geneCoverageStats);
        }


        watch.stop();
        return new OpenCGAResult<>(((int) watch.getTime()), Collections.emptyList(), geneCoverageStatsList.size(), geneCoverageStatsList,
                geneCoverageStatsList.size());
    }

    private RegionCoverageStats computeExonStats(RegionCoverage regionCoverage, int bp) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double agg = 0;

        double[] values = regionCoverage.getValues();
        int lastPosition = values.length - bp;
        for (int i = bp; i < lastPosition; i++) {
            if (values[i] < min) {
                min = values[i];
            }
            if (values[i] > max) {
                max = values[i];
            }
            agg += values[i];
        }
        return new RegionCoverageStats((int) Math.round(min), (int) Math.round(max), agg / (values.length - (2 * bp)));
    }

//-------------------------------------------------------------------------
// Counts
//-------------------------------------------------------------------------

    public OpenCGAResult<Long> getTotalCounts(String studyIdStr, String fileIdStr, String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().getTotalCounts(Paths.get(file.getUri()));
    }


    public OpenCGAResult<Long> count(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long fileId = fileAndStudyId.getLong("fileId");
//        Path filePath = getFilePath(fileId, sessionId);

        return alignmentStorageEngine.getDBAdaptor().count(studyInfo.getFileInfo().getPhysicalFilePath(), query, options);
    }

//-------------------------------------------------------------------------
// MISCELANEOUS
//-------------------------------------------------------------------------

    public List<Region> mergeRegions(List<Region> regions, List<String> genes, boolean onlyExons, int offset, String study, String token)
            throws CatalogException, IOException, StorageEngineException {
        // Process initial regions
        Map<String, Region> regionMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(regions)) {
            for (Region region : regions) {
                Region newRegion = new Region(region.getChromosome(), region.getStart() -  offset, region.getEnd() + offset);
                updateRegionMap(newRegion, regionMap);
            }
        }

        // Get species and assembly from catalog
        OpenCGAResult<Project> projectQueryResult = catalogManager.getProjectManager().get(
                new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), study),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), token);
        if (projectQueryResult.getNumResults() != 1) {
            throw new CatalogException("Error getting species and assembly from catalog");
        }

        // Query CellBase to get gene coordinates and then apply the offset (up and downstream) to create a gene region
        String species = projectQueryResult.first().getOrganism().getScientificName();
        String assembly = projectQueryResult.first().getOrganism().getAssembly();
        CellBaseClient cellBaseClient = new CellBaseClient(storageEngineFactory.getVariantStorageEngine().getConfiguration().getCellbase()
                .toClientConfiguration());
        GeneClient geneClient = new GeneClient(species, assembly, cellBaseClient.getClientConfiguration());
        QueryResponse<Gene> response = geneClient.get(genes, QueryOptions.empty());
        if (CollectionUtils.isNotEmpty(response.allResults())) {
            for (Gene gene : response.allResults()) {
                // Create region from gene coordinates
                Region region = null;
                if (onlyExons) {
                    if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
                        for (Transcript transcript : gene.getTranscripts()) {
                            if (CollectionUtils.isNotEmpty(transcript.getExons())) {
                                for (Exon exon : transcript.getExons()) {
                                    region = new Region(exon.getChromosome(), exon.getGenomicCodingStart() - offset,
                                            exon.getGenomicCodingEnd() + offset);
                                    updateRegionMap(region, regionMap);
                                }
                            }
                        }
                    }
                } else {
                    region = new Region(gene.getChromosome(), gene.getStart() - offset, gene.getEnd() + offset);
                    updateRegionMap(region, regionMap);
                }
            }
        }
        return new ArrayList<>(regionMap.values());
    }

    private void updateRegionMap(Region region, Map<String, Region> map) {
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

//-------------------------------------------------------------------------
// PRIVATE METHODS
//-------------------------------------------------------------------------

    public Map<String, List<Region>> getExonRegionsPerTranscript(String geneName, String species, String assembly)
            throws StorageEngineException, IOException {
        // Init region map, where key = transcript and value = list of exon regions
        Map<String, List<Region>> regionMap = new HashMap<>();

        // Query CellBase to get gene coordinates and then apply the offset (up and downstream) to create a gene region
        CellBaseClient cellBaseClient = new CellBaseClient(storageEngineFactory.getVariantStorageEngine().getConfiguration().getCellbase()
                .toClientConfiguration());
        GeneClient geneClient = new GeneClient(species, assembly, cellBaseClient.getClientConfiguration());
        QueryResponse<Gene> response = geneClient.get(Collections.singletonList(geneName), QueryOptions.empty());
        Gene gene = response.firstResult();
        if (gene != null) {
            // Create region from gene coordinates
            if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
                for (Transcript transcript : gene.getTranscripts()) {
                    List<Region> regions = new ArrayList<>();
                    if (CollectionUtils.isNotEmpty(transcript.getExons())) {
                        for (Exon exon : transcript.getExons()) {
                            if (exon.getGenomicCodingEnd() != 0 && exon.getGenomicCodingStart() != 0) {
                                regions.add(new Region(exon.getChromosome(), exon.getGenomicCodingStart(), exon.getGenomicCodingEnd()));
                            }
                        }
                    }
                    if (CollectionUtils.isNotEmpty(regions)) {
                        regionMap.put(transcript.getId(), regions);
                    }
                }
            }
        }
        return regionMap;
    }

    private File extractAlignmentOrCoverageFile(String studyIdStr, String fileIdStr, String sessionId) throws CatalogException {
        OpenCGAResult<File> fileResult = catalogManager.getFileManager().get(studyIdStr, fileIdStr, QueryOptions.empty(), sessionId);
        if (fileResult.getNumResults() == 0) {
            throw new CatalogException("File " + fileIdStr + " not found");
        }

        File.Bioformat bioformat = fileResult.first().getBioformat();
        if (bioformat != File.Bioformat.ALIGNMENT && bioformat != File.Bioformat.COVERAGE) {
            throw new CatalogException("File " + fileResult.first().getName() + " not supported. "
                    + "Expecting an alignment or coverage file.");
        }
        return fileResult.first();
    }

    private void checkAlignmentBioformat(List<FileInfo> fileInfo) throws CatalogException {
        for (FileInfo file : fileInfo) {
            if (!file.getBioformat().equals(File.Bioformat.ALIGNMENT)) {
                throw new CatalogException("File " + file.getName() + " not supported. Expecting an alignment file.");
            }
        }
    }

    @Override
    public void testConnection() throws StorageEngineException {
    }

    @Deprecated
    private Path getFilePath(long fileId, String sessionId) throws CatalogException, IOException {
        QueryOptions fileOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.NAME.key()));
        OpenCGAResult<File> fileResult = catalogManager.getFileManager().get(fileId, fileOptions, sessionId);

        if (fileResult.getNumResults() != 1) {
            logger.error("Critical error: File {} not found in catalog.", fileId);
            throw new CatalogException("Critical error: File " + fileId + " not found in catalog");
        }

        Path path = Paths.get(fileResult.first().getUri().getRawPath());
        FileUtils.checkFile(path);

        return path;
    }

    @Deprecated
    private Path getWorkspace(long studyId, String sessionId) throws CatalogException, IOException {
        // Obtain the study uri
        QueryOptions studyOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
        OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), studyOptions, sessionId);
        if (studyResult .getNumResults() != 1) {
            logger.error("Critical error: Study {} not found in catalog.", studyId);
            throw new CatalogException("Critical error: Study " + studyId + " not found in catalog");
        }

        Path workspace = Paths.get(studyResult.first().getUri().getRawPath()).resolve(".opencga").resolve("alignments");
        if (!workspace.toFile().exists()) {
            Files.createDirectories(workspace);
        }

        return workspace;
    }

    private void initStatsMap() {
        statsMap.put(RAW_TOTAL_SEQUENCES, "raw_total_sequences");
        statsMap.put(FILTERED_SEQUENCES, "filtered_sequences");
        statsMap.put(READS_MAPPED, "reads_mapped");
        statsMap.put(READS_MAPPED_AND_PAIRED, "reads_mapped_and_paired");
        statsMap.put(READS_UNMAPPED, "reads_unmapped");
        statsMap.put(READS_PROPERLY_PAIRED, "reads_properly_paired");
        statsMap.put(READS_PAIRED, "reads_paired");
        statsMap.put(READS_DUPLICATED, "reads_duplicated");
        statsMap.put(READS_MQ0, "reads_MQ0");
        statsMap.put(READS_QC_FAILED, "reads_QC_failed");
        statsMap.put(NON_PRIMARY_ALIGNMENTS, "non_primary_alignments");
        statsMap.put(MISMATCHES, "mismatches");
        statsMap.put(ERROR_RATE, "error_rate");
        statsMap.put(AVERAGE_LENGTH, "average_length");
        statsMap.put(AVERAGE_FIRST_FRAGMENT_LENGTH, "average_first_fragment_length");
        statsMap.put(AVERAGE_LAST_FRAGMENT_LENGTH, "average_last_fragment_length");
        statsMap.put(AVERAGE_QUALITY, "average_quality");
        statsMap.put(INSERT_SIZE_AVERAGE, "insert_size_average");
        statsMap.put(INSERT_SIZE_STANDARD_DEVIATION, "insert_size_standard_deviation");
        statsMap.put(PAIRS_WITH_OTHER_ORIENTATION, "pairs_with_other_orientation");
        statsMap.put(PAIRS_ON_DIFFERENT_CHROMOSOMES, "pairs_on_different_chromosomes");
        statsMap.put(PERCENTAGE_OF_PROPERLY_PAIRED_READS, "percentage_of_properly_paired_reads");
    }
}
