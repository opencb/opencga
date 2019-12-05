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

package org.opencb.opencga.analysis.alignment;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.update.FileUpdateParams;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.VariableSet;
import org.opencb.opencga.core.results.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentStorageEngine;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.analysis.models.FileInfo;
import org.opencb.opencga.analysis.models.StudyInfo;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by pfurio on 31/10/16.
 */
public class AlignmentStorageManager extends StorageManager {

    private AlignmentStorageEngine alignmentStorageEngine;

    private static final String GLOBAL_STATS = "globalStats";

    public AlignmentStorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);

        // TODO: Create this alignmentStorageEngine by reflection
        this.alignmentStorageEngine = new LocalAlignmentStorageEngine();
    }


    public void index(String studyIdStr, String fileIdStr, Path outDir, ObjectMap options, String sessionId) throws Exception {
        options = ParamUtils.defaultObject(options, ObjectMap::new);
        StopWatch watch = new StopWatch();

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());
        FileInfo fileInfo = studyInfo.getFileInfo();
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long studyId = fileAndStudyId.getLong("studyId");
//        long fileId = fileAndStudyId.getLong("fileId");
//        Path filePath = getFilePath(fileId, sessionId);
//        Path workspace = getWorkspace(studyId, sessionId);

        Path linkedBamFilePath = Files.createSymbolicLink(outDir.resolve(fileInfo.getName()), fileInfo.getPhysicalFilePath());

        List<URI> fileUris = Arrays.asList(linkedBamFilePath.toUri());

        // TODO: Check if index is already created and link bai file
        logger.info("Creating index...");
        watch.start();
        try {
            alignmentStorageEngine.index(fileUris, outDir.toUri(), false, options.getBoolean("transform"), options.getBoolean("load"));
        } finally {
            // Remove symbolic link
            Files.delete(linkedBamFilePath);
        }
        watch.stop();
        logger.info("Indexing took {} seconds", watch.getTime() / 1000.0);

        // Create the stats and store them in catalog
        logger.info("Calculating the stats...");
        watch.reset();
        watch.start();
        DataResult<AlignmentGlobalStats> stats = alignmentStorageEngine.getDBAdaptor().stats(fileInfo.getPhysicalFilePath(), outDir);

        if (stats != null && stats.getNumResults() == 1) {
            // Store the stats in catalog
            ObjectWriter objectWriter = new ObjectMapper().typedWriter(AlignmentGlobalStats.class);
            ObjectMap globalStats = new ObjectMap(GLOBAL_STATS, objectWriter.writeValueAsString(stats.first()));
            FileUpdateParams fileUpdateParams = new FileUpdateParams().setStats(globalStats);
            catalogManager.getFileManager().update(studyIdStr, fileInfo.getPath(), fileUpdateParams,
                    new QueryOptions(), sessionId);

            // Remove the stats file
            Path statsFile = outDir.resolve(fileInfo.getName() + ".stats");
            if (statsFile.toFile().exists()) {
                Files.delete(statsFile);
            }
        }
        watch.stop();
        logger.info("Stats calculation took {} seconds", watch.getTime() / 1000.0);

        // Create the coverage
        logger.info("Calculating the coverage...");
        watch.reset();
        watch.start();
//        alignmentStorageEngine.getDBAdaptor().coverage(fileInfo.getPath(), studyInfo.getWorkspace());
        watch.stop();
        logger.info("Coverage calculation took {} seconds", watch.getTime() / 1000.0);
    }

    public DataResult<ReadAlignment> query(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());

        return alignmentStorageEngine.getDBAdaptor().get(studyInfo.getFileInfo().getPhysicalFilePath(), query, options);
    }

    public AlignmentIterator<ReadAlignment> iterator(String studyId, String fileId, Query query, QueryOptions options,
                                                     String sessionId) throws CatalogException, IOException, StorageEngineException {
        return iterator(studyId, fileId, query, options, sessionId, ReadAlignment.class);
    }

    public <T> AlignmentIterator<T> iterator(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId,
                                             Class<T> clazz) throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long fileId = fileAndStudyId.getLong("fileId");
//        Path filePath = getFilePath(fileId, sessionId);

        return alignmentStorageEngine.getDBAdaptor().iterator(studyInfo.getFileInfo().getPhysicalFilePath(), query, options, clazz);
//        return alignmentDBAdaptor.iterator((Path) fileInfo.get("filePath"), query, options, clazz);
    }

    public DataResult<AlignmentGlobalStats> stats(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws Exception {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());
        FileInfo fileInfo = studyInfo.getFileInfo();
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long studyId = fileAndStudyId.getLong("studyId");
//        long fileId = fileAndStudyId.getLong("fileId");

        if (query.isEmpty() && options.isEmpty()) {
            QueryOptions includeOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.STATS.key());
            DataResult<File> fileDataResult = catalogManager.getFileManager().get(fileInfo.getFileUid(), includeOptions, sessionId);

            logger.info("Obtaining the stats from catalog...");

            if (fileDataResult.getNumResults() == 1) {
                Map<String, Object> stats = fileDataResult.first().getStats();
                Object value = stats.get(GLOBAL_STATS);
                if (value != null && value instanceof String) {
                    ObjectReader reader = new ObjectMapper().reader(AlignmentGlobalStats.class);
                    AlignmentGlobalStats globalStats = reader.readValue((String) value);
                    return new DataResult<>(fileDataResult.getTime(), fileDataResult.getEvents(), 1, Arrays.asList(globalStats), 1);
                }

            }
        }

        // Calculate the stats
        logger.info("Calculating the stats...");
//        Path filePath = getFilePath(fileId, sessionId);
//        Path workspace = getWorkspace(studyId, sessionId);
        return alignmentStorageEngine.getDBAdaptor().stats(fileInfo.getPhysicalFilePath(), studyInfo.getWorkspace(), query, options);

    }


    public void statsRun(String study, String inputFile, String outdir, String token) throws AnalysisException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.ID.key(), "fileId")
                .append(Constants.ANNOTATION, Constants.VARIABLE_SET +"=alignment_stats");
        try {
            if (catalogManager.getFileManager().count(study, query, token).getNumMatches() > 0) {
                // Skip
                return;
            }
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }

        ObjectMap params = new ObjectMap();
        params.put(SamtoolsWrapperAnalysis.CALLBACK, (Consumer<SamtoolsWrapperAnalysis>) w -> statsRunCallback(w));

        SamtoolsWrapperAnalysis samtools = new SamtoolsWrapperAnalysis();
        samtools.setUp(null, catalogManager, storageEngineFactory, params, Paths.get(outdir), token);

        samtools.setStudy(study);
        samtools.setCommand("stats")
                .setInputFile(inputFile);

        samtools.start();
    }

    private void statsRunCallback(SamtoolsWrapperAnalysis samtools) {
        try {
            OpenCGAResult<File> fileResult = catalogManager.getFileManager().get(samtools.getStudy(), samtools.getInputFile(),
                    QueryOptions.empty(), samtools.getToken());

            URI uri = fileResult.getResults().get(0).getUri();
            java.io.File inputFile = new java.io.File(uri.getPath());
            java.io.File outputFile = new java.io.File(samtools.getOutDir() + "/" + inputFile.getName() + ".stats.txt");

            Path linkedFile = Files.createSymbolicLink(inputFile.getParentFile().toPath().resolve(outputFile.getName()),
                    Paths.get(outputFile.getAbsolutePath()));

            // Create a variable set with the summary numbers of the statistics
            Map<String, Object> annotations = new HashMap<>();
            List<String> lines = org.apache.commons.io.FileUtils.readLines(outputFile, Charset.defaultCharset());
            int count = 0;

            for (String line : lines) {
                if (line.startsWith("SN")) {
                    count++;
                    String[] splits = line.split("\t");
                    String key = splits[1].split("\\(")[0].trim().replace(" ", "_").replace(":", "");
                    if (line.contains("bases mapped (cigar):")) {
                        key += "_cigar";
                    }
                    String value = splits[2].split(" ")[0];
                    annotations.put(key, value);

                } else if (count > 0) {
                    break;
                }
            }

            AnnotationSet annotationSet = new AnnotationSet("alignment_stats", "alignment_stats", annotations);

            FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

            catalogManager.getFileManager().update(samtools.getStudy(), samtools.getInputFile(), updateParams, QueryOptions.empty(),
                    samtools.getToken());
        } catch (CatalogException | IOException e) {
            // TODO: be nice
            e.printStackTrace();
        }
    }

    public DataResult<RegionCoverage> coverage(String studyIdStr, String fileIdStr, Region region, int windowSize, String sessionId)
            throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().coverage(Paths.get(file.getUri()), region, windowSize);
    }

    public DataResult<RegionCoverage> coverage(String studyIdStr, String fileIdStr, Region region, int minCoverage, int maxCoverage,
                                               String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().coverage(Paths.get(file.getUri()), region, minCoverage, maxCoverage);
    }

    public DataResult<RegionCoverage> getLowCoverageRegions(String studyIdStr, String fileIdStr, Region region, int minCoverage,
                                                            String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().getLowCoverageRegions(Paths.get(file.getUri()), region, minCoverage);
    }

    public DataResult<Long> getTotalCounts(String studyIdStr, String fileIdStr, String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().getTotalCounts(Paths.get(file.getUri()));
    }


    File extractAlignmentOrCoverageFile(String studyIdStr, String fileIdStr, String sessionId) throws CatalogException {
        DataResult<File> fileDataResult = catalogManager.getFileManager().get(studyIdStr, fileIdStr,
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.URI.key(),
                        FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.FORMAT.key())), sessionId);
        if (fileDataResult.getNumResults() == 0) {
            throw new CatalogException("File " + fileIdStr + " not found");
        }

        File.Bioformat bioformat = fileDataResult.first().getBioformat();
        if (bioformat != File.Bioformat.ALIGNMENT && bioformat != File.Bioformat.COVERAGE) {
            throw new CatalogException("File " + fileDataResult.first().getName() + " not supported. "
                    + "Expecting an alignment or coverage file.");
        }
        return fileDataResult.first();
    }

    public DataResult<Long> count(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long fileId = fileAndStudyId.getLong("fileId");
//        Path filePath = getFilePath(fileId, sessionId);

        return alignmentStorageEngine.getDBAdaptor().count(studyInfo.getFileInfo().getPhysicalFilePath(), query, options);
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
        DataResult<File> fileDataResult = catalogManager.getFileManager().get(fileId, fileOptions, sessionId);

        if (fileDataResult.getNumResults() != 1) {
            logger.error("Critical error: File {} not found in catalog.", fileId);
            throw new CatalogException("Critical error: File " + fileId + " not found in catalog");
        }

        Path path = Paths.get(fileDataResult.first().getUri().getRawPath());
        FileUtils.checkFile(path);

        return path;
    }

    @Deprecated
    private Path getWorkspace(long studyId, String sessionId) throws CatalogException, IOException {
        // Obtain the study uri
        QueryOptions studyOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
        DataResult<Study> studyDataResult = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), studyOptions, sessionId);
        if (studyDataResult .getNumResults() != 1) {
            logger.error("Critical error: Study {} not found in catalog.", studyId);
            throw new CatalogException("Critical error: Study " + studyId + " not found in catalog");
        }

        Path workspace = Paths.get(studyDataResult.first().getUri().getRawPath()).resolve(".opencga").resolve("alignments");
        if (!workspace.toFile().exists()) {
            Files.createDirectories(workspace);
        }

        return workspace;
    }

}
