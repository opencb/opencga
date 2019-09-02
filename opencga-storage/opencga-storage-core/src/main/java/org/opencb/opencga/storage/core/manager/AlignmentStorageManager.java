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

package org.opencb.opencga.storage.core.manager;

import org.apache.commons.lang3.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.update.FileUpdateParams;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentStorageEngine;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.models.FileInfo;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        QueryResult<AlignmentGlobalStats> stats = alignmentStorageEngine.getDBAdaptor().stats(fileInfo.getPhysicalFilePath(), outDir);

        if (stats != null && stats.getNumResults() == 1) {
            // Store the stats in catalog
            ObjectWriter objectWriter = new ObjectMapper().typedWriter(AlignmentGlobalStats.class);
            ObjectMap globalStats = new ObjectMap(GLOBAL_STATS, objectWriter.writeValueAsString(stats.first()));
            FileUpdateParams fileUpdateParams = new FileUpdateParams().setStats(globalStats);
            catalogManager.getFileManager().update(studyIdStr, fileInfo.getPath(), fileUpdateParams, new QueryOptions(), sessionId);

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

    public QueryResult<ReadAlignment> query(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
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

    public QueryResult<AlignmentGlobalStats> stats(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
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
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileInfo.getFileUid(), includeOptions, sessionId);

            logger.info("Obtaining the stats from catalog...");

            if (fileQueryResult.getNumResults() == 1) {
                Map<String, Object> stats = fileQueryResult.first().getStats();
                Object value = stats.get(GLOBAL_STATS);
                if (value != null && value instanceof String) {
                    ObjectReader reader = new ObjectMapper().reader(AlignmentGlobalStats.class);
                    AlignmentGlobalStats globalStats = reader.readValue((String) value);
                    return new QueryResult<>("Get stats", fileQueryResult.getDbTime(), 1, 1, fileQueryResult.getWarningMsg(),
                            fileQueryResult.getErrorMsg(), Arrays.asList(globalStats));
                }

            }
        }

        // Calculate the stats
        logger.info("Calculating the stats...");
//        Path filePath = getFilePath(fileId, sessionId);
//        Path workspace = getWorkspace(studyId, sessionId);
        return alignmentStorageEngine.getDBAdaptor().stats(fileInfo.getPhysicalFilePath(), studyInfo.getWorkspace(), query, options);

//        return alignmentDBAdaptor.stats((Path) fileInfo.get("filePath"), (Path) fileInfo.get("workspace"), query, options);
    }

    public QueryResult<RegionCoverage> coverage(String studyIdStr, String fileIdStr, Region region, int windowSize, String sessionId)
            throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().coverage(Paths.get(file.getUri()), region, windowSize);
    }

    public QueryResult<RegionCoverage> coverage(String studyIdStr, String fileIdStr, Region region, int minCoverage, int maxCoverage,
                                                String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().coverage(Paths.get(file.getUri()), region, minCoverage, maxCoverage);
    }

    public QueryResult<RegionCoverage> getLowCoverageRegions(String studyIdStr, String fileIdStr, Region region, int minCoverage,
                                                             String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().getLowCoverageRegions(Paths.get(file.getUri()), region, minCoverage);
    }

    public QueryResult<Long> getTotalCounts(String studyIdStr, String fileIdStr, String sessionId) throws Exception {
        File file = extractAlignmentOrCoverageFile(studyIdStr, fileIdStr, sessionId);
        return alignmentStorageEngine.getDBAdaptor().getTotalCounts(Paths.get(file.getUri()));
    }


    File extractAlignmentOrCoverageFile(String studyIdStr, String fileIdStr, String sessionId) throws CatalogException {
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(studyIdStr, fileIdStr,
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.URI.key(),
                        FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.FORMAT.key())), sessionId);
        if (fileQueryResult.getNumResults() == 0) {
            throw new CatalogException("File " + fileIdStr + " not found");
        }

        File.Bioformat bioformat = fileQueryResult.first().getBioformat();
        if (bioformat != File.Bioformat.ALIGNMENT && bioformat != File.Bioformat.COVERAGE) {
            throw new CatalogException("File " + fileQueryResult.first().getName() + " not supported. "
                    + "Expecting an alignment or coverage file.");
        }
        return fileQueryResult.first();
    }

    public QueryResult<Long> count(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
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
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, fileOptions, sessionId);

        if (fileQueryResult.getNumResults() != 1) {
            logger.error("Critical error: File {} not found in catalog.", fileId);
            throw new CatalogException("Critical error: File " + fileId + " not found in catalog");
        }

        Path path = Paths.get(fileQueryResult.first().getUri().getRawPath());
        FileUtils.checkFile(path);

        return path;
    }

    @Deprecated
    private Path getWorkspace(long studyId, String sessionId) throws CatalogException, IOException {
        // Obtain the study uri
        QueryOptions studyOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), studyOptions, sessionId);
        if (studyQueryResult .getNumResults() != 1) {
            logger.error("Critical error: Study {} not found in catalog.", studyId);
            throw new CatalogException("Critical error: Study " + studyId + " not found in catalog");
        }

        Path workspace = Paths.get(studyQueryResult.first().getUri().getRawPath()).resolve(".opencga").resolve("alignments");
        if (!workspace.toFile().exists()) {
            Files.createDirectories(workspace);
        }

        return workspace;
    }

}
