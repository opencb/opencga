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

import ga4gh.Reads;
import org.apache.commons.lang3.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.utils.ParamUtils;
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

    private AlignmentStorageEngine alignmentStorageManager;

    private static final String GLOBAL_STATS = "globalStats";

    public AlignmentStorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);

        // TODO: Create this alignmentStorageManager by reflection
        this.alignmentStorageManager = new LocalAlignmentStorageEngine();
    }


    public void index(String studyIdStr, String fileIdStr, ObjectMap options, String sessionId) throws Exception {
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

        List<URI> fileUris = Arrays.asList(fileInfo.getPath().toUri());

        // TODO: Check if index is already created and link bai file
        logger.info("Creating index...");
        watch.start();
        alignmentStorageManager
                .index(fileUris, studyInfo.getWorkspace().toUri(), false, options.getBoolean("transform"), options.getBoolean("load"));
        watch.stop();
        logger.info("Indexing took {} seconds", watch.getTime() / 1000.0);

        // Create the stats and store them in catalog
        logger.info("Calculating the stats...");
        watch.reset();
        watch.start();
        QueryResult<AlignmentGlobalStats> stats = alignmentStorageManager.getDBAdaptor()
                .stats(fileInfo.getPath(), studyInfo.getWorkspace());

        if (stats != null && stats.getNumResults() == 1) {
            // Store the stats in catalog
            ObjectWriter objectWriter = new ObjectMapper().typedWriter(AlignmentGlobalStats.class);
            ObjectMap globalStats = new ObjectMap(GLOBAL_STATS, objectWriter.writeValueAsString(stats.first()));
            ObjectMap alignmentStats = new ObjectMap(FileDBAdaptor.QueryParams.STATS.key(), globalStats);
            catalogManager.getFileManager().update(fileInfo.getFileId(), alignmentStats, new QueryOptions(), sessionId);

            // Remove the stats file
            Path statsFile = studyInfo.getWorkspace().resolve(fileInfo.getPath().toFile().getName() + ".stats");
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
        alignmentStorageManager.getDBAdaptor().coverage(fileInfo.getPath(), studyInfo.getWorkspace());
        watch.stop();
        logger.info("Coverage calculation took {} seconds", watch.getTime() / 1000.0);
    }

    public QueryResult<ReadAlignment> query(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long fileId = fileAndStudyId.getLong("fileId");
//        Path filePath = getFilePath(fileId, sessionId);

        return alignmentStorageManager.getDBAdaptor().get(studyInfo.getFileInfo().getPath(), query, options);
    }

    public AlignmentIterator<Reads.ReadAlignment> iterator(String studyId, String fileId, Query query, QueryOptions options,
                                                           String sessionId) throws CatalogException, IOException, StorageEngineException {
        return iterator(studyId, fileId, query, options, sessionId, Reads.ReadAlignment.class);
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

        return alignmentStorageManager.getDBAdaptor().iterator(studyInfo.getFileInfo().getPath(), query, options, clazz);
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
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileInfo.getFileId(), includeOptions, sessionId);

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
        return alignmentStorageManager.getDBAdaptor().stats(fileInfo.getPath(), studyInfo.getWorkspace(), query, options);

//        return alignmentDBAdaptor.stats((Path) fileInfo.get("filePath"), (Path) fileInfo.get("workspace"), query, options);
    }

    public QueryResult<RegionCoverage> coverage(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws Exception {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
        checkAlignmentBioformat(studyInfo.getFileInfos());
        FileInfo fileInfo = studyInfo.getFileInfo();
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long studyId = fileAndStudyId.getLong("studyId");
//        long fileId = fileAndStudyId.getLong("fileId");
//        Path filePath = getFilePath(fileId, sessionId);
//        Path workspace = getWorkspace(studyId, sessionId);

        return alignmentStorageManager.getDBAdaptor().coverage(fileInfo.getPath(), studyInfo.getWorkspace(), query, options);
//        return alignmentDBAdaptor.coverage((Path) fileInfo.get("filePath"), (Path) fileInfo.get("workspace"), query, options);
    }


    public QueryResult<Long> count(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        StudyInfo studyInfo = getStudyInfo(studyIdStr, fileIdStr, sessionId);
//        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
//        long fileId = fileAndStudyId.getLong("fileId");
//        Path filePath = getFilePath(fileId, sessionId);

        return alignmentStorageManager.getDBAdaptor().count(studyInfo.getFileInfo().getPath(), query, options);
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
        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyId, studyOptions, sessionId);
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
