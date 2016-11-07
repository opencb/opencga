package org.opencb.opencga.storage.core.local;

import ga4gh.Reads;
import org.apache.commons.lang3.StringUtils;
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
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentStorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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

    private org.opencb.opencga.storage.core.alignment.AlignmentStorageManager storageManager;

    private static final String GLOBAL_STATS = "globalStats";

    public AlignmentStorageManager() {
    }

    public AlignmentStorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, storageConfiguration);
        this.logger = LoggerFactory.getLogger(AlignmentStorageManager.class);

        // TODO: Create this storageManager by reflection
        this.storageManager = new LocalAlignmentStorageManager();

    }

    public void index(String studyIdStr, String fileIdStr, ObjectMap options, String sessionId) throws Exception {
        options = ParamUtils.defaultObject(options, ObjectMap::new);

        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
        long studyId = fileAndStudyId.getLong("studyId");
        long fileId = fileAndStudyId.getLong("fileId");
        Path filePath = getFilePath(fileId, sessionId);
        Path workspace = getWorkspace(studyId, sessionId);

        List<URI> fileUris = Arrays.asList(filePath.toUri());

        // TODO: Check if index is already created and link bai file
        storageManager.index(fileUris, workspace.toUri(), false, options.getBoolean("transform"), options.getBoolean("load"));

        // Create the stats and store them in catalog
        logger.info("Calculating the stats...");
        QueryResult<AlignmentGlobalStats> stats = storageManager.getDBAdaptor().stats(filePath, workspace);

        if (stats != null && stats.getNumResults() == 1) {
            // Store the stats in catalog
            ObjectWriter objectWriter = new ObjectMapper().typedWriter(AlignmentGlobalStats.class);
            ObjectMap globalStats = new ObjectMap(GLOBAL_STATS, objectWriter.writeValueAsString(stats.first()));
            ObjectMap alignmentStats = new ObjectMap(FileDBAdaptor.QueryParams.STATS.key(), globalStats);
            catalogManager.getFileManager().update(fileId, alignmentStats, new QueryOptions(), sessionId);

            // Remove the stats file
            Path statsFile = workspace.resolve(filePath.toFile().getName() + ".stats");
            if (statsFile.toFile().exists()) {
                Files.delete(statsFile);
            }
        }
        // Create the coverage
        logger.info("Calculating the coverage...");
        storageManager.getDBAdaptor().coverage(filePath, workspace);
    }

    public QueryResult<ReadAlignment> query(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageManagerException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
        long fileId = fileAndStudyId.getLong("fileId");
        Path filePath = getFilePath(fileId, sessionId);

        return storageManager.getDBAdaptor().get(filePath, query, options);
    }

    public AlignmentIterator<Reads.ReadAlignment> iterator(String studyId, String fileId, Query query, QueryOptions options,
                                                           String sessionId) throws CatalogException, IOException, StorageManagerException {
        return iterator(studyId, fileId, query, options, sessionId, Reads.ReadAlignment.class);
    }

    public <T> AlignmentIterator<T> iterator(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId,
                                             Class<T> clazz) throws CatalogException, IOException, StorageManagerException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
        long fileId = fileAndStudyId.getLong("fileId");
        Path filePath = getFilePath(fileId, sessionId);

        return storageManager.getDBAdaptor().iterator(filePath, query, options, clazz);
//        return alignmentDBAdaptor.iterator((Path) fileInfo.get("filePath"), query, options, clazz);
    }

    public QueryResult<AlignmentGlobalStats> stats(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws Exception {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
        long studyId = fileAndStudyId.getLong("studyId");
        long fileId = fileAndStudyId.getLong("fileId");

        if (query.isEmpty() && options.isEmpty()) {
            QueryOptions includeOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.STATS.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, includeOptions, sessionId);

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
        Path filePath = getFilePath(fileId, sessionId);
        Path workspace = getWorkspace(studyId, sessionId);
        return storageManager.getDBAdaptor().stats(filePath, workspace, query, options);

//        return alignmentDBAdaptor.stats((Path) fileInfo.get("filePath"), (Path) fileInfo.get("workspace"), query, options);
    }

    public QueryResult<RegionCoverage> coverage(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws Exception {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
        long studyId = fileAndStudyId.getLong("studyId");
        long fileId = fileAndStudyId.getLong("fileId");
        Path filePath = getFilePath(fileId, sessionId);
        Path workspace = getWorkspace(studyId, sessionId);

        return storageManager.getDBAdaptor().coverage(filePath, workspace, query, options);
//        return alignmentDBAdaptor.coverage((Path) fileInfo.get("filePath"), (Path) fileInfo.get("workspace"), query, options);
    }


    public QueryResult<Long> count(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageManagerException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap fileAndStudyId = getFileAndStudyId(studyIdStr, fileIdStr, sessionId);
        long fileId = fileAndStudyId.getLong("fileId");
        Path filePath = getFilePath(fileId, sessionId);

        return storageManager.getDBAdaptor().count(filePath, query, options);
//        return alignmentDBAdaptor.count((Path) fileInfo.get("filePath"), query, options);
    }

    @Override
    public void testConnection() throws StorageManagerException {
    }

    @Override
    public StorageETL newStorageETL(boolean connected) throws StorageManagerException {
        return null;
    }

    /**
     * Given the file and study string, retrieve the corresponding long ids.
     *
     * @param studyIdStr study string.
     * @param fileIdStr file string.
     * @param sessionId session id.
     * @return an objectMap containing the keys "fileId" and "studyId"
     * @throws CatalogException catalog exception.
     */
    private ObjectMap getFileAndStudyId(@Nullable String studyIdStr, String fileIdStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = 0;
        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyId = catalogManager.getStudyManager().getId(userId, studyIdStr);
        }

        long fileId;
        if (studyId > 0) {
            fileId = catalogManager.getFileManager().getId(userId, studyId, fileIdStr);
            if (fileId <= 0) {
                throw new CatalogException("The id of file " + fileIdStr + " could not be found under study " + studyIdStr);
            }
        } else {
            fileId = catalogManager.getFileManager().getId(userId, fileIdStr);
            if (fileId <= 0) {
                throw new CatalogException("The id of file " + fileIdStr + " could not be found");
            }
            studyId = catalogManager.getFileManager().getStudyId(fileId);
        }

        return new ObjectMap("fileId", fileId).append("studyId", studyId);
    }

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
