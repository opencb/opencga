package org.opencb.opencga.storage.core.alignment;

import ga4gh.Reads;
import org.apache.commons.lang3.StringUtils;
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
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentStorageETL;
import org.opencb.opencga.storage.core.cache.CacheManager;
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

/**
 * Created by pfurio on 31/10/16.
 */
public class AlignmentStorageManager extends StorageManager<AlignmentDBAdaptor> {

    private StorageETL alignmentETL;
    private AlignmentDBAdaptor alignmentDBAdaptor;

    private CacheManager cacheManager;

    public AlignmentStorageManager() {
    }

    public AlignmentStorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, storageConfiguration);
        this.logger = LoggerFactory.getLogger(AlignmentStorageManager.class);

        this.cacheManager = new CacheManager(storageConfiguration);

        // Fixme: Initialize alignmentETL and alignmentDBAdaptor using reflection
        this.alignmentDBAdaptor = new LocalAlignmentDBAdaptor();
        this.alignmentETL = new LocalAlignmentStorageETL();
    }

    public void index(String studyIdStr, String fileIdStr, ObjectMap options, String sessionId)
            throws CatalogException, IOException, StorageManagerException {
        ObjectMap fileInfo = checkAndGetInfoFile(studyIdStr, fileIdStr, sessionId);
        List<URI> fileUris = Arrays.asList(((Path) fileInfo.get("filePath")).toUri());
        Path workspace = (Path) fileInfo.get("workspace");
        super.index(fileUris, workspace.toUri(), false, options.getBoolean("transform"), options.getBoolean("load"));
    }

    public QueryResult<ReadAlignment> query(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        ObjectMap fileInfo = checkAndGetInfoFile(studyIdStr, fileIdStr, sessionId);
        return alignmentDBAdaptor.get((Path) fileInfo.get("filePath"), query, options);
    }

    public AlignmentIterator<Reads.ReadAlignment> iterator(String studyId, String fileId, Query query, QueryOptions options,
                                                           String sessionId) throws CatalogException, IOException {
        return iterator(studyId, fileId, query, options, sessionId, Reads.ReadAlignment.class);
    }

    public <T> AlignmentIterator<T> iterator(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId,
                                             Class<T> clazz) throws CatalogException, IOException {
        ObjectMap fileInfo = checkAndGetInfoFile(studyIdStr, fileIdStr, sessionId);
        return alignmentDBAdaptor.iterator((Path) fileInfo.get("filePath"), query, options, clazz);
    }

    public QueryResult<AlignmentGlobalStats> stats(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws Exception {
        ObjectMap fileInfo = checkAndGetInfoFile(studyIdStr, fileIdStr, sessionId);
        return alignmentDBAdaptor.stats((Path) fileInfo.get("filePath"), (Path) fileInfo.get("workspace"), query, options);
    }

    public QueryResult<RegionCoverage> coverage(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws Exception {
        ObjectMap fileInfo = checkAndGetInfoFile(studyIdStr, fileIdStr, sessionId);

        return alignmentDBAdaptor.coverage((Path) fileInfo.get("filePath"), (Path) fileInfo.get("workspace"), query, options);
    }


    public QueryResult<Long> count(String studyIdStr, String fileIdStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        ObjectMap fileInfo = checkAndGetInfoFile(studyIdStr, fileIdStr, sessionId);
        return alignmentDBAdaptor.count((Path) fileInfo.get("filePath"), query, options);
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        return null;
    }

    @Override
    public void testConnection() throws StorageManagerException {
    }

    @Override
    public StorageETL newStorageETL(boolean connected) throws StorageManagerException {
        return alignmentETL;
    }

    /**
     * Validate all the fields and return a map containing.
     *   fileName
     *   filePath
     *   studyId
     *   workspace
     *
     * @param studyIdStr study string.
     * @param fileIdStr file string.
     * @param sessionId session id.
     * @return a map with the described parameters filled.
     * @throws CatalogException catalogException.
     * @throws IOException IOException.
     */
    private ObjectMap checkAndGetInfoFile(@Nullable String studyIdStr, String fileIdStr, String sessionId)
            throws CatalogException, IOException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = 0;
        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyId = catalogManager.getStudyManager().getId(userId, studyIdStr);
        }

        ObjectMap ret = new ObjectMap();

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

        QueryOptions fileOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.NAME.key()));
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, fileOptions, sessionId);

        if (fileQueryResult.getNumResults() != 1) {
            logger.error("Critical error: File {} not found in catalog.", fileId);
            throw new CatalogException("Critical error: File " + fileIdStr + " not found in catalog");
        }

        Path path = Paths.get(fileQueryResult.first().getUri().getRawPath());
        FileUtils.checkFile(path);

        // Obtain the study uri
        QueryOptions studyOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyId, studyOptions, sessionId);
        if (studyQueryResult .getNumResults() != 1) {
            logger.error("Critical error: Study {} not found in catalog.", studyId);
            throw new CatalogException("Critical error: Study " + studyIdStr + " not found in catalog");
        }

        Path workspace = Paths.get(studyQueryResult.first().getUri().getRawPath()).resolve(".opencga").resolve("alignments");
        if (!workspace.toFile().exists()) {
            Files.createDirectories(workspace);
        }

        ret.put("fileName", fileQueryResult.first().getName());
        ret.put("filePath", path);
        ret.put("studyId", studyId);
        ret.put("workspace", workspace);

        return ret;
    }

}
