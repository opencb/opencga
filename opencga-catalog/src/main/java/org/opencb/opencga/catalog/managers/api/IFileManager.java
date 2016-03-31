package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;

import java.io.DataInputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IFileManager extends ResourceManager<Long, File> {

    /*-------------*/
    /* URI METHODS */
    /*-------------*/
    URI getStudyUri(long studyId) throws CatalogException;

    URI getFileUri(Study study, File file) throws CatalogException;

    URI getFileUri(File file) throws CatalogException;

    URI getFileUri(long studyId, String relativeFilePath) throws CatalogException;

    @Deprecated
    URI getFileUri(URI studyUri, String relativeFilePath) throws CatalogException;

    /*-------------*/
    /* ID METHODS  */
    /*-------------*/
    String getUserId(long fileId) throws CatalogException;

    Long getStudyId(long fileId) throws CatalogException;

    Long getFileId(String fileId) throws CatalogException;

    boolean isExternal(File file) throws CatalogException;

    /*--------------*/
    /* CRUD METHODS */
    /*--------------*/
    QueryResult<File> create(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path, String ownerId,
                             String creationDate, String description, File.FileStatus status, long diskUsage, long experimentId,
                             List<Long> sampleIds, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                             boolean parents, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> createFolder(long studyId, String path, File.FileStatus status, boolean parents, String description,
                                   QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> getParent(long fileId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> rename(long fileId, String newName, String sessionId) throws CatalogException;

    QueryResult move(long fileId, String newPath, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files, Map<String, Object> attributes,
                                       QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Dataset> readDataset(long dataSetId, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream download(long fileId, int offset, int limit, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream head(long fileId, int lines, QueryOptions options, String sessionId) throws CatalogException;

}
