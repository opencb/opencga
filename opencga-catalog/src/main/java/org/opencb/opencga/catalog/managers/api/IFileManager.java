package org.opencb.opencga.catalog.managers.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
* @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
*/
public interface IFileManager extends ResourceManager<Integer, File> {
    URI getStudyUri(int studyId)
            throws CatalogException;

    URI getFileUri(int studyId, String relativeFilePath)
            throws CatalogException;

    URI getFileUri(File file) throws CatalogException;

    URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogIOException, IOException;

    public String  getUserId(int fileId) throws CatalogException;

    public Integer getProjectId(int fileId) throws CatalogException;

    public Integer getStudyId(int fileId) throws CatalogException;

    public Integer getFileId(String fileId) throws CatalogException;

    QueryResult<File> createFolder(int studyId, String path, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException;

    public QueryResult<File> create(int studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                    String ownerId, String creationDate, String description, File.Status status,
                                    long diskUsage, int experimentId, List<Integer> sampleIds, int jobId,
                                    Map<String, Object> stats, Map<String, Object> attributes,
                                    boolean parents, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<File> getParent(int fileId, QueryOptions options, String sessionId)
    throws CatalogException;

    QueryResult<File> rename(int fileId, String newName, String sessionId)
            throws CatalogException;

    QueryResult move(int fileId, String newPath, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<Dataset> createDataset(int studyId, String name, String description, List<Integer> files,
                                       Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<Dataset> readDataset(int dataSetId, QueryOptions options, String sessionId)
            throws CatalogException;

    DataInputStream grep(int fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream download(int fileId, int offset, int limit, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream head(int fileId, int lines, QueryOptions options, String sessionId) throws CatalogException;
}
