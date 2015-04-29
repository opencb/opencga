package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Dataset;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface IFileManager extends ResourceManager<Integer, File> {
    URI getStudyUri(int studyId)
            throws CatalogException;

    URI getFileUri(int studyId, String relativeFilePath)
            throws CatalogException;

    URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogIOManagerException, IOException;

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

    QueryResult<File> renameFile(int fileId, String newName, String sessionId)
            throws CatalogException;

    QueryResult<Dataset> createDataset(int studyId, String name, String description, List<Integer> files,
                                       Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<Dataset> getDataset(int dataSetId, QueryOptions options, String sessionId)
            throws CatalogException;
}
