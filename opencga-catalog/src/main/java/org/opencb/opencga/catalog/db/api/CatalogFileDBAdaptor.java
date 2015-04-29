package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.Dataset;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * Created by hpccoll1 on 28/04/15.
 */
public interface CatalogFileDBAdaptor {

    /**
     * File methods
     * ***************************
     */

    // add file to study
    public abstract QueryResult<File> createFileToStudy(int studyId, File file, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException;

    public abstract int getFileId(int studyId, String path) throws CatalogDBException;

    public abstract QueryResult<File> getAllFiles(int studyId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult setFileStatus(int fileId, File.Status status) throws CatalogDBException;

    public abstract QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException;

    public abstract QueryResult renameFile(int fileId, String name) throws CatalogDBException;

    public abstract int getStudyIdByFileId(int fileId) throws CatalogDBException;

    public abstract String getFileOwnerId(int fileId) throws CatalogDBException;

    public abstract QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogDBException;

    public abstract QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogDBException;

    public abstract QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Dataset> createDataset(int studyId, Dataset dataset, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Dataset> getDataset(int datasetId, QueryOptions options) throws CatalogDBException;

    public abstract int getStudyIdByDatasetId(int datasetId) throws CatalogDBException;

}
