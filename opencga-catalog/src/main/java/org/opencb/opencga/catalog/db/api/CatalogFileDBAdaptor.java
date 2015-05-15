package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.Acl;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogFileDBAdaptor {

    public enum FileFilterOption {
        id(""),
        studyId(""),
        name(""),
        type(""),
        path(""),
        bioformat(""),
        status(""),
        maxSize(""),
        minSize(""),
        startDate(""),
        endDate(""),
        like(""),
        startsWith(""),
        directory(""),
        attributes("Format: <key><operation><value> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        ;

        private FileFilterOption(String description) {
            this.description = description;
        }

        private String description;

        public String getDescription() {
            return description;
        }

    }

    /**
     * File methods
     * ***************************
     */

    // add file to study
    QueryResult<File> createFileToStudy(int studyId, File file, QueryOptions options) throws CatalogDBException;

    QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException;

    int getFileId(int studyId, String path) throws CatalogDBException;

    QueryResult<File> getAllFiles(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException;

    QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException;

    QueryResult setFileStatus(int fileId, File.Status status) throws CatalogDBException;

    QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException;

    QueryResult renameFile(int fileId, String name) throws CatalogDBException;

    int getStudyIdByFileId(int fileId) throws CatalogDBException;

    String getFileOwnerId(int fileId) throws CatalogDBException;

    QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogDBException;

    QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogDBException;

    QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogDBException;

    QueryResult<Dataset> createDataset(int studyId, Dataset dataset, QueryOptions options) throws CatalogDBException;

    QueryResult<Dataset> getDataset(int datasetId, QueryOptions options) throws CatalogDBException;

    int getStudyIdByDatasetId(int datasetId) throws CatalogDBException;

}
