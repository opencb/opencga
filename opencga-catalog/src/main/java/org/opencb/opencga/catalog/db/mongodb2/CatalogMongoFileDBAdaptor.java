package org.opencb.opencga.catalog.db.mongodb2;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api2.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.File;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by pfurio on 08/01/16.
 */
public class CatalogMongoFileDBAdaptor extends AbstractCatalogMongoDBAdaptor implements CatalogFileDBAdaptor {

    public CatalogMongoFileDBAdaptor(Logger logger) {
        super(logger);
    }

    @Override
    public int getFileId(int studyId, String path) throws CatalogDBException {
        return 0;
    }

    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogDBException {
        return 0;
    }

    @Override
    public String getFileOwnerId(int fileId) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<File> createFile(int studyId, File file, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<File> getAllFiles(QueryOptions query, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<File> getAllFilesInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<File> modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult renameFile(int fileId, String name) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<AclEntry> getFileAcl(int fileId, String userId) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Map<String, Map<String, AclEntry>>> getFilesAcl(int studyId, List<String> filePaths, List<String> userIds) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<AclEntry> setFileAcl(int fileId, AclEntry newAcl) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<AclEntry> unsetFileAcl(int fileId, String userId) throws CatalogDBException {
        return null;
    }

    @Override
    public int getStudyIdByDatasetId(int datasetId) throws CatalogDBException {
        return 0;
    }

    @Override
    public QueryResult<Dataset> createDataset(int studyId, Dataset dataset, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Dataset> getDataset(int datasetId, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> count(Query query) {
        return null;
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        return null;
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<File> get(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult<File> update(Query query, ObjectMap parameters) {
        return null;
    }

    @Override
    public QueryResult<Integer> delete(Query query) {
        return null;
    }

    @Override
    public Iterator<File> iterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public Iterator nativeIterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) {

    }
}
