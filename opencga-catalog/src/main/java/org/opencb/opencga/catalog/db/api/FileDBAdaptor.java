/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.db.AbstractDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface FileDBAdaptor extends AclDBAdaptor<File, FileAclEntry> {

    enum QueryParams implements QueryParam {
        DELETE_DATE("deleteDate", TEXT_ARRAY, ""),
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        TYPE("type", TEXT_ARRAY, ""),
        FORMAT("format", TEXT_ARRAY, ""),
        BIOFORMAT("bioformat", TEXT_ARRAY, ""),
        URI("uri", TEXT_ARRAY, ""),
        PATH("path", TEXT_ARRAY, ""),
        OWNER_ID("ownerId", TEXT_ARRAY, ""),
        CREATION_DATE("creationDate", TEXT_ARRAY, ""),
        MODIFICATION_DATE("modificationDate", TEXT_ARRAY, ""),
        DESCRIPTION("description", TEXT_ARRAY, ""),
        EXTERNAL("external", BOOLEAN, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        @Deprecated
        FILE_STATUS("status.name", TEXT, ""),
        RELATED_FILES("relatedFiles", TEXT_ARRAY, ""),
        RELATED_FILES_RELATION("relatedFiles.relation", TEXT, ""),
        DISK_USAGE("diskUsage", INTEGER_ARRAY, ""),
        EXPERIMENT_ID("experimentId", INTEGER_ARRAY, ""),
        SAMPLE_IDS("sampleIds", INTEGER_ARRAY, ""),

        JOB_ID("jobId", INTEGER_ARRAY, ""),
        ACL("acl", TEXT_ARRAY, ""),
        ACL_MEMBER("acl.member", TEXT_ARRAY, ""),
        ACL_PERMISSIONS("acl.permissions", TEXT_ARRAY, ""),
//        ACL_USER_ID("acls.userId", TEXT_ARRAY, ""),
//        ACL_READ("acls.read", BOOLEAN, ""),
//        ACL_WRITE("acls.write", BOOLEAN, ""),
//        ACL_EXECUTE("acls.execute", BOOLEAN, ""),
//        ACL_DELETE("acls.delete", BOOLEAN, ""),

        INDEX("index", TEXT_ARRAY, ""),
        INDEX_USER_ID("index.userId", TEXT, ""),
        INDEX_CREATION_DATE("index.creationDate", TEXT, ""),
        INDEX_STATUS_NAME("index.status.name", TEXT, ""),
        INDEX_STATUS_MESSAGE("index.status.message", TEXT, ""),
        INDEX_JOB_ID("index.jobId", TEXT, ""),
        INDEX_TRANSFORMED_FILE("index.transformedFile", TEXT_ARRAY, ""),

        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATS("stats", TEXT, ""),
        NSTATS("nstats", DECIMAL, ""),

        DIRECTORY("directory", TEXT, ""),
        STUDY_ID("studyId", INTEGER_ARRAY, "");

        // Fixme: Index attributes
        private static Map<String, QueryParams> map = new HashMap<>();
        static {
            for (QueryParams param : QueryParams.values()) {
                map.put(param.key(), param);
            }
        }

        // TOCHECK: Pedro. Add annotation support?

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    default boolean exists(long fileId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), fileId)).first() > 0;
    }

    default void checkId(long fileId) throws CatalogDBException {
        if (fileId < 0) {
            throw CatalogDBException.newInstance("File id '{}' is not valid: ", fileId);
        }

        long count = count(new Query(QueryParams.ID.key(), fileId)).first();
        if (count <= 0) {
            throw CatalogDBException.newInstance("File id '{}' does not exist", fileId);
        } else if (count > 1) {
            throw CatalogDBException.newInstance("'{}' documents found with the File id '{}'", count, fileId);
        }
    }

    long getId(long studyId, String path) throws CatalogDBException;

    long getStudyIdByFileId(long fileId) throws CatalogDBException;

    List<Long> getStudyIdsByFileIds(String fileIds) throws CatalogDBException;

    /***
     * Inserts the passed file in the database.
     *
     * @param file The file to be inserted in the database.
     * @param studyId Id of the study where the file belongs to.
     * @param options Options to filter the output that will be returned after the insertion of the file.
     * @return A QueryResult object containing information regarding the inserted file.
     * @throws CatalogDBException when the file could not be inserted due to different reasons.
     */
    QueryResult<File> insert(File file, long studyId, QueryOptions options) throws CatalogDBException;

    /***
     * Retrieves the file from the database containing the fileId given.
     *
     * @param fileId File ID of the required file.
     * @param options Options to filter the output.
     * @return A QueryResult object containing the required file.
     * @throws CatalogDBException when the file could not be found in the database.
     */
    QueryResult<File> get(long fileId, QueryOptions options) throws CatalogDBException;

    /***
     * Retrieves all the files belonging to the given study.
     *
     * @param studyId Study id where the files will be extracted from.
     * @param options Options to filter the output.
     * @return A QueryResult object containing all the files belonging to the study.
     * @throws CatalogDBException when the study does not exist.
     */
    QueryResult<File> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException;

    /***
     * Retrieves all the files present in the folder.
     *
     * @param studyId Study id where the files will be extracted from.
     * @param path Directory where the files will be extracted from.
     * @param options Options to filter the file output.
     * @return A QueryResult object containing the files present in the folder of the given study.
     * @throws CatalogDBException when the study or the path does not exist.
     */
    QueryResult<File> getAllFilesInFolder(long studyId, String path, QueryOptions options) throws CatalogDBException;

    /***
     * Renames the file.
     *
     * @param fileId Id of the file to be renamed.
     * @param filePath New file or directory name (containing the full path).
     * @param fileUri New file uri (containing the full path).
     * @param options Options to filter the file output.
     * @return A QueryResult object containing the file that have been renamed.
     * @throws CatalogDBException when the filePath already exists.
     */
    QueryResult<File> rename(long fileId, String filePath, String fileUri, QueryOptions options) throws CatalogDBException;

    /**
     * Extract the sampleIds given from the files that matching the query.
     *
     * @param query query.
     * @param sampleIds sample ids.
     * @return A queryResult object containing the number of files matching the query.
     * @throws CatalogDBException CatalogDBException.
     */
    QueryResult<Long> extractSampleFromFiles(Query query, List<Long> sampleIds) throws CatalogDBException;

    /**
     * Delete file.
     *
     * @param fileId fileId.
     * @param update Map containing the parameters that will be updated after deletion. SKIP_CHECK might also be found in the map
     *               to avoid checking whether the fileId is being used and delete it anyway.
     * @param queryOptions Query Options.
     * @return the deleted file.
     * @throws CatalogDBException when the file could not be deleted.
     */
    QueryResult<File> delete(long fileId, ObjectMap update, QueryOptions queryOptions) throws CatalogDBException;

    /*
     * ACL methods
     * ***************************
     */

    /***
     * Retrieves the AclEntries of the files and users given.
     *
     * @param studyId The id of the study where the files belong to.
     * @param filePaths The file paths of the files to extract the permissions from.
     * @param userIds The list of user ids from whom the permissions will be checked.
     * @return A map of files containing a map of user - AclEntries.
     * @throws CatalogDBException when the study does not exist.
     */
    QueryResult<Map<String, Map<String, FileAclEntry>>> getAcls(long studyId, List<String> filePaths, List<String> userIds)
            throws CatalogDBException;

    default QueryResult<FileAclEntry> getAcl(long fileId, String member) throws CatalogDBException {
        return getAcl(fileId, Arrays.asList(member));
    }

    @Deprecated
    enum FileFilterOption implements AbstractDBAdaptor.FilterOption {
        studyId(Type.NUMERICAL, ""),
        directory(Type.TEXT, ""),

        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        type(Type.TEXT, ""),
        format(Type.TEXT, ""),
        bioformat(Type.TEXT, ""),
        uri(Type.TEXT, ""),
        path(Type.TEXT, ""),
        ownerId(Type.TEXT, ""),
        creationDate(Type.TEXT, ""),
        modificationDate(Type.TEXT, ""),
        description(Type.TEXT, ""),
        status(Type.TEXT, ""),
        diskUsage(Type.NUMERICAL, ""),
        experimentId(Type.NUMERICAL, ""),
        sampleIds(Type.NUMERICAL, ""),
        jobId(Type.NUMERICAL, ""),
        acl(Type.TEXT, ""),
        bacl("acl", Type.BOOLEAN, ""),
        index("index", Type.TEXT, ""),

        stats(Type.TEXT, ""),
        nstats("stats", Type.NUMERICAL, ""),

        attributes(Type.TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        nattributes("attributes", Type.NUMERICAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        battributes("attributes", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),

        @Deprecated maxSize(Type.NUMERICAL, ""),
        @Deprecated minSize(Type.NUMERICAL, ""),
        @Deprecated startDate(Type.TEXT, ""),
        @Deprecated endDate(Type.TEXT, ""),
        @Deprecated like(Type.TEXT, ""),
        @Deprecated startsWith(Type.TEXT, "");

        private final String _key;
        private final String _description;
        private final Type _type;

        FileFilterOption(Type type, String description) {
            this._key = name();
            this._description = description;
            this._type = type;
        }

        FileFilterOption(String key, Type type, String description) {
            this._key = key;
            this._description = description;
            this._type = type;
        }

        @Override
        public String getDescription() {
            return _description;
        }

        @Override
        public Type getType() {
            return _type;
        }

        @Override
        public String getKey() {
            return _key;
        }
    }

    /**
     * Remove all the Acls defined for the member in the resource.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member member from whom the Acls will be removed.
     * @throws CatalogDBException if any problem occurs during the removal.
     */
    void removeAclsFromStudy(long studyId, String member) throws CatalogDBException;

}
