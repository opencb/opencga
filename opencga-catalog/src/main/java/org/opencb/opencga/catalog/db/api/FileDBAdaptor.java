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

package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections.map.LinkedMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.VariableSet;
import org.opencb.opencga.core.results.OpenCGAResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface FileDBAdaptor extends AnnotationSetDBAdaptor<File> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER_ARRAY, ""),
        UUID("uuid", TEXT, ""),
        NAME("name", TEXT_ARRAY, ""),
        TYPE("type", TEXT_ARRAY, ""),
        CHECKSUM("checksum", TEXT, ""),
        FORMAT("format", TEXT_ARRAY, ""),
        BIOFORMAT("bioformat", TEXT_ARRAY, ""),
        URI("uri", TEXT_ARRAY, ""),
        PATH("path", TEXT_ARRAY, ""),
        OWNER_ID("ownerId", TEXT_ARRAY, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", TEXT_ARRAY, ""),
        DESCRIPTION("description", TEXT_ARRAY, ""),
        EXTERNAL("external", BOOLEAN, ""),
        RELEASE("release", INTEGER, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        RELATED_FILES("relatedFiles", TEXT_ARRAY, ""),
        RELATED_FILES_RELATION("relatedFiles.relation", TEXT, ""),
        SIZE("size", INTEGER_ARRAY, ""),
        EXPERIMENT_UID("experiment.uid", INTEGER_ARRAY, ""),
        SOFTWARE("software", TEXT_ARRAY, ""),
        SOFTWARE_NAME("software.name", TEXT, ""),
        SOFTWARE_VERSION("software.version", TEXT, ""),
        SOFTWARE_COMMIT("software.commit", TEXT, ""),
        SAMPLES("samples", TEXT_ARRAY, ""),
        SAMPLE_UIDS("samples.uid", INTEGER_ARRAY, ""),
        TAGS("tags", TEXT_ARRAY, ""),

        JOB_UID("job.uid", INTEGER_ARRAY, ""),

        DELETED("deleted", BOOLEAN, ""),

        INDEX("index", TEXT_ARRAY, ""),
        INDEX_USER_ID("index.userId", TEXT, ""),
        INDEX_CREATION_DATE("index.creationDate", TEXT, ""),
        INDEX_STATUS_NAME("index.status.name", TEXT, ""),
        INDEX_STATUS_MESSAGE("index.status.message", TEXT, ""),
        INDEX_JOB_ID("index.jobId", TEXT, ""),
        INDEX_TRANSFORMED_FILE("index.transformedFile", TEXT_ARRAY, ""),
        INDEX_RELEASE("index.release", INTEGER, ""),

        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATS("stats", TEXT, ""),
        NSTATS("nstats", DECIMAL, ""),

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        ANNOTATION(Constants.ANNOTATION, TEXT_ARRAY, ""),

        DIRECTORY("directory", TEXT, ""),
        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""); // Alias to studyId in the database. Only for the webservices.

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

    enum UpdateParams {
        NAME(QueryParams.NAME.key()),
        FORMAT(QueryParams.FORMAT.key()),
        BIOFORMAT(QueryParams.BIOFORMAT.key()),
        CHECKSUM(QueryParams.CHECKSUM.key()),
        STATS(QueryParams.STATS.key()),
        DESCRIPTION(QueryParams.DESCRIPTION.key()),
        JOB_UID(QueryParams.JOB_UID.key()),
        SOFTWARE(QueryParams.SOFTWARE.key()),
        STATUS_NAME(QueryParams.STATUS_NAME.key()),
        SAMPLES(QueryParams.SAMPLES.key()),
        URI(QueryParams.URI.key()),
        SIZE(QueryParams.SIZE.key()),
        ATTRIBUTES(QueryParams.ATTRIBUTES.key()),
        ANNOTATION_SETS(QueryParams.ANNOTATION_SETS.key()),
        ANNOTATIONS(AnnotationSetManager.ANNOTATIONS);

        private static Map<String, UpdateParams> map;
        static {
            map = new LinkedMap();
            for (UpdateParams params : UpdateParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;

        UpdateParams(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }

        public static UpdateParams getParam(String key) {
            return map.get(key);
        }
    }

    default boolean exists(long fileId) throws CatalogDBException {
        return count(new Query(QueryParams.UID.key(), fileId)).getNumMatches() > 0;
    }

    default void checkId(long fileId) throws CatalogDBException {
        if (fileId < 0) {
            throw CatalogDBException.newInstance("File id '{}' is not valid: ", fileId);
        }

        long count = count(new Query(QueryParams.UID.key(), fileId)).getNumMatches();
        if (count <= 0) {
            throw CatalogDBException.newInstance("File id '{}' does not exist", fileId);
        } else if (count > 1) {
            throw CatalogDBException.newInstance("'{}' documents found with the File id '{}'", count, fileId);
        }
    }

    long getId(long studyId, String path) throws CatalogDBException;

    long getStudyIdByFileId(long fileId) throws CatalogDBException;

    OpenCGAResult nativeInsert(Map<String, Object> file, String userId) throws CatalogDBException;

    /***
     * Inserts the passed file in the database.
     *
     * @param studyId Id of the study where the file belongs to.
     * @param file The file to be inserted in the database.
     * @param variableSetList Variable set list.
     * @param options Options to filter the output that will be returned after the insertion of the file.
     * @return A OpenCGAResult object containing the time spent.
     * @throws CatalogDBException when the file could not be inserted due to different reasons.
     */
    OpenCGAResult insert(long studyId, File file, List<VariableSet> variableSetList, QueryOptions options) throws CatalogDBException;

    /***
     * Retrieves the file from the database containing the fileId given.
     *
     * @param fileId File ID of the required file.
     * @param options Options to filter the output.
     * @return A OpenCGAResult object containing the required file.
     * @throws CatalogDBException when the file could not be found in the database.
     */
    OpenCGAResult<File> get(long fileId, QueryOptions options) throws CatalogDBException;

    /***
     * Retrieves all the files belonging to the given study.
     *
     * @param studyId Study id where the files will be extracted from.
     * @param options Options to filter the output.
     * @return A OpenCGAResult object containing all the files belonging to the study.
     * @throws CatalogDBException when the study does not exist.
     */
    OpenCGAResult<File> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException;

    /***
     * Retrieves all the files present in the folder.
     *
     * @param studyId Study id where the files will be extracted from.
     * @param path Directory where the files will be extracted from.
     * @param options Options to filter the file output.
     * @return A OpenCGAResult object containing the files present in the folder of the given study.
     * @throws CatalogDBException when the study or the path does not exist.
     */
    OpenCGAResult<File> getAllFilesInFolder(long studyId, String path, QueryOptions options) throws CatalogDBException;

    /***
     * Renames the file.
     *
     * @param fileId Id of the file to be renamed.
     * @param filePath New file or directory name (containing the full path).
     * @param fileUri New file uri (containing the full path).
     * @param options Options to filter the file output.
     * @return A OpenCGAResult object.
     * @throws CatalogDBException when the filePath already exists.
     */
    OpenCGAResult rename(long fileId, String filePath, String fileUri, QueryOptions options) throws CatalogDBException;

    /**
     * Add the samples to the array of samples in the file entry.
     *
     * @param fileId file id corresponding to the file being updated.
     * @param samples List of samples to be added to the array.
     * @return OpenCGAResult object.
     * @throws CatalogDBException CatalogDBException.
     */
    OpenCGAResult addSamplesToFile(long fileId, List<Sample> samples) throws CatalogDBException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

    /**
     * Delete file.
     *
     * @param file File to be deleted.
     * @param status Deletion status we want to set.
     * @return a OpenCGAResult object.
     * @throws CatalogDBException when the status is not a valid delete status or if there was any problem during the deletion.
     */
    OpenCGAResult delete(File file, String status) throws CatalogDBException;

    /**
     * Delete file.
     *
     * @param query Delete all the files matching the query.
     * @param status Deletion status we want to set.
     * @return a OpenCGAResult object.
     * @throws CatalogDBException when the status is not a valid delete status or if there was any problem during the deletion.
     */
    OpenCGAResult delete(Query query, String status) throws CatalogDBException;

}
