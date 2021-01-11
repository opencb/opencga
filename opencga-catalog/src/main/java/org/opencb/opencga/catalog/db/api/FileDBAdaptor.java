/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;

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
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", TEXT_ARRAY, ""),
        DESCRIPTION("description", TEXT_ARRAY, ""),
        EXTERNAL("external", BOOLEAN, ""),
        RELEASE("release", INTEGER, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        STATUS_DESCRIPTION("status.description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DESCRIPTION("internal.status.description", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        RELATED_FILES("relatedFiles", TEXT_ARRAY, ""),
        RELATED_FILES_RELATION("relatedFiles.relation", TEXT, ""),
        SIZE("size", INTEGER_ARRAY, ""),
        EXPERIMENT("experiment", OBJECT, ""),
        SOFTWARE("software", TEXT_ARRAY, ""),
        SOFTWARE_NAME("software.name", TEXT, ""),
        SOFTWARE_VERSION("software.version", TEXT, ""),
        SOFTWARE_COMMIT("software.commit", TEXT, ""),
        SAMPLE_IDS("sampleIds", TEXT_ARRAY, ""),
        TAGS("tags", TEXT_ARRAY, ""),

        JOB_ID("jobId", TEXT, ""),

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

        INTERNAL("internal", OBJECT, ""),
        INTERNAL_INDEX("internal.index", TEXT_ARRAY, ""),
        INTERNAL_INDEX_USER_ID("internal.index.userId", TEXT, ""),
        INTERNAL_INDEX_CREATION_DATE("internal.index.creationDate", TEXT, ""),
        INTERNAL_INDEX_STATUS_NAME("internal.index.status.name", TEXT, ""),
        INTERNAL_INDEX_STATUS_MESSAGE("internal.index.status.message", TEXT, ""),
        INTERNAL_INDEX_JOB_ID("internal.index.jobId", TEXT, ""),
        INTERNAL_INDEX_TRANSFORMED_FILE("internal.index.transformedFile", TEXT_ARRAY, ""),
        INTERNAL_INDEX_RELEASE("internal.index.release", INTEGER, ""),
        INTERNAL_MISSING_SAMPLES("internal.missingSamples", OBJECT, ""),
        INTERNAL_MISSING_SAMPLES_EXISTING("internal.missingSamples.existing", TEXT_ARRAY, ""),
        INTERNAL_MISSING_SAMPLES_NON_EXISTING("internal.missingSamples.nonExisting", TEXT_ARRAY, ""),

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
        JOB_ID(QueryParams.JOB_ID.key()),
        SOFTWARE(QueryParams.SOFTWARE.key()),
        STATUS_NAME(QueryParams.INTERNAL_STATUS_NAME.key()),
        SAMPLE_IDS(QueryParams.SAMPLE_IDS.key()),
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

    default boolean exists(long fileId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), fileId)).getNumMatches() > 0;
    }

    default void checkId(long fileId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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

    long getId(long studyId, String path) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyIdByFileId(long fileId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult nativeInsert(Map<String, Object> file, String userId) throws CatalogDBException;

    /***
     * Inserts the passed file in the database.
     *
     * @param studyId Id of the study where the file belongs to.
     * @param file The file to be inserted in the database.
     * @param existingSamples List of existing samples to associate to the File.
     * @param nonExistingSamples List of non-existing samples to create and associate to the File.
     * @param variableSetList Variable set list.
     * @param options Options to filter the output that will be returned after the insertion of the file.
     * @return A OpenCGAResult object containing the time spent.
     * @throws CatalogDBException when the file could not be inserted due to different reasons.
     * @throws CatalogParameterException if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    OpenCGAResult insert(long studyId, File file, List<Sample> existingSamples, List<Sample> nonExistingSamples,
                         List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /***
     * Retrieves the file from the database containing the fileId given.
     *
     * @param fileId File ID of the required file.
     * @param options Options to filter the output.
     * @return A OpenCGAResult object containing the required file.
     * @throws CatalogDBException when the file could not be found in the database.
     * @throws CatalogParameterException if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    OpenCGAResult<File> get(long fileId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /***
     * Retrieves all the files belonging to the given study.
     *
     * @param studyId Study id where the files will be extracted from.
     * @param options Options to filter the output.
     * @return A OpenCGAResult object containing all the files belonging to the study.
     * @throws CatalogDBException when the study does not exist.
     * @throws CatalogParameterException if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    OpenCGAResult<File> getAllInStudy(long studyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

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
     * @throws CatalogParameterException if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    OpenCGAResult rename(long fileId, String filePath, String fileUri, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

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
     * @throws CatalogParameterException if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    OpenCGAResult delete(File file, String status) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Delete file.
     *
     * @param query Delete all the files matching the query.
     * @param status Deletion status we want to set.
     * @return a OpenCGAResult object.
     * @throws CatalogDBException when the status is not a valid delete status or if there was any problem during the deletion.
     * @throws CatalogParameterException if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    OpenCGAResult delete(Query query, String status) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    int getFileSampleLinkThreshold();

    void setFileSampleLinkThreshold(int numSamples);

}
