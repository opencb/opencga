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
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.catalog.models.acls.permissions.JobAclEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface JobDBAdaptor extends AclDBAdaptor<Job, JobAclEntry> {

    default boolean exists(long jobId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), jobId)).first() > 0;
    }

    default void checkId(long jobId) throws CatalogDBException {
        if (jobId < 0) {
            throw CatalogDBException.newInstance("Job id '{}' is not valid: ", jobId);
        }

        if (!exists(jobId)) {
            throw CatalogDBException.newInstance("Job id '{}' does not exist", jobId);
        }
    }

    QueryResult<Job> insert(Job job, long studyId, QueryOptions options) throws CatalogDBException;


    default QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        //return updateStatus(query, new Job.JobStatus(Job.JobStatus.PREPARED));
        throw new CatalogDBException("Non implemented action.");
    }

    default QueryResult<Job> setStatus(long jobId, String status) throws CatalogDBException {
        return update(jobId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    default QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    default QueryResult<Job> get(long jobId, QueryOptions options) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), jobId);
        QueryResult<Job> jobQueryResult = get(query, options);
        if (jobQueryResult == null || jobQueryResult.getResult().size() == 0) {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
        return jobQueryResult;
    }

    QueryResult<Job> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException;

    String getStatus(long jobId, String sessionId) throws CatalogDBException;

    QueryResult<ObjectMap> incJobVisits(long jobId) throws CatalogDBException;

    long getStudyId(long jobId) throws CatalogDBException;

    /**
     * Remove all the Acls defined for the member in the resource.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member member from whom the Acls will be removed.
     * @throws CatalogDBException if any problem occurs during the removal.
     */
    void removeAclsFromStudy(long studyId, String member) throws CatalogDBException;


    /**
     * Extract the fileIds given from the jobs matching the query. It will try to take them out from the input and output arrays.
     *
     * @param fileIds file ids.
     * @return A queryResult object containing the number of datasets matching the query.
     * @throws CatalogDBException CatalogDBException.
     */
    QueryResult<Long> extractFiles(List<Long> fileIds) throws CatalogDBException;

    /*
     * Tool methods
     */

    QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException;

    QueryResult<Tool> getTool(long id) throws CatalogDBException;

    long getToolId(String userId, String toolAlias) throws CatalogDBException;

    QueryResult<Tool> getAllTools(Query query, QueryOptions queryOptions) throws CatalogDBException;

    /*
     * Experiments methods
     */

    boolean experimentExists(long experimentId);

//    public abstract QueryResult<Tool> searchTool(QueryOptions options);

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        USER_ID("userId", TEXT_ARRAY, ""),
        TOOL_NAME("toolName", TEXT_ARRAY, ""),
        TYPE("type", TEXT_ARRAY, ""),
        CREATION_DATE("creationDate", TEXT_ARRAY, ""),
        DESCRIPTION("description", TEXT_ARRAY, ""),
        START_TIME("startTime", INTEGER_ARRAY, ""),
        END_TIME("endTime", INTEGER_ARRAY, ""),
        OUTPUT_ERROR("outputError", TEXT_ARRAY, ""),
        EXECUTION("execution", TEXT_ARRAY, ""),
        //PARAMS,
        COMMAND_LINE("commandLine", TEXT_ARRAY, ""),
        VISITS("visits", INTEGER_ARRAY, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        DISK_USAGE("diskUsage", DECIMAL, ""),
        OUT_DIR_ID("outDirId", INTEGER_ARRAY, ""),
        TMP_OUT_DIR_URI("tmpOutDirUri", TEXT_ARRAY, ""),
        INPUT("input", INTEGER_ARRAY, ""),
        OUTPUT("output", INTEGER_ARRAY, ""),
        TAGS("tags", TEXT_ARRAY, ""),
        ACL("acl", TEXT_ARRAY, ""),
        ACL_MEMBER("acl.member", TEXT_ARRAY, ""),
        ACL_PERMISSIONS("acl.permissions", TEXT_ARRAY, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        RESOURCE_MANAGER_ATTRIBUTES("resourceManagerAttributes", TEXT_ARRAY, ""),
        ERROR("error", TEXT_ARRAY, ""),
        ERROR_DESCRIPTION("errorDescription", TEXT_ARRAY, ""),

        STUDY_ID("studyId", INTEGER_ARRAY, "");

        private static Map<String, QueryParams> map = new HashMap<>();
        static {
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

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

}
