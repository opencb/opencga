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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface JobDBAdaptor extends CoreDBAdaptor<Job> {

    default boolean exists(long jobId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), jobId)).getNumMatches() > 0;
    }

    default void checkId(long jobId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (jobId < 0) {
            throw CatalogDBException.newInstance("Job id '{}' is not valid: ", jobId);
        }

        if (!exists(jobId)) {
            throw CatalogDBException.newInstance("Job id '{}' does not exist", jobId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> job, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, Job job, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Insert all jobs in a single transaction.
     * Jobs must be sorted and those that do not depend on other jobs must be first.
     * Job dependencies are also handled in this method, but if a job depend on another, the dependent job must also be passed here.
     * Example: Insert 2 jobs where 'job1' depends on 'job2'.
     * In this scenario, we would need to pass a list of jobs containing [job2, job1] in that exact order.
     * job2 and job1 will be created which means none of those should exist before this method is run.
     *
     * @param studyId studyId
     * @param jobs    Job list.
     * @param options Options.
     * @return an OpenCGAResult.
     * @throws CatalogDBException            CatalogDBException.
     * @throws CatalogParameterException     CatalogParameterException.
     * @throws CatalogAuthorizationException CatalogAuthorizationException.
     */
    OpenCGAResult insert(long studyId, List<Job> jobs, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        //return updateStatus(query, new Job.JobStatus(Job.JobStatus.PREPARED));
        throw new CatalogDBException("Non implemented action.");
    }

    default OpenCGAResult setStatus(long jobId, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(jobId, new ObjectMap(QueryParams.INTERNAL_STATUS_ID.key(), status), QueryOptions.empty());
    }

    default OpenCGAResult setStatus(Query query, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(query, new ObjectMap(QueryParams.INTERNAL_STATUS_ID.key(), status), QueryOptions.empty());
    }

    default OpenCGAResult<Job> get(long jobId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), jobId);
        OpenCGAResult<Job> jobDataResult = get(query, options);
        if (jobDataResult == null || jobDataResult.getResults().size() == 0) {
            throw CatalogDBException.uidNotFound("Job", jobId);
        }
        return jobDataResult;
    }

    OpenCGAResult<Job> getAllInStudy(long studyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    String getStatus(long jobId, String sessionId) throws CatalogDBException;

    long getStudyId(long jobId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId          study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", LONG, ""),
        UUID("uuid", TEXT, ""),
        USER_ID("userId", TEXT, ""),
        COMMAND_LINE("commandLine", TEXT, ""),
        PARAMS("params", MAP, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),

        TOOL("tool", OBJECT, ""),
        TOOL_ID("tool.id", TEXT, ""),
        TOOL_TYPE("tool.type", TEXT, ""),

        PRIORITY("priority", TEXT, ""),

        STATUS("status", OBJECT, ""),
        STATUS_ID("status.id", TEXT, ""),

        INTERNAL("internal", OBJECT, ""),
        INTERNAL_STATUS("internal.status", OBJECT, ""),
        INTERNAL_STATUS_ID("internal.status.id", TEXT, ""),
        INTERNAL_STATUS_DESCRIPTION("internal.status.description", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        INTERNAL_WEBHOOK("internal.webhook", OBJECT, ""),
        INTERNAL_EVENTS("internal.events", OBJECT, ""),
        OUT_DIR("outDir", OBJECT, ""),

        INPUT("input", OBJECT, ""),
        OUTPUT("output", OBJECT, ""),
        DEPENDS_ON("dependsOn", TEXT_ARRAY, ""),
        TAGS("tags", TEXT_ARRAY, ""),

        EXECUTION_ID("executionId", TEXT, ""),
        EXECUTION("execution", OBJECT, ""),
        EXECUTION_START("execution.start", DATE, ""),
        EXECUTION_END("execution.end", DATE, ""),

        STDOUT("stdout", OBJECT, ""),
        STDERR("stderr", OBJECT, ""),

        RELEASE("release", INTEGER, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"

        // The parameters below this line are under revision
        DESCRIPTION("description", TEXT_ARRAY, ""),
        //PARAMS,
        VISITED("visited", BOOLEAN, ""),
        OUT_DIR_UID("outDir.uid", INTEGER, ""),
        INPUT_UID("input.uid", INTEGER_ARRAY, ""),
        OUTPUT_UID("output.uid", INTEGER_ARRAY, ""),
        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", TEXT_ARRAY, ""),
        STUDY_ID("study.id", TEXT, ""),
        STUDY_OTHERS("study.others", TEXT, "");

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
