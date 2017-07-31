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

package org.opencb.opencga.catalog.managers.api;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.JobAclEntry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface IJobManager extends ResourceManager<Long, Job> {

    Long getStudyId(long jobId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param jobStr Job id in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one job id is found.
     */
    AbstractManager.MyResourceId getId(String jobStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param jobStr Job id in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    AbstractManager.MyResourceIds getIds(String jobStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtains the numeric job id given a string.
     *
     * @param userId User id of the user asking for the job id.
     * @param jobStr Job id in string format. Could be one of [id | user@aliasProject:aliasStudy:jobName
     *                | user@aliasStudy:jobName | aliasStudy:jobName | jobName].
     * @return the numeric job id.
     * @throws CatalogException when more than one job id is found or the study or project ids cannot be resolved.
     */
    @Deprecated
    Long getId(String userId, String jobStr) throws CatalogException;

    /**
     * Obtains the list of job ids corresponding to the comma separated list of job strings given in jobStr.
     *
     * @param userId User demanding the action.
     * @param jobStr Comma separated list of job ids.
     * @return A list of job ids.
     * @throws CatalogException CatalogException.
     */
    @Deprecated
    default List<Long> getIds(String userId, String jobStr) throws CatalogException {
        List<Long> jobIds = new ArrayList<>();
        for (String jobId : jobStr.split(",")) {
            jobIds.add(getId(userId, jobId));
        }
        return jobIds;
    }

    /**
     * Delete entries from Catalog.
     *
     * @param ids       Comma separated list of ids corresponding to the objects to delete
     * @param studyStr  Study string.
     * @param options   Deleting options.
     * @param sessionId sessionId   @return A list with the deleted objects
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     * @return A list of queryResult containing the jobs deleted.
     */
    List<QueryResult<Job>> delete(String ids, @Nullable String studyStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException;

    QueryResult<ObjectMap> visit(long jobId, String sessionId) throws CatalogException;

    QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor, Map<String, String> params,
                            String commandLine, URI tmpOutDirUri, long outDirId, List<File> inputFiles, List<File> outputFiles,
                            Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes, Job.JobStatus status,
                            long startTime, long endTime, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Job> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    DBIterator<Job> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Job> count(String studyStr, Query query, String sessionId) throws CatalogException;

    URI createJobOutDir(long studyId, String dirName, String sessionId) throws CatalogException;

    @Deprecated
    long getToolId(String toolId) throws CatalogException;

    QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result, String path, boolean openTool,
                                 String sessionId) throws CatalogException;

    QueryResult<Tool> getTool(long id, String sessionId) throws CatalogException;

    QueryResult<Tool> getTools(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyId    Study id.
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  sessionId.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    default QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        long studyId = query.getLong(JobDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Job[rank]: Study id not found in the query");
        }
        return rank(studyId, query, field, numResults, asc, sessionId);
    }

    default QueryResult groupBy(@Nullable String studyStr, Query query, QueryOptions options, String fields, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(studyStr, query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException;

    @Deprecated
    @Override
    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Group by has to be called passing the study string");
    }

    @Deprecated
    @Override
    default QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Group by has to be called passing the study string");
    }

    QueryResult<Job> queue(long studyId, String jobName, String description, String executable, Job.Type type, Map<String, String> params,
                           List<File> input, List<File> output, File outDir, String userId, Map<String, Object> attributes)
            throws CatalogException;

    List<QueryResult<JobAclEntry>> updateAcl(String job, String studyStr, String memberId, AclParams aclParams, String sessionId)
            throws CatalogException;
}
