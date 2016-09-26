package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.catalog.models.acls.permissions.JobAclEntry;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface IJobManager extends ResourceManager<Long, Job> {

    Long getStudyId(long jobId) throws CatalogException;

    /**
     * Obtains the numeric job id given a string.
     *
     * @param userId User id of the user asking for the job id.
     * @param jobStr Job id in string format. Could be one of [id | user@aliasProject:aliasStudy:jobName
     *                | user@aliasStudy:jobName | aliasStudy:jobName | jobName].
     * @return the numeric job id.
     * @throws CatalogException when more than one job id is found or the study or project ids cannot be resolved.
     */
    Long getId(String userId, String jobStr) throws CatalogException;

    /**
     * Obtains the list of job ids corresponding to the comma separated list of job strings given in jobStr.
     *
     * @param userId User demanding the action.
     * @param jobStr Comma separated list of job ids.
     * @return A list of job ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String jobStr) throws CatalogException {
        List<Long> jobIds = new ArrayList<>();
        for (String jobId : jobStr.split(",")) {
            jobIds.add(getId(userId, jobId));
        }
        return jobIds;
    }

    /**
     * Retrieve the job Acls for the given members in the job.
     *
     * @param jobStr Job id of which the acls will be obtained.
     * @param members userIds/groupIds for which the acls will be retrieved. When this is null, it will obtain all the acls.
     * @param sessionId Session of the user that wants to retrieve the acls.
     * @return A queryResult containing the job acls.
     * @throws CatalogException when the userId does not have permissions (only the users with an "admin" role will be able to do this),
     * the job id is not valid or the members given do not exist.
     */
    QueryResult<JobAclEntry> getAcls(String jobStr, List<String> members, String sessionId) throws CatalogException;
    default List<QueryResult<JobAclEntry>> getAcls(List<String> jobIds, List<String> members, String sessionId) throws CatalogException {
        List<QueryResult<JobAclEntry>> result = new ArrayList<>(jobIds.size());
        for (String jobId : jobIds) {
            result.add(getAcls(jobId, members, sessionId));
        }
        return result;
    }

    QueryResult<ObjectMap> visit(long jobId, String sessionId) throws CatalogException;

    QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor, Map<String, String> params,
                            String commandLine, URI tmpOutDirUri, long outDirId, List<Long> inputFiles, List<Long> outputFiles,
                            Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes, Job.JobStatus status,
                            long startTime, long endTime, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Job> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

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

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param studyId Study id.
     * @param query   Query object containing the query that will be executed.
     * @param field   Field by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the field.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = query.getLong(JobDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Job[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param studyId Study id.
     * @param query   Query object containing the query that will be executed.
     * @param fields  List of fields by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, List<String> field, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = query.getLong(JobDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Job[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    QueryResult<Job> queue(long studyId, String jobName, String executable, Job.Type type, Map<String, String> params, List<Long> input,
                           List<Long> output, long outDirId, String userId, Map<String, Object> attributes) throws CatalogException;

}
