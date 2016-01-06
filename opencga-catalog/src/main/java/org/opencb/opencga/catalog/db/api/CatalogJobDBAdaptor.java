package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogJobDBAdaptor {

    /**
     * Job methods
     * ***************************
     */

    boolean jobExists(int jobId);

    QueryResult<Job> createJob(int studyId, Job job, QueryOptions options) throws CatalogDBException;

    QueryResult<Job> deleteJob(int jobId) throws CatalogDBException;

    QueryResult<Job> getJob(int jobId, QueryOptions options) throws CatalogDBException;

    QueryResult<Job> getAllJobs(QueryOptions query, QueryOptions options) throws CatalogDBException;

    QueryResult<Job> getAllJobsInStudy(int studyId, QueryOptions options) throws CatalogDBException;

    String getJobStatus(int jobId, String sessionId) throws CatalogDBException;

    QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException;

    QueryResult<Job> modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException;

    int getStudyIdByJobId(int jobId) throws CatalogDBException;


    /**
     * Tool methods
     * ***************************
     */

    QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException;

    QueryResult<Tool> getTool(int id) throws CatalogDBException;

    int getToolId(String userId, String toolAlias) throws CatalogDBException;

    QueryResult<Tool> getAllTools(QueryOptions queryOptions) throws CatalogDBException;

//    public abstract QueryResult<Tool> searchTool(QueryOptions options);

    /**
     * Experiments methods
     * ***************************
     */

    boolean experimentExists(int experimentId);

}
