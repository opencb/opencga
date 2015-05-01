package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.beans.Tool;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogJobDBAdaptor {

    /**
     * Job methods
     * ***************************
     */

    public abstract boolean jobExists(int jobId);

    public abstract QueryResult<Job> createJob(int studyId, Job job, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Job> deleteJob(int jobId) throws CatalogDBException;

    public abstract QueryResult<Job> getJob(int jobId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Job> getAllJobs(int studyId, QueryOptions options) throws CatalogDBException;

    public abstract String getJobStatus(int jobId, String sessionId) throws CatalogDBException;

    public abstract QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException;

    public abstract QueryResult modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException;

    public abstract int getStudyIdByJobId(int jobId) throws CatalogDBException;

    public abstract QueryResult<Job> searchJob(QueryOptions options) throws CatalogDBException;


    /**
     * Tool methods
     * ***************************
     */

    public abstract QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException;

    public abstract QueryResult<Tool> getTool(int id) throws CatalogDBException;

    public abstract int getToolId(String userId, String toolAlias) throws CatalogDBException;


//    public abstract QueryResult<Tool> searchTool(QueryOptions options);

    /**
     * Experiments methods
     * ***************************
     */

    public abstract boolean experimentExists(int experimentId);

}
