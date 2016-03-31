package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IJobManager extends ResourceManager<Long, Job> {

    Long getStudyId(long jobId) throws CatalogException;

    QueryResult<ObjectMap> visit(long jobId, String sessionId) throws CatalogException;

    QueryResult<Job> create(long studyId, String name, String toolName, String description, String executor, Map<String, String> params,
                            String commandLine, URI tmpOutDirUri, long outDirId, List<Long> inputFiles, List<Long> outputFiles,
                            Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes, Job.JobStatus status,
                            long startTime, long endTime, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Job> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    URI createJobOutDir(long studyId, String dirName, String sessionId) throws CatalogException;

    long getToolId(String toolId) throws CatalogException;

    QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result, String path, boolean openTool,
                                 String sessionId) throws CatalogException;

    QueryResult<Tool> readTool(long id, String sessionId) throws CatalogException;

    QueryResult<Tool> readAllTools(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException;

}
