package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface IJobManager extends ResourceManager<Integer, Job> {
    Integer getStudyId(int jobId) throws CatalogException;

    QueryResult<ObjectMap> visit(int jobId, String sessionId) throws CatalogException;

    QueryResult<Job> create(int studyId, String name, String toolName, String description, String commandLine,
                            URI tmpOutDirUri, int outDirId, List<Integer> inputFiles, Map<String, Object> attributes,
                            Map<String, Object> resourceManagerAttributes, Job.Status status, QueryOptions options,
                            String sessionId)
            throws CatalogException;

    QueryResult<Job> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException;

    URI createJobOutDir(int studyId, String dirName, String sessionId)
            throws CatalogException ;
}
