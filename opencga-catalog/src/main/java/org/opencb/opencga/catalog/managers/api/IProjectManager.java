package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IProjectManager extends ResourceManager<Long, Project> {

    String getUserId(long projectId) throws CatalogException;

    long getProjectId(String projectId) throws CatalogException;

    QueryResult<Project> create(String ownerId, String name, String alias, String description, String organization, QueryOptions options,
                                String sessionId) throws CatalogException;

}
