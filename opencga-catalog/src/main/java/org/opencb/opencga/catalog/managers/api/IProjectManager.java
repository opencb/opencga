package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IProjectManager extends ResourceManager<Integer, Project> {

    String getUserId(int projectId) throws CatalogException;

    int getProjectId(String projectId) throws CatalogException;

    QueryResult<Project> create(String ownerId, String name, String alias, String description,
                                String organization, QueryOptions options, String sessionId) throws CatalogException;
}
