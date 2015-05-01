package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Project;

/**
* @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
*/
public interface IProjectManager extends ResourceManager<Integer, Project> {

    public String  getUserId(int projectId) throws CatalogException;
    public int getProjectId(String projectId) throws CatalogException;

    public QueryResult<Project> create(String ownerId, String name, String alias, String description,
                                       String organization, QueryOptions options, String sessionId) throws CatalogException;
}
