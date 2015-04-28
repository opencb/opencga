package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Project;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface IProjectManager extends ResourceManager<Project, Integer> {

    public String  getUserId(int projectId) throws CatalogException;
    public Integer getProjectId(String projectId) throws CatalogException;

    public QueryResult<Project> create(String ownerId, String name, String alias, String description,
                                       String organization, QueryOptions options) throws CatalogException;
}
