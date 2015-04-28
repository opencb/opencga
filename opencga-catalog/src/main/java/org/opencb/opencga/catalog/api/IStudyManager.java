package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.Study;

import java.util.Map;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface IStudyManager extends ResourceManager<Study, Integer> {
    public String  getUserId(int studyId) throws CatalogException;
    public Integer getProjectId(int studyId) throws CatalogException;
    public Integer getStudyId(String studyId) throws CatalogException;

    public QueryResult<Study> create(int projectId, String name, String alias, Study.Type type,
                                     String creatorId, String creationDate, String description, String status,
                                     String cipher, String uriScheme, Map<String, Object> stats,
                                     Map<String, Object> attributes) throws CatalogException;
    public QueryResult<Study> share(int studyId, Acl acl) throws CatalogException;
}
