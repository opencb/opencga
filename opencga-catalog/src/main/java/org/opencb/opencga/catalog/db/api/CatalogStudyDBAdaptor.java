package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.Acl;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogStudyDBAdaptor {

    /**
     * Study methods
     * ***************************
     */

    QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException;

    boolean studyExists(int studyId);

    QueryResult<Study> getAllStudies(int projectId, QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException;

//  QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats) throws CatalogManagerException;

    void updateStudyLastActivity(int studyId) throws CatalogDBException;

    QueryResult<ObjectMap> modifyStudy(int studyId, ObjectMap params) throws CatalogDBException;

    QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException;

    int getStudyId(int projectId, String studyAlias) throws CatalogDBException;

    int getProjectIdByStudyId(int studyId) throws CatalogDBException;

    String getStudyOwnerId(int studyId) throws CatalogDBException;

    QueryResult<Acl> getStudyAcl(int projectId, String userId) throws CatalogDBException;

    QueryResult setStudyAcl(int studyId, Acl newAcl) throws CatalogDBException;


}
