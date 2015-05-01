package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogStudyDBAdaptor {

    /**
     * Study methods
     * ***************************
     */

    public abstract QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException;

    public abstract boolean studyExists(int studyId);

    public abstract QueryResult<Study> getAllStudies(int projectId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException;

//    public abstract QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats) throws CatalogManagerException;

    public abstract void updateStudyLastActivity(int studyId) throws CatalogDBException;

    public abstract QueryResult<ObjectMap> modifyStudy(int studyId, ObjectMap params) throws CatalogDBException;

    public abstract QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException;

    public abstract int getStudyId(int projectId, String studyAlias) throws CatalogDBException;

    public abstract int getProjectIdByStudyId(int studyId) throws CatalogDBException;

    public abstract String getStudyOwnerId(int studyId) throws CatalogDBException;

    public abstract QueryResult<Acl> getStudyAcl(int projectId, String userId) throws CatalogDBException;

    public abstract QueryResult setStudyAcl(int studyId, Acl newAcl) throws CatalogDBException;


}
