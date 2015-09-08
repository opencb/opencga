package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.VariableSet;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogStudyDBAdaptor {

    enum StudyFilterOptions implements CatalogDBAdaptor.FilterOption {
        id(Type.NUMERICAL, ""),
        projectId(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        alias(Type.TEXT, ""),
        type(Type.TEXT, ""),
        creatorId(Type.TEXT, ""),
        creationDate(Type.TEXT, ""),
        status(Type.TEXT, "")
        ;

        StudyFilterOptions(String key, Type type, String description) {
            this._key = key;
            this._description = description;
            this._type = type;
        }

        StudyFilterOptions(Type type, String description) {
            this._key = name();
            this._description = description;
            this._type = type;
        }

        final private String _key;
        final private String _description;
        final private Type _type;

        @Override public String getKey() {return _key;}
        @Override public String getDescription() {return _description;}
        @Override public Type getType() {return _type;}
    }


    /**
     * Study methods
     * ***************************
     */

    QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException;

    boolean studyExists(int studyId);

    void checkStudyId(int studyId) throws CatalogDBException;

    QueryResult<Study> getAllStudies(QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getAllStudiesInProject(int projectId, QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException;

//  QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats) throws CatalogManagerException;

    void updateStudyLastActivity(int studyId) throws CatalogDBException;

    QueryResult<ObjectMap> modifyStudy(int studyId, ObjectMap params) throws CatalogDBException;

    QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException;

    int getStudyId(int projectId, String studyAlias) throws CatalogDBException;

    int getProjectIdByStudyId(int studyId) throws CatalogDBException;

    String getStudyOwnerId(int studyId) throws CatalogDBException;

    QueryResult<Group> getGroup(int studyId, String userId, String groupId, QueryOptions options) throws CatalogDBException;

    QueryResult<Group> addMemberToGroup(int studyId, String groupId, String userId) throws CatalogDBException;

    QueryResult<Group> removeMemberFromGroup(int studyId, String groupId, String userId) throws CatalogDBException;


    /**
     * VariableSet Methods
     * ***************************
     */

    QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException;

    QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException;

    QueryResult<VariableSet> getAllVariableSets(int studyId, QueryOptions queryOptions) throws CatalogDBException;

    QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions) throws CatalogDBException;

    int getStudyIdByVariableSetId(int variableSetId) throws CatalogDBException;
}
