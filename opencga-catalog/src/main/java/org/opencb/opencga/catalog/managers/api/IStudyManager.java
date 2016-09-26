package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.catalog.models.summaries.VariableSetSummary;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface IStudyManager extends ResourceManager<Long, Study> {

    String getUserId(long studyId) throws CatalogException;

    Long getProjectId(long studyId) throws CatalogException;

    /**
     * Obtains the numeric study id given a string.
     *
     * @param userId User id of the user asking for the project id.
     * @param studyStr Study id in string format. Could be one of [id | user@aliasProject:aliasStudy | user@aliasStudy |
     *                 aliasProject:aliasStudy | aliasStudy ].
     * @return the numeric study id.
     * @throws CatalogException CatalogDBException when more than one study id are found.
     */
    Long getId(String userId, String studyStr) throws CatalogException;

    /**
     * Obtains the list of studyIds corresponding to the comma separated list of study strings given in studyStr.
     *
     * @param userId User demanding the action.
     * @param studyStr Comma separated list of study ids.
     * @return A list of study ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String studyStr) throws CatalogException {
        List<Long> studyIds = new ArrayList<>();
        for (String studyId : studyStr.split(",")) {
            studyIds.add(getId(userId, studyId));
        }
        return studyIds;
    }

    @Deprecated
    Long getId(String studyId) throws CatalogException;

    /**
     * Creates a new Study in catalog.
     *
     * @param projectId    Parent project id
     * @param name         Study Name
     * @param alias        Study Alias. Must be unique in the project's studies
     * @param type         Study type: CONTROL_CASE, CONTROL_SET, ... (see org.opencb.opencga.catalog.models.Study.Type)
     * @param creationDate Creation date. If null, now
     * @param description  Study description. If null, empty string
     * @param status       Unused
     * @param cipher       Unused
     * @param uriScheme    UriScheme to select the CatalogIOManager. Default: CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME
     * @param uri          URI for the folder where to place the study. Scheme must match with the uriScheme. Folder must exist.
     * @param datastores   DataStores information
     * @param stats        Optional stats
     * @param attributes   Optional attributes
     * @param options      QueryOptions
     * @param sessionId    User's sessionId
     * @return Generated study
     * @throws CatalogException CatalogException
     */
    QueryResult<Study> create(long projectId, String name, String alias, Study.Type type, String creationDate,
                              String description, Status status, String cipher, String uriScheme, URI uri,
                              Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats, Map<String, Object> attributes,
                              QueryOptions options, String sessionId) throws CatalogException;

    @Deprecated
    QueryResult<Study> share(long studyId, AclEntry acl) throws CatalogException;


    /*---------------------*/
    /* VariableSet METHODS */
    /*---------------------*/

    QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                               Map<String, Object> attributes, List<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                               Map<String, Object> attributes, Set<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> readVariableSet(long variableSet, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> readAllVariableSets(long studyId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions, String sessionId) throws CatalogException;

    QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable, String sessionId) throws CatalogException;

    QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name, String sessionId) throws CatalogException;

    QueryResult<VariableSet> renameFieldFromVariableSet(long variableSetId, String oldName, String newName, String sessionId)
            throws CatalogException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param projectId  Project id.
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  sessionId.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(long projectId, Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    default QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        long projectId = query.getLong(StudyDBAdaptor.QueryParams.PROJECT_ID.key());
        if (projectId == 0L) {
            throw new CatalogException("Study[rank]: Study id not found in the query");
        }
        return rank(projectId, query, field, numResults, asc, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param projectId  Project id.
     * @param query   Query object containing the query that will be executed.
     * @param field   Field by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the field.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long projectId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        long projectId = query.getLong(StudyDBAdaptor.QueryParams.PROJECT_ID.key());
        if (projectId == 0L) {
            throw new CatalogException("Study[groupBy]: Study id not found in the query");
        }
        return groupBy(projectId, query, field, options, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param projectId  Project id.
     * @param query   Query object containing the query that will be executed.
     * @param fields  List of fields by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long projectId, Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, List<String> field, QueryOptions options, String sessionId) throws CatalogException {
        long projectId = query.getLong(StudyDBAdaptor.QueryParams.PROJECT_ID.key());
        if (projectId == 0L) {
            throw new CatalogException("Study[groupBy]: Study id not found in the query");
        }
        return groupBy(projectId, query, field, options, sessionId);
    }

    /**
     * Gets the general stats of a study.
     *
     * @param studyId Study id.
     * @param sessionId Session id.
     * @param queryOptions QueryOptions object.
     * @return a QueryResult object containing a summary with the general stats of the study.
     * @throws CatalogException CatalogException
     */
    QueryResult<StudySummary> getSummary(long studyId, String sessionId, QueryOptions queryOptions) throws CatalogException;

    /**
     * Retrieve the study Acls for the given members.
     *
     * @param studyStr Study id of which the acls will be obtained.
     * @param members userIds/groupIds for which the acls will be retrieved. When this is null, it will obtain all the acls.
     * @param sessionId Session of the user that wants to retrieve the acls.
     * @return A queryResult containing the study acls.
     * @throws CatalogException when the userId does not have permissions (only the users with an "admin" role will be able to do this),
     * the study id is not valid or the members given do not exist.
     */
    @Deprecated
    QueryResult<StudyAclEntry> getAcls(String studyStr, List<String> members, String sessionId) throws CatalogException;

    //-----------------     GROUPS         ------------------

    /**
     * Creates a group in the study.
     *
     * @param studyStr study where the group will be added.
     * @param groupId name of the group that will be used as a unique identifier.
     * @param userList Comma separated list of users that will be added to the group.
     * @param sessionId session id of the user that wants to perform this action.
     * @return the group that has been created.
     * @throws CatalogException when the group already exists or any of the users already belong to a group.
     */
    QueryResult<Group> createGroup(String studyStr, String groupId, String userList, String sessionId) throws CatalogException;

    /**
     * Obtain all the groups that are present in the study.
     *
     * @param studyStr study.
     * @param sessionId session id of the user that wants to perform this action.
     * @return all the groups present in the study.
     * @throws CatalogException catalogException.
     */
    QueryResult<Group> getAllGroups(String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtain the group asked.
     *
     * @param studyStr study.
     * @param groupId group asked.
     * @param sessionId session id of the user that wants to perform this action.
     * @return the group asked from the study.
     * @throws CatalogException catalogException.
     */
    QueryResult<Group> getGroup(String studyStr, String groupId, String sessionId) throws CatalogException;

    /**
     * Update the members of a group.
     *
     * @param studyStr study.
     * @param groupId group id.
     * @param addUsers Comma separated list of users that will be added to the group.
     * @param removeUsers Comma separated list of users that will be removed from the group.
     * @param setUsers Comma separated list of users that will be set to the group. Previous users will be removed.
     * @param sessionId session id of the user that wants to perform this action.
     * @return the group after the update action.
     * @throws CatalogException catalogException.
     */
    QueryResult<Group> updateGroup(String studyStr, String groupId, @Nullable String addUsers, @Nullable String removeUsers,
                                   @Nullable String setUsers, String sessionId) throws CatalogException;

    /**
     * Delete the group.
     *
     * @param studyStr study.
     * @param groupId group id.
     * @param sessionId session id of the user that wants to perform this action.
     * @return the group recently deleted.
     * @throws CatalogException catalogException.
     */
    QueryResult<Group> deleteGroup(String studyStr, String groupId, String sessionId) throws CatalogException;

    // DISEASE PANEL METHODS
    /**
     * Obtains the numeric panel id given a string.
     *
     * @param userId User id of the user asking for the panel id.
     * @param panelStr Panel id in string format. Could be one of [id | user@aliasProject:aliasStudy:panelName
     *                | user@aliasStudy:panelName | aliasStudy:panelName | panelName].
     * @return the numeric panel id.
     * @throws CatalogException when more than one panel id is found.
     */
    Long getDiseasePanelId(String userId, String panelStr) throws CatalogException;

    /**
     * Obtains the list of panel ids corresponding to the comma separated list of panel strings given in panelStr.
     *
     * @param userId User demanding the action.
     * @param panelStr Comma separated list of panel ids.
     * @return A list of panel ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getDiseasePanelIds(String userId, String panelStr) throws CatalogException {
        List<Long> panelIds = new ArrayList<>();
        for (String panelId : panelStr.split(",")) {
            panelIds.add(getDiseasePanelId(userId, panelId));
        }
        return panelIds;
    }

    QueryResult<DiseasePanel> createDiseasePanel(String studyStr, String name, String disease, String description, String genes,
                                                 String regions, String variants, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<DiseasePanel> getDiseasePanel(String panelStr, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<DiseasePanel> updateDiseasePanel(String panelStr, ObjectMap parameters, String sessionId) throws CatalogException;

    QueryResult<VariableSetSummary> getVariableSetSummary(long variableSetId, String sessionId) throws CatalogException;
}
