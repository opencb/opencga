/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.catalog.models.summaries.VariableSetSummary;

import javax.annotation.Nullable;
import java.io.IOException;
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
     * @param studyStr Study id in string format. Could be one of [id | user@aliasProject:aliasStudy | aliasProject:aliasStudy |
     *                 aliasStudy ].
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
    List<Long> getIds(String userId, String studyStr) throws CatalogException;

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

    /**
     * Delete entries from Catalog.
     *
     * @param ids       Comma separated list of ids corresponding to the objects to delete
     * @param options   Deleting options.
     * @param sessionId sessionId
     * @return A list with the deleted objects
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<Study>> delete(String ids, QueryOptions options, String sessionId) throws CatalogException, IOException;

    @Deprecated
    QueryResult<Study> share(long studyId, AclEntry acl) throws CatalogException;

//    void membersHavePermissionsInStudy(long studyId, List<String> members) throws CatalogException;

    /*---------------------*/
    /* VariableSet METHODS */
    /*---------------------*/

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param variableStr VariableSet in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested id.
     * @throws CatalogException when more than one variableSet is found.
     */
    AbstractManager.MyResourceId getVariableSetId(String variableStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, Boolean confidential, String description,
                                               Map<String, Object> attributes, List<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, Boolean confidential, String description,
                                               Map<String, Object> attributes, Set<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> getVariableSet(String studyStr, String variableSet, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> searchVariableSets(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> deleteVariableSet(String studyStr, String variableSetStr, String sessionId) throws CatalogException;

    QueryResult<VariableSet> addFieldToVariableSet(String studyStr, String variableSetStr, Variable variable, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> removeFieldFromVariableSet(String studyStr, String variableSetStr, String name, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> renameFieldFromVariableSet(String studyStr, String variableSetStr, String oldName, String newName,
                                                        String sessionId) throws CatalogException;

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

    List<QueryResult<StudyAclEntry>> updateAcl(String studyStr, String memberId, Study.StudyAclParams aclParams, String sessionId)
            throws CatalogException;

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
     * Obtain the group asked.
     *
     * @param studyStr study.
     * @param groupId group asked.
     * @param sessionId session id of the user that wants to perform this action.
     * @return the group asked from the study.
     * @throws CatalogException catalogException.
     */
    QueryResult<Group> getGroup(String studyStr, String groupId, String sessionId) throws CatalogException;

    QueryResult<Group> updateGroup(String studyStr, String groupId, GroupParams groupParams, String sessionId) throws CatalogException;

    /**
     * Update the parameters of a group.
     *
     * @param studyStr study.
     * @param groupId group id.
     * @param syncFrom Sync object that will be set.
     * @param sessionId session id of the user that wants to perform this action.
     * @return the group after the update action.
     * @throws CatalogException catalogException.
     */
    QueryResult<Group> syncGroupWith(String studyStr, String groupId, Group.Sync syncFrom, String sessionId) throws CatalogException;

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

    QueryResult<VariableSetSummary> getVariableSetSummary(String studyStr, String variableSetId, String sessionId) throws CatalogException;

    int getCurrentRelease(long studyId) throws CatalogException;
}
