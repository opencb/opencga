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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.StudyConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.VariableSetConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.StudyMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.checkCanViewStudy;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.checkStudyPermission;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created on 07/09/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyMongoDBAdaptor extends MongoDBAdaptor implements StudyDBAdaptor {

    private final MongoDBCollection studyCollection;
    private StudyConverter studyConverter;
    private VariableSetConverter variableSetConverter;

    public StudyMongoDBAdaptor(MongoDBCollection studyCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(StudyMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.studyCollection = studyCollection;
        this.studyConverter = new StudyConverter();
        this.variableSetConverter = new VariableSetConverter();
    }

    private boolean studyAliasExists(long projectId, String studyAlias) throws CatalogDBException {
        if (projectId < 0) {
            throw CatalogDBException.newInstance("Project id '{}' is not valid: ", projectId);
        }

        Query query = new Query(QueryParams.PROJECT_ID.key(), projectId).append("alias", studyAlias);
        QueryResult<Long> count = count(query);
        return count.first() != 0;
    }

    @Override
    public void nativeInsert(Map<String, Object> study, String userId) throws CatalogDBException {
        Document studyDocument = getMongoDBDocument(study, "study");
        studyDocument.put(PRIVATE_OWNER_ID, userId);
        studyCollection.insert(studyDocument, null);
    }

    @Override
    public QueryResult<Study> insert(long projectId, Study study, String ownerId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (projectId < 0) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }

        // Check if study.alias already exists.
        if (studyAliasExists(projectId, study.getAlias())) {
            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }

        //Set new ID
//        int newId = getNewAutoIncrementId(metaCollection);
        long newId = getNewId();
        study.setId(newId);

        //Empty nested fields
        List<File> files = study.getFiles();
        study.setFiles(Collections.emptyList());

        List<Job> jobs = study.getJobs();
        study.setJobs(Collections.emptyList());

        List<Cohort> cohorts = study.getCohorts();
        study.setCohorts(Collections.emptyList());

        List<Dataset> datasets = study.getDatasets();
        study.setDatasets(Collections.emptyList());

        List<DiseasePanel> panels = study.getPanels();
        study.setPanels(Collections.emptyList());

        //Create DBObject
        Document studyObject = studyConverter.convertToStorageType(study);
        studyObject.put(PRIVATE_ID, newId);

        //Set ProjectId
        studyObject.put(PRIVATE_PROJECT_ID, projectId);
        studyObject.put(PRIVATE_OWNER_ID, ownerId);

        if (StringUtils.isNotEmpty(study.getCreationDate())) {
            studyObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(study.getCreationDate()));
        } else {
            studyObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }

        //Insert
        QueryResult<WriteResult> updateResult = studyCollection.insert(studyObject, null);

        String errorMsg = updateResult.getErrorMsg() != null ? updateResult.getErrorMsg() : "";

        for (File file : files) {
            String fileErrorMsg = dbAdaptorFactory.getCatalogFileDBAdaptor().insert(file, study.getId(), options).getErrorMsg();
            if (fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg += file.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        for (Job job : jobs) {
//            String jobErrorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
            String jobErrorMsg = dbAdaptorFactory.getCatalogJobDBAdaptor().insert(job, study.getId(), options).getErrorMsg();
            if (jobErrorMsg != null && !jobErrorMsg.isEmpty()) {
                errorMsg += job.getName() + ":" + jobErrorMsg + ", ";
            }
        }

        for (Cohort cohort : cohorts) {
            String fileErrorMsg = dbAdaptorFactory.getCatalogCohortDBAdaptor().insert(cohort, study.getId(), options).getErrorMsg();
            if (fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg += cohort.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        for (Dataset dataset : datasets) {
            String fileErrorMsg = dbAdaptorFactory.getCatalogDatasetDBAdaptor().insert(dataset, study.getId(), options)
                    .getErrorMsg();
            if (fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg += dataset.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        for (DiseasePanel diseasePanel : panels) {
            String fileErrorMsg = dbAdaptorFactory.getCatalogPanelDBAdaptor().insert(diseasePanel, study.getId(), options)
                    .getErrorMsg();
            if (fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg += diseasePanel.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        QueryResult<Study> result = get(study.getId(), options);
        List<Study> studyList = result.getResult();
        return endQuery("Create Study", startTime, studyList, errorMsg, null);

    }

    @Override
    public QueryResult<Study> getAllStudiesInProject(long projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogProjectDbAdaptor().checkId(projectId);
        Query query = new Query(QueryParams.PROJECT_ID.key(), projectId);
        return endQuery("getAllSudiesInProject", startTime, get(query, options));
    }

    @Override
    public boolean hasStudyPermission(long studyId, String user, StudyAclEntry.StudyPermissions permission) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), studyId);
        QueryResult queryResult = nativeGet(query, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + studyId + " not found");
        }

        return checkStudyPermission((Document) queryResult.first(), user, permission.name());
    }

    @Override
    public void updateStudyLastModified(long studyId) throws CatalogDBException {
        update(studyId, new ObjectMap("lastModified", TimeUtils.getTime()), QueryOptions.empty());
    }

    @Override
    public long getId(long projectId, String studyAlias) throws CatalogDBException {
        Query query1 = new Query(QueryParams.PROJECT_ID.key(), projectId).append("alias", studyAlias);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key());
        QueryResult<Study> studyQueryResult = get(query1, queryOptions);
        List<Study> studies = studyQueryResult.getResult();
        return studies == null || studies.isEmpty() ? -1 : studies.get(0).getId();
    }

    @Override
    public long getProjectIdByStudyId(long studyId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), studyId);
        QueryOptions queryOptions = new QueryOptions("include", FILTER_ROUTE_STUDIES + PRIVATE_PROJECT_ID);
        QueryResult result = nativeGet(query, queryOptions);

        if (!result.getResult().isEmpty()) {
            Document study = (Document) result.getResult().get(0);
            Object id = study.get(PRIVATE_PROJECT_ID);
            return id instanceof Number ? ((Number) id).longValue() : Long.parseLong(id.toString());
        } else {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
    }

    @Override
    public String getOwnerId(long studyId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), studyId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, PRIVATE_OWNER_ID);
        QueryResult<Document> documentQueryResult = nativeGet(query, options);
        if (documentQueryResult.getNumResults() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
        return documentQueryResult.first().getString(PRIVATE_OWNER_ID);
    }

    @Override
    public QueryResult<Group> createGroup(long studyId, Group group) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_NAME.key(), new Document("$ne", group.getName()));
        Document update = new Document("$push", new Document(QueryParams.GROUPS.key(), getMongoDBDocument(group, "Group")));

        QueryResult<UpdateResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            QueryResult<Group> group1 = getGroup(studyId, group.getName(), Collections.emptyList());
            if (group1.getNumResults() > 0) {
                throw new CatalogDBException("Unable to create the group " + group.getName() + ". Group already existed.");
            } else {
                throw new CatalogDBException("Unable to create the group " + group.getName() + ".");
            }
        }

        return endQuery("Create group", startTime, getGroup(studyId, group.getName(), Collections.emptyList()));
    }

    private long getDiskUsageByStudy(int studyId) {

        List<Bson> operations = new ArrayList<>();
        operations.add(Aggregates.match(Filters.eq(PRIVATE_STUDY_ID, studyId)));
        operations.add(Aggregates.group("$" + PRIVATE_STUDY_ID, Accumulators.sum("size", "$diskUsage")));

        QueryResult<Document> aggregate = dbAdaptorFactory.getCatalogFileDBAdaptor().getFileCollection()
                .aggregate(operations, null);
        if (aggregate.getNumResults() == 1) {
            Object size = aggregate.getResult().get(0).get("size");
            if (size instanceof Integer) {
                return ((Integer) size).longValue();
            } else if (size instanceof Long) {
                return ((Long) size);
            } else {
                return Long.parseLong(size.toString());
            }
        } else {
            return 0;
        }
    }

    @Override
    public QueryResult<Group> getGroup(long studyId, @Nullable String groupId, List<String> userIds) throws CatalogDBException {
        long startTime = startQuery();
        checkId(studyId);

        if (userIds == null) {
            userIds = Collections.emptyList();
        }

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, studyId)));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.GROUPS.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.GROUPS.key()));

        if (userIds.size() > 0) {
            aggregation.add(Aggregates.match(Filters.in(QueryParams.GROUP_USER_IDS.key(), userIds)));
        }
        if (groupId != null && groupId.length() > 0) {
            aggregation.add(Aggregates.match(Filters.eq(QueryParams.GROUP_NAME.key(), groupId)));
        }

        QueryResult<Document> queryResult = studyCollection.aggregate(aggregation, null);

        List<Study> studies = MongoDBUtils.parseStudies(queryResult);
        List<Group> groups = new ArrayList<>();
        studies.stream().filter(study -> study.getGroups() != null).forEach(study -> groups.addAll(study.getGroups()));
        return endQuery("getGroup", startTime, groups);
    }

    @Override
    public void setUsersToGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        if (members == null) {
            members = Collections.emptyList();
        }

        // Check that the members exist.
        if (members.size() > 0) {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkIds(members);
        }

        Document query = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_NAME.key(), groupId)
                .append("$isolated", 1);
        Document update = new Document("$set", new Document("groups.$.userIds", members));
        QueryResult<UpdateResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getMatchedCount() != 1) {
            throw new CatalogDBException("Unable to set users to group " + groupId + ". The group does not exist.");
        }
    }

    @Override
    public void addUsersToGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        if (members == null) {
            members = Collections.emptyList();
        }

        Document query = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_NAME.key(), groupId)
                .append("$isolated", 1);
        Document update = new Document("$addToSet", new Document("groups.$.userIds", new Document("$each", members)));
        QueryResult<UpdateResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getMatchedCount() != 1) {
            throw new CatalogDBException("Unable to add members to group " + groupId + ". The group does not exist.");
        }
    }

    @Override
    public void removeUsersFromGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        if (members == null || members.size() == 0) {
            throw new CatalogDBException("Unable to remove members from group. List of members is empty");
        }

        Document query = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_NAME.key(), groupId)
                .append("$isolated", 1);
        Bson pull = Updates.pullAll("groups.$.userIds", members);
        QueryResult<UpdateResult> update = studyCollection.update(query, pull, null);
        if (update.first().getMatchedCount() != 1) {
            throw new CatalogDBException("Unable to remove members from group " + groupId + ". The group does not exist.");
        }
    }

    @Override
    public void removeUsersFromAllGroups(long studyId, List<String> users) throws CatalogDBException {
        if (users == null || users.size() == 0) {
            throw new CatalogDBException("Unable to remove users from groups. List of users is empty");
        }

        Document query = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_USER_IDS.key(), new Document("$in", users))
                .append("$isolated", 1);
        Bson pull = Updates.pullAll("groups.$.userIds", users);

        // Pull those users while they are still there
        QueryResult<UpdateResult> update;
        do {
            update = studyCollection.update(query, pull, null);
        } while (update.first().getModifiedCount() > 0);
    }

    @Override
    public void deleteGroup(long studyId, String groupId) throws CatalogDBException {
        Bson queryBson = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_NAME.key(), groupId)
                .append("$isolated", 1);
        Document pull = new Document("$pull", new Document("groups", new Document("name", groupId)));
        QueryResult<UpdateResult> update = studyCollection.update(queryBson, pull, null);

        if (update.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Could not remove the group " + groupId);
        }
    }

    @Override
    public void syncGroup(long studyId, String groupId, Group.Sync syncedFrom) throws CatalogDBException {
        Document mongoDBDocument = getMongoDBDocument(syncedFrom, "Group.Sync");

        Document query = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_NAME.key(), groupId)
                .append("$isolated", 1);
        Document updates = new Document("$set", new Document("groups.$.syncedFrom", mongoDBDocument));
        studyCollection.update(query, updates, null);
    }

    @Override
    public void resyncUserWithSyncedGroups(String user, List<String> groupList, String authOrigin) throws CatalogDBException {
        if (StringUtils.isEmpty(user)) {
            throw new CatalogDBException("Missing user field");
        }

        // 1. Take the user out from all synced groups
        Document query = new Document()
                .append(QueryParams.GROUPS.key(), new Document("$elemMatch", new Document()
                        .append("userIds", user)
                        .append("syncedFrom.authOrigin", authOrigin)
                ))
                .append("$isolated", 1);
        Bson pull = Updates.pull("groups.$.userIds", user);

        // Pull the user while it still belongs to a synced group
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        QueryResult<UpdateResult> update;
        do {
            update = studyCollection.update(query, pull, multi);
        } while (update.first().getModifiedCount() > 0);

        // 2. Add user to all synced groups
        if (groupList != null && groupList.size() > 0) {
            // Add the user to all the synced groups matching
            query = new Document()
                    .append(QueryParams.GROUPS.key(), new Document("$elemMatch", new Document()
                            .append("userIds", new Document("$ne", user))
                            .append("syncedFrom.remoteGroup", new Document("$in", groupList))
                            .append("syncedFrom.authOrigin", authOrigin)
                    ))
                    .append("$isolated", 1);
            Document push = new Document("$addToSet", new Document("groups.$.userIds", user));
            do {
                update = studyCollection.update(query, push, multi);
            } while (update.first().getModifiedCount() > 0);

            // We need to be updated with the internal @members group, so we fetch all the studies where the user has been added
            // and attempt to add it to the each @members group
            query = new Document()
                    .append(QueryParams.GROUP_USER_IDS.key(), user)
                    .append(QueryParams.GROUP_SYNCED_FROM_AUTH_ORIGIN.key(), authOrigin);
            QueryResult<Study> studyQueryResult = studyCollection.find(query, studyConverter, new QueryOptions(QueryOptions.INCLUDE,
                    QueryParams.ID.key()));
            for (Study study : studyQueryResult.getResult()) {
                addUsersToGroup(study.getId(), "@members", Arrays.asList(user));
            }
        }
    }

    @Override
    public void createPermissionRule(long studyId, Study.Entry entry, PermissionRule permissionRule) throws CatalogDBException {
        if (entry == null) {
            throw new CatalogDBException("Missing entry parameter");
        }

        // Get permission rules from study
        QueryResult<PermissionRule> permissionRulesResult = getPermissionRules(studyId, entry);

        List<Document> permissionDocumentList = new ArrayList<>();
        if (permissionRulesResult.getNumResults() > 0) {
            for (PermissionRule rule : permissionRulesResult.getResult()) {
                // We add all the permission rules with different id
                if (!rule.getId().equals(permissionRule.getId())) {
                    permissionDocumentList.add(getMongoDBDocument(rule, "PermissionRules"));
                } else {
                    throw new CatalogDBException("Permission rule " + permissionRule.getId() + " already exists.");
                }
            }
        }

        permissionDocumentList.add(getMongoDBDocument(permissionRule, "PermissionRules"));

        // We update the study document to contain the new permission rules
        Query query = new Query(QueryParams.ID.key(), studyId);
        Document update = new Document("$set", new Document(QueryParams.PERMISSION_RULES.key() + "." + entry, permissionDocumentList));
        QueryResult<UpdateResult> updateResult = studyCollection.update(parseQuery(query, true), update, QueryOptions.empty());

        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("Unexpected error occurred when adding new permission rules to study");
        }
    }

    @Override
    public void markDeletedPermissionRule(long studyId, Study.Entry entry, String permissionRuleId,
                                          PermissionRule.DeleteAction deleteAction) throws CatalogDBException {
        if (entry == null) {
            throw new CatalogDBException("Missing entry parameter");
        }

        String newPermissionRuleId = permissionRuleId + INTERNAL_DELIMITER + "DELETE_" + deleteAction.name();

        Document query = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.PERMISSION_RULES.key() + "." + entry + ".id", permissionRuleId);
        // Change permissionRule id
        Document update = new Document("$set", new Document(QueryParams.PERMISSION_RULES.key() + "." + entry + ".$.id",
                newPermissionRuleId));

        logger.debug("Mark permission rule for deletion: Query {}, Update {}",
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        QueryResult<UpdateResult> updateQueryResult = studyCollection.update(query, update, QueryOptions.empty());
        if (updateQueryResult.first().getMatchedCount() == 0) {
            throw new CatalogDBException("Permission rule " + permissionRuleId + " not found");
        }

        if (updateQueryResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("Unexpected error: Permission rule " + permissionRuleId + " could not be marked for deletion");
        }
    }

    @Override
    public QueryResult<PermissionRule> getPermissionRules(long studyId, Study.Entry entry) throws CatalogDBException {
        // Get permission rules from study
        Query query = new Query(QueryParams.ID.key(), studyId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.PERMISSION_RULES.key());

        QueryResult<Study> studyQueryResult = get(query, options);
        if (studyQueryResult.getNumResults() == 0) {
            throw new CatalogDBException("Unexpected error: Study " + studyId + " not found");
        }

        List<PermissionRule> permissionRules = studyQueryResult.first().getPermissionRules().get(entry);
        if (permissionRules == null) {
            permissionRules = Collections.emptyList();
        }

        return new QueryResult<>(String.valueOf(studyId), studyQueryResult.getDbTime(), permissionRules.size(), permissionRules.size(),
                "", "", permissionRules);
    }

    /*
     * Variables Methods
     * ***************************
     */

    @Override
    public Long variableSetExists(long variableSetId) {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.elemMatch(QueryParams.VARIABLE_SET.key(),
                Filters.eq(VariableSetParams.ID.key(), variableSetId))));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.VARIABLE_SET.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.VARIABLE_SET.key()));
        aggregation.add(Aggregates.match(Filters.eq(QueryParams.VARIABLE_SET_ID.key(), variableSetId)));
        QueryResult<VariableSet> queryResult = studyCollection.aggregate(aggregation, variableSetConverter, new QueryOptions());

        return (long) queryResult.getResult().size();
    }

    @Override
    public QueryResult<VariableSet> createVariableSet(long studyId, VariableSet variableSet) throws CatalogDBException {
        long startTime = startQuery();

        if (variableSetExists(variableSet.getName(), studyId) > 0) {
            throw new CatalogDBException("VariableSet { name: '" + variableSet.getName() + "'} already exists.");
        }

        long variableSetId = getNewId();
        variableSet.setId(variableSetId);
        Document object = getMongoDBDocument(variableSet, "VariableSet");
        object.put("id", variableSetId);

        Bson bsonQuery = Filters.eq(PRIVATE_ID, studyId);
        Bson update = Updates.push("variableSets", object);
        QueryResult<UpdateResult> queryResult = studyCollection.update(bsonQuery, update, null);

        if (queryResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("createVariableSet: Could not create a new variable set in study " + studyId);
        }

        return endQuery("createVariableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<VariableSet> variableSet = getVariableSet(variableSetId, new QueryOptions(), user);
        checkVariableNotInVariableSet(variableSet.first(), variable.getName());

        Bson bsonQuery = Filters.eq(QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        Bson update = Updates.push(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(),
                getMongoDBDocument(variable, "variable"));
        QueryResult<UpdateResult> queryResult = studyCollection.update(bsonQuery, update, null);
        if (queryResult.first().getModifiedCount() == 0) {
            throw CatalogDBException.updateError("VariableSet", variableSetId);
        }
        if (variable.isRequired()) {
            dbAdaptorFactory.getCatalogSampleDBAdaptor().addVariableToAnnotations(variableSetId, variable);
            dbAdaptorFactory.getCatalogCohortDBAdaptor().addVariableToAnnotations(variableSetId, variable);
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().addVariableToAnnotations(variableSetId, variable);
            dbAdaptorFactory.getCatalogFamilyDBAdaptor().addVariableToAnnotations(variableSetId, variable);
        }
        return endQuery("Add field to variable set", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> renameFieldVariableSet(long variableSetId, String oldName, String newName, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<VariableSet> variableSet = getVariableSet(variableSetId, new QueryOptions(), user);
        checkVariableNotInVariableSet(variableSet.first(), newName);

        // The field can be changed if we arrive to this point.
        // 1. we obtain the variable
        Variable variable = getVariable(variableSet.first(), oldName);
        if (variable == null) {
            throw new CatalogDBException("VariableSet {id: " + variableSet.getId() + "}. The variable {id: " + oldName + "} does not "
                    + "exist.");
        }

        // 2. we take it out from the array.
        Bson bsonQuery = Filters.eq(QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        Bson update = Updates.pull(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(), Filters.eq("name", oldName));
        QueryResult<UpdateResult> queryResult = studyCollection.update(bsonQuery, update, null);

        if (queryResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - Could not rename the field " + oldName);
        }
        if (queryResult.first().getModifiedCount() > 1) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - An unexpected error happened when extracting the "
                    + "variable from the variableSet to do the rename. Please, report this error to the OpenCGA developers.");
        }

        // 3. we change the name in the variable object and push it again in the array.
        variable.setName(newName);
        update = Updates.push(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(),
                getMongoDBDocument(variable, "Variable"));
        queryResult = studyCollection.update(bsonQuery, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - A critical error happened when trying to rename one "
                    + "of the variables of the variableSet object. Please, report this error to the OpenCGA developers.");
        }

        // 4. Change the field id in the annotations
        dbAdaptorFactory.getCatalogSampleDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);
        dbAdaptorFactory.getCatalogFamilyDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);
        dbAdaptorFactory.getCatalogIndividualDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);

        return endQuery("Rename field in variableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<VariableSet> variableSet = getVariableSet(variableSetId, new QueryOptions(), user);
        checkVariableInVariableSet(variableSet.first(), name);

        Bson bsonQuery = Filters.eq(QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        Bson update = Updates.pull(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(),
                Filters.eq("name", name));
        QueryResult<UpdateResult> queryResult = studyCollection.update(bsonQuery, update, null);
        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Remove field from Variable Set. Could not remove the field " + name
                    + " from the variableSet id " +  variableSetId);
        }

        // Remove all the annotations from that field
        dbAdaptorFactory.getCatalogSampleDBAdaptor().removeAnnotationField(variableSetId, name);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().removeAnnotationField(variableSetId, name);
        dbAdaptorFactory.getCatalogIndividualDBAdaptor().removeAnnotationField(variableSetId, name);
        dbAdaptorFactory.getCatalogFamilyDBAdaptor().removeAnnotationField(variableSetId, name);

        return endQuery("Remove field from Variable Set", startTime, getVariableSet(variableSetId, null));
    }

    private Variable getVariable(VariableSet variableSet, String variableId) throws CatalogDBException {
        for (Variable variable : variableSet.getVariables()) {
            if (variable.getName().equals(variableId)) {
                return variable;
            }
        }
        return null;
    }

    /**
     * Checks if the variable given is present in the variableSet.
     * @param variableSet Variable set.
     * @param variableId VariableId that will be checked.
     * @throws CatalogDBException when the variableId is not present in the variableSet.
     */
    private void checkVariableInVariableSet(VariableSet variableSet, String variableId) throws CatalogDBException {
        if (getVariable(variableSet, variableId) == null) {
            throw new CatalogDBException("VariableSet {id: " + variableSet.getId() + "}. The variable {id: " + variableId + "} does not "
                    + "exist.");
        }
    }

    /**
     * Checks if the variable given is not present in the variableSet.
     * @param variableSet Variable set.
     * @param variableId VariableId that will be checked.
     * @throws CatalogDBException when the variableId is present in the variableSet.
     */
    private void checkVariableNotInVariableSet(VariableSet variableSet, String variableId) throws CatalogDBException {
        if (getVariable(variableSet, variableId) != null) {
            throw new CatalogDBException("VariableSet {id: " + variableSet.getId() + "}. The variable {id: " + variableId + "} already "
                    + "exists.");
        }
    }

    @Override
    public QueryResult<VariableSet> getVariableSet(long variableSetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query(QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        Bson projection = Projections.elemMatch("variableSets", Filters.eq("id", variableSetId));
        if (options == null) {
            options = new QueryOptions();
        }
        QueryOptions qOptions = new QueryOptions(options);
        qOptions.put(MongoDBCollection.ELEM_MATCH, projection);
        QueryResult<Study> studyQueryResult = get(query, qOptions);

        if (studyQueryResult.getResult().isEmpty() || studyQueryResult.first().getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist.");
        }

        return endQuery("", startTime, studyQueryResult.first().getVariableSets());
    }

    @Override
    public QueryResult<VariableSet> getVariableSet(long variableSetId, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        Bson query = new Document("variableSets", new Document("$elemMatch", new Document("id", variableSetId)));
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "variableSets.$,_ownerId,groups,_acl");
        QueryResult<Document> studyQueryResult = studyCollection.find(query, qOptions);

        if (studyQueryResult.getNumResults() == 0) {
            throw new CatalogDBException("Variable set not found.");
        }
        if (!checkCanViewStudy(studyQueryResult.first(), user)) {
            throw CatalogAuthorizationException.deny(user, "view", "VariableSet", variableSetId, "");
        }
        Study study = studyConverter.convertToDataModelType(studyQueryResult.first());
        if (study.getVariableSets() == null || study.getVariableSets().isEmpty()) {
            throw new CatalogDBException("Variable set not found.");
        }
        // Check if it is confidential
        if (study.getVariableSets().get(0).isConfidential()) {
            if (!checkStudyPermission(studyQueryResult.first(), user,
                    StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString())) {
                throw CatalogAuthorizationException.deny(user, StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString(),
                        "VariableSet", variableSetId, "");
            }
        }

        return endQuery("", startTime, study.getVariableSets());
    }

    @Override
    public QueryResult<VariableSet> getVariableSets(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        List<Document> mongoQueryList = new LinkedList<>();
        long studyId = -1;

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                StudyDBAdaptor.VariableSetParams option = StudyDBAdaptor.VariableSetParams.getParam(key) != null
                        ? StudyDBAdaptor.VariableSetParams.getParam(key)
                        : StudyDBAdaptor.VariableSetParams.getParam(entry.getKey());
                if (option == null) {
                    logger.warn("{} unknown", entry.getKey());
                    continue;
                }
                switch (option) {
                    case STUDY_ID:
                        studyId = query.getLong(VariableSetParams.STUDY_ID.key());
                        break;
                    default:
                        String optionsKey = "variableSets." + entry.getKey().replaceFirst(option.name(), option.key());
                        addCompQueryFilter(option, entry.getKey(), optionsKey, query, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        if (studyId == -1) {
            throw new CatalogDBException("Cannot look for variable sets if studyId is not passed");
        }

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, studyId)));
        aggregation.add(Aggregates.project(Projections.include("variableSets")));
        aggregation.add(Aggregates.unwind("$variableSets"));
        if (mongoQueryList.size() > 0) {
            List<Bson> bsonList = new ArrayList<>(mongoQueryList.size());
            bsonList.addAll(mongoQueryList);
            aggregation.add(Aggregates.match(Filters.and(bsonList)));
        }

        QueryResult<Document> queryResult = studyCollection.aggregate(aggregation, filterOptions(queryOptions, FILTER_ROUTE_STUDIES));

        List<VariableSet> variableSets = parseObjects(queryResult, Study.class).stream().map(study -> study.getVariableSets().get(0))
                .collect(Collectors.toList());

        return endQuery("", startTime, variableSets);
    }

    @Override
    public QueryResult<VariableSet> getVariableSets(Query query, QueryOptions queryOptions, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        List<Document> mongoQueryList = new LinkedList<>();
        long studyId = -1;

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                StudyDBAdaptor.VariableSetParams option = StudyDBAdaptor.VariableSetParams.getParam(key) != null
                        ? StudyDBAdaptor.VariableSetParams.getParam(key)
                        : StudyDBAdaptor.VariableSetParams.getParam(entry.getKey());
                if (option == null) {
                    logger.warn("{} unknown", entry.getKey());
                    continue;
                }
                switch (option) {
                    case STUDY_ID:
                        studyId = query.getLong(VariableSetParams.STUDY_ID.key());
                        break;
                    default:
                        String optionsKey = "variableSets." + entry.getKey().replaceFirst(option.name(), option.key());
                        addCompQueryFilter(option, entry.getKey(), optionsKey, query, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        if (studyId == -1) {
            throw new CatalogDBException("Cannot look for variable sets if studyId is not passed");
        }

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, studyId)));
        aggregation.add(Aggregates.unwind("$variableSets"));
        if (mongoQueryList.size() > 0) {
            List<Bson> bsonList = new ArrayList<>(mongoQueryList.size());
            bsonList.addAll(mongoQueryList);
            aggregation.add(Aggregates.match(Filters.and(bsonList)));
        }

        QueryResult<Document> queryResult = studyCollection.aggregate(aggregation, filterOptions(queryOptions, FILTER_ROUTE_STUDIES));
        if (queryResult.getNumResults() == 0) {
            return endQuery("", startTime, Collections.emptyList());
        }

        if (!checkCanViewStudy(queryResult.first(), user)) {
            throw new CatalogAuthorizationException("Permission denied: " + user + " cannot see any variable set");
        }

        boolean hasConfidentialPermission = checkStudyPermission(queryResult.first(), user,
                StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString());
        List<VariableSet> variableSets = new ArrayList<>();
        for (Document studyDocument : queryResult.getResult()) {
            Study study = studyConverter.convertToDataModelType(studyDocument);
            VariableSet vs = study.getVariableSets().get(0);
            if (!vs.isConfidential() || hasConfidentialPermission) {
                variableSets.add(vs);
            }
        }

        return endQuery("", startTime, variableSets);
    }

    @Override
    public QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<VariableSet> variableSet = getVariableSet(variableSetId, queryOptions, user);
        checkVariableSetInUse(variableSetId);

        Bson query = Filters.eq(QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        Bson operation = Updates.pull("variableSets", Filters.eq("id", variableSetId));
        QueryResult<UpdateResult> update = studyCollection.update(query, operation, null);

        if (update.first().getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
        return endQuery("Delete VariableSet", startTime, variableSet);
    }

    public void checkVariableSetInUse(long variableSetId) throws CatalogDBException {
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(
                new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId), new QueryOptions());
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of samples : [";
            for (Sample sample : samples.getResult()) {
                msg += " { id: " + sample.getId() + ", name: \"" + sample.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Individual> individuals = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(
                new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId),
                new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of individuals : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor().get(
                new Query(CohortDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId), new QueryOptions());
        if (cohorts.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of cohorts : [";
            for (Cohort cohort : cohorts.getResult()) {
                msg += " { id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Family> families = dbAdaptorFactory.getCatalogFamilyDBAdaptor().get(
                new Query(FamilyDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId), new QueryOptions());
        if (cohorts.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of families : [";
            for (Family family : families.getResult()) {
                msg += " { id: " + family.getId() + ", name: \"" + family.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
    }


    @Override
    public long getStudyIdByVariableSetId(long variableSetId) throws CatalogDBException {
//        DBObject query = new BasicDBObject("variableSets.id", variableSetId);
        Bson query = Filters.eq("variableSets.id", variableSetId);
        Bson projection = Projections.include("id");

//        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", true), null);
        QueryResult<Document> queryResult = studyCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
    }


    @Override
    public QueryResult<Study> getStudiesFromUser(String userId, QueryOptions queryOptions) throws CatalogDBException {
        QueryResult<Study> result = new QueryResult<>("Get studies from user");

        QueryResult<Project> allProjects = dbAdaptorFactory.getCatalogProjectDbAdaptor().get(userId, new QueryOptions());
        if (allProjects.getNumResults() == 0) {
            return result;
        }

        for (Project project : allProjects.getResult()) {
            QueryResult<Study> allStudiesInProject = getAllStudiesInProject(project.getId(), queryOptions);
            if (allStudiesInProject.getNumResults() > 0) {
                result.getResult().addAll(allStudiesInProject.getResult());
                result.setDbTime(result.getDbTime() + allStudiesInProject.getDbTime());
            }
        }

        result.setNumTotalResults(result.getResult().size());
        result.setNumResults(result.getResult().size());

        return result;
    }

    /*
    * Helper methods
    ********************/


    //Join fields from other collections
    private void joinFields(User user, QueryOptions options) throws CatalogDBException {
        if (options == null) {
            return;
        }
        if (user.getProjects() != null) {
            for (Project project : user.getProjects()) {
                joinFields(project, options);
            }
        }
    }

    private void joinFields(Project project, QueryOptions options) throws CatalogDBException {
        if (options == null) {
            return;
        }
        if (options.getBoolean("includeStudies")) {
            project.setStudies(getAllStudiesInProject(project.getId(), options).getResult());
        }
    }

    private void joinFields(Study study, QueryOptions options) throws CatalogDBException {
        try {
            joinFields(study, options, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
    }

    private void joinFields(Study study, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long studyId = study.getId();
        if (studyId <= 0 || options == null) {
            return;
        }

        if (options.getBoolean("includeFiles")) {
            if (StringUtils.isEmpty(user)) {
                study.setFiles(dbAdaptorFactory.getCatalogFileDBAdaptor().getAllInStudy(studyId, options).getResult());
            } else {
                Query query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                study.setFiles(dbAdaptorFactory.getCatalogFileDBAdaptor().get(query, options, user).getResult());
            }
        }
        if (options.getBoolean("includeJobs")) {
            if (StringUtils.isEmpty(user)) {
                study.setJobs(dbAdaptorFactory.getCatalogJobDBAdaptor().getAllInStudy(studyId, options).getResult());
            } else {
                Query query = new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                study.setJobs(dbAdaptorFactory.getCatalogJobDBAdaptor().get(query, options, user).getResult());
            }
        }
        if (options.getBoolean("includeSamples")) {
            if (StringUtils.isEmpty(user)) {
                study.setSamples(dbAdaptorFactory.getCatalogSampleDBAdaptor().getAllInStudy(studyId, options).getResult());
            } else {
                Query query = new Query(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                study.setSamples(dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, options, user).getResult());
            }
        }
        if (options.getBoolean("includeIndividuals")) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
            if (StringUtils.isEmpty(user)) {
                study.setIndividuals(dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(query, options).getResult());
            } else {
                study.setIndividuals(dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(query, options, user).getResult());
            }
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return studyCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission) throws CatalogDBException {
        throw new NotImplementedException("Count not implemented for study collection");
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return studyCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        //FIXME: Check the commented code from modifyStudy
        /*
        long startTime = startQuery();

        checkStudyId(studyId);
//        BasicDBObject studyParameters = new BasicDBObject();
        Document studyParameters = new Document();

        String[] acceptedParams = {"name", "creationDate", "creationId", "description", "status", "lastModified", "cipher"};
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {"size"};
        filterLongParams(parameters, parameters, acceptedLongParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("type"), Study.Type.class);
        filterEnumParams(parameters, studyParameters, acceptedEnums);

        if (parameters.containsKey("uri")) {
            URI uri = parameters.get("uri", URI.class);
            studyParameters.put("uri", uri.toString());
        }

        if (!studyParameters.isEmpty()) {
//            BasicDBObject query = new BasicDBObject(PRIVATE_ID, studyId);
            Bson eq = Filters.eq(PRIVATE_ID, studyId);
            BasicDBObject updates = new BasicDBObject("$set", studyParameters);

//            QueryResult<WriteResult> updateResult = studyCollection.update(query, updates, null);
            QueryResult<UpdateResult> updateResult = studyCollection.update(eq, updates, null);
            if (updateResult.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Study", studyId);
            }
        }
        return endQuery("Modify study", startTime, getStudy(studyId, null));
        * */

        long startTime = startQuery();
        Document studyParameters = new Document();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.CREATION_DATE.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.CIPHER.key(), };
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {QueryParams.SIZE.key()};
        filterLongParams(parameters, studyParameters, acceptedLongParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

        //Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("type"), Study.Type.class);
        //filterEnumParams(parameters, studyParameters, acceptedEnums);

        if (parameters.containsKey(QueryParams.URI.key())) {
            URI uri = parameters.get(QueryParams.URI.key(), URI.class);
            studyParameters.put(QueryParams.URI.key(), uri.toString());
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            studyParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            studyParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!studyParameters.isEmpty()) {
            Document updates = new Document("$set", studyParameters);
            Long nModified = studyCollection.update(parseQuery(query, false), updates, null).getNumTotalResults();
            return endQuery("Study update", startTime, Collections.singletonList(nModified));
        }

        return endQuery("Study update", startTime, Collections.singletonList(0L));
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = studyCollection.remove(parseQuery(query, false), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("Study");
        }
    }

    @Override
    public QueryResult<Study> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {

        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters, QueryOptions.empty());
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update study with id " + id);
        }
        return endQuery("Update study", startTime, get(id, null));

    }

    @Override
    public QueryResult<Study> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check the study is active
        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.READY);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED + "," + Status.DELETED);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_NAME.key());
            Study study = get(query, options).first();
            throw new CatalogDBException("The study {" + id + "} was already " + study.getStatus().getName());
        }

        // If we don't find the force parameter, we check first if the user does not have an active project.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            // Delete the active studies (if any)
            query = new Query(PRIVATE_STUDY_ID, id);
            dbAdaptorFactory.getCatalogFileDBAdaptor().setStatus(query, Status.TRASHED);
            dbAdaptorFactory.getCatalogJobDBAdaptor().setStatus(query, Status.TRASHED);
            dbAdaptorFactory.getCatalogSampleDBAdaptor().setStatus(query, Status.TRASHED);
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().setStatus(query, Status.TRASHED);
            dbAdaptorFactory.getCatalogCohortDBAdaptor().setStatus(query, Status.TRASHED);
            dbAdaptorFactory.getCatalogDatasetDBAdaptor().setStatus(query, Status.TRASHED);
        }

        // Change the status of the project to deleted
        setStatus(id, Status.TRASHED);

        query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.TRASHED);

        return endQuery("Delete study", startTime, get(query, null));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    QueryResult<Study> setStatus(long studyId, String status) throws CatalogDBException {
        return update(studyId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_NAME.key(), Status.READY);
        QueryResult<Study> studyQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Study study : studyQueryResult.getResult()) {
            delete(study.getId(), queryOptions);
        }
        return endQuery("Delete study", startTime, Collections.singletonList(studyQueryResult.getNumTotalResults()));
    }

    /**
     * Checks whether the studyId has any active document in the study.
     *
     * @param studyId study id.
     * @throws CatalogDBException when the study has active documents.
     */
    private void checkCanDelete(long studyId) throws CatalogDBException {
        checkId(studyId);
        Query query = new Query(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);

        Long count = dbAdaptorFactory.getCatalogFileDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
                    + " files in use.");
        }
        count = dbAdaptorFactory.getCatalogJobDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
                    + " jobs in use.");
        }
        count = dbAdaptorFactory.getCatalogSampleDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
                    + " samples in use.");
        }
        count = dbAdaptorFactory.getCatalogIndividualDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
                    + " individuals in use.");
        }
        count = dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
                    + " cohorts in use.");
        }
        count = dbAdaptorFactory.getCatalogDatasetDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
                    + " datasets in use.");
        }
    }

    /**
     * Checks if the study is empty or has more active information.
     *
     * @param studyId Id of the study.
     * @throws CatalogDBException when there exists active files, samples, cohorts...
     */
    private void checkEmptyStudy(long studyId) throws CatalogDBException {
        Query query = new Query(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);

        // Check files
        if (dbAdaptorFactory.getCatalogFileDBAdaptor().count(query).first() > 0) {
            throw new CatalogDBException("Cannot delete study " + studyId + ". There are files being used.");
        }

        // Check samples
        if (dbAdaptorFactory.getCatalogSampleDBAdaptor().count(query).first() > 0) {
            throw new CatalogDBException("Cannot delete study " + studyId + ". There are samples being used.");
        }

        // Check individuals
        if (dbAdaptorFactory.getCatalogIndividualDBAdaptor().count(query).first() > 0) {
            throw new CatalogDBException("Cannot delete study " + studyId + ". There are individuals being used.");
        }

        // Check cohorts
        if (dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).first() > 0) {
            throw new CatalogDBException("Cannot delete study " + studyId + ". There are cohorts being used.");
        }
    }

    @Override
    public QueryResult<Study> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore studies", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Study> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The study {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore study", startTime, get(query, null));
    }

    public QueryResult<Study> remove(int studyId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), studyId);
        QueryResult<Study> studyQueryResult = get(query, null);
        if (studyQueryResult.getResult().size() == 1) {
            QueryResult<DeleteResult> remove = studyCollection.remove(parseQuery(query, false), null);
            if (remove.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Study id '{}' has not been deleted", studyId);
            }
        } else {
            throw CatalogDBException.idNotFound("Study id '{}' does not exist (or there are too many)", studyId);
        }
        return studyQueryResult;
    }

    @Override
    public QueryResult<Study> get(long studyId, QueryOptions options) throws CatalogDBException {
        checkId(studyId);
        Query query = new Query(QueryParams.ID.key(), studyId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
    }

    @Override
    public QueryResult<Study> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Study> documentList = new ArrayList<>();
        QueryResult<Study> studyQueryResult;
        try (DBIterator<Study> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        studyQueryResult = endQuery("Get", startTime, documentList);
        for (Study study : studyQueryResult.getResult()) {
            joinFields(study, options);
        }
        return studyQueryResult;
    }

    @Override
    public QueryResult<Study> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Study> documentList = new ArrayList<>();
        QueryResult<Study> studyQueryResult;
        try (DBIterator<Study> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        studyQueryResult = endQuery("Get", startTime, documentList);
        for (Study study : studyQueryResult.getResult()) {
            joinFields(study, options, user);
        }
        return studyQueryResult;
    }

    @Override
    public QueryResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery("Native get", startTime, documentList);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery("Native get", startTime, documentList);
    }

    @Override
    public DBIterator<Study> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new StudyMongoDBIterator<>(mongoCursor, studyConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new StudyMongoDBIterator<>(mongoCursor);
    }

    @Override
    public DBIterator<Study> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        Function<Document, Boolean> iteratorFilter = (d) -> checkCanViewStudy(d, user);
        return new StudyMongoDBIterator<>(mongoCursor, studyConverter, iteratorFilter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        Function<Document, Boolean> iteratorFilter = (d) -> checkCanViewStudy(d, user);
        return new StudyMongoDBIterator<Document>(mongoCursor, iteratorFilter);
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (options.containsKey(QueryOptions.INCLUDE)) {
            options = new QueryOptions(options);
            List<String> includeList = new ArrayList<>(options.getAsStringList(QueryOptions.INCLUDE));
            includeList.add("_ownerId");
            includeList.add("_acl");
            includeList.add(QueryParams.GROUPS.key());
            options.put(QueryOptions.INCLUDE, includeList);
        }
        options = filterOptions(options, FILTER_ROUTE_STUDIES);

        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query, false);

        logger.debug("Study native get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        return studyCollection.nativeQuery().find(bson, options).iterator();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return rank(studyCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(studyCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(studyCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Study> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                continue;
            }
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case PROJECT_ID:
                        addOrQuery(PRIVATE_PROJECT_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case NAME:
                    case ALIAS:
                    case DESCRIPTION:
                    case CIPHER:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case LAST_MODIFIED:
                    case DATASTORES:
                    case SIZE:
                    case URI:
                    case STATS:
                    case TYPE:
                    case GROUPS:
                    case GROUP_NAME:
                    case GROUP_USER_IDS:
                    case RELEASE:
                    case ROLES:
                    case ROLES_ID:
                    case ROLES_USERS:
                    case ROLES_PERMISSIONS:
                    case EXPERIMENT_ID:
                    case EXPERIMENT_NAME:
                    case EXPERIMENT_TYPE:
                    case EXPERIMENT_PLATFORM:
                    case EXPERIMENT_MANUFACTURER:
                    case EXPERIMENT_DATE:
                    case EXPERIMENT_LAB:
                    case EXPERIMENT_CENTER:
                    case EXPERIMENT_RESPONSIBLE:
                    case COHORTS:
                    case VARIABLE_SET:
                    case VARIABLE_SET_ID:
                    case VARIABLE_SET_NAME:
                    case VARIABLE_SET_DESCRIPTION:
                    case OWNER:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    public MongoDBCollection getStudyCollection() {
        return studyCollection;
    }

    /***
     * This method is called every time a file has been inserted, modified or deleted to keep track of the current study size.
     *
     * @param studyId   Study Identifier
     * @param size disk usage of a new created, updated or deleted file belonging to studyId. This argument
     *                  will be > 0 to increment the size field in the study collection or < 0 to decrement it.
     * @throws CatalogDBException An exception is launched when the update crashes.
     */
    public void updateDiskUsage(long studyId, long size) throws CatalogDBException {
        Bson query = new Document(QueryParams.ID.key(), studyId);
        Bson update = Updates.inc(QueryParams.SIZE.key(), size);
        if (studyCollection.update(query, update, null).getNumTotalResults() == 0) {
            throw new CatalogDBException("CatalogMongoStudyDBAdaptor updateDiskUsage: Couldn't update the size field of"
                    + " the study " + studyId);
        }
    }
}
