/*
 * Copyright 2015-2016 OpenCB
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

import com.mongodb.WriteResult;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
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
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    /*
     * Study methods
     * ***************************
     */

    //    @Override
//    public boolean studyExists(int studyId) {
//        QueryResult<Long> count = studyCollection.count(new BasicDBObject(PRIVATE_ID, studyId));
//        return count.getResult().get(0) != 0;
//    }
//
//    @Override
//    public void checkStudyId(int studyId) throws CatalogDBException {
//        if (!studyExists(studyId)) {
//            throw CatalogDBException.idNotFound("Study", studyId);
//        }
//    }
    private boolean studyAliasExists(long projectId, String studyAlias) throws CatalogDBException {
        if (projectId < 0) {
            throw CatalogDBException.newInstance("Project id '{}' is not valid: ", projectId);
        }

        Query query = new Query(QueryParams.PROJECT_ID.key(), projectId).append("alias", studyAlias);
        QueryResult<Long> count = count(query);
        return count.first() != 0;
    }

    @Override
    public QueryResult<Study> insert(long projectId, Study study, QueryOptions options) throws CatalogDBException {
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

        //Insert
        QueryResult<WriteResult> updateResult = studyCollection.insert(studyObject, null);

        //Check if the the study has been inserted
//        if (updateResult.getResult().get(0).getN() == 0) {
//            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
//        }

        // Insert nested fields
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
    public QueryResult<Study> get(long studyId, QueryOptions options) throws CatalogDBException {
        checkId(studyId);
        return get(new Query(QueryParams.ID.key(), studyId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED), options);
    }

    @Override
    public QueryResult<Study> getAllStudiesInProject(long projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogProjectDbAdaptor().checkId(projectId);
        Query query = new Query(QueryParams.PROJECT_ID.key(), projectId);
        return endQuery("getAllSudiesInProject", startTime, get(query, options));
    }

    @Override
    public void updateStudyLastModified(long studyId) throws CatalogDBException {
        update(studyId, new ObjectMap("lastModified", TimeUtils.getTime()));
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
        return dbAdaptorFactory.getCatalogProjectDbAdaptor().getOwnerId(getProjectIdByStudyId(studyId));
    }

    @Override
    public QueryResult<Group> createGroup(long studyId, String groupId, List<String> userIds) throws CatalogDBException {
        long startTime = startQuery();

        Group group = new Group(groupId, userIds);
        Document query = new Document(PRIVATE_ID, studyId);
        Document update = new Document("$push", new Document(QueryParams.GROUPS.key(), getMongoDBDocument(group, "Group")));

        QueryResult<UpdateResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to create the group " + groupId);
        }

        return endQuery("Create group", startTime, getGroup(studyId, groupId, Collections.emptyList()));
    }


    private long getDiskUsageByStudy(int studyId) {
        /*
        List<DBObject> operations = Arrays.<DBObject>asList(
                new BasicDBObject(
                        "$match",
                        new BasicDBObject(
                                PRIVATE_STUDY_ID,
                                studyId
                                //new BasicDBObject("$in",studyIds)
                        )
                ),
                new BasicDBObject(
                        "$group",
                        BasicDBObjectBuilder
                                .start("_id", "$" + PRIVATE_STUDY_ID)
                                .append("size",
                                        new BasicDBObject(
                                                "$sum",
                                                "$diskUsage"
                                        )).get()
                )
        );*/
        List<Bson> operations = new ArrayList<>();
        operations.add(Aggregates.match(Filters.eq(PRIVATE_STUDY_ID, studyId)));
        operations.add(Aggregates.group("$" + PRIVATE_STUDY_ID, Accumulators.sum("size", "$diskUsage")));

//        Bson match = Aggregates.match(Filters.eq(PRIVATE_STUDY_ID, studyId));
//        Aggregates.group()

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
    public QueryResult<Group> getGroup(long studyId, String userId, String groupName, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Bson query = new Document(PRIVATE_ID, studyId);
        Document groupQuery = new Document();
        if (userId != null) {
            groupQuery.put("userIds", userId);
        }
        if (groupName != null) {
            groupQuery.put("name", groupName);
        }
        Bson projection = new Document(QueryParams.GROUPS.key(), new Document("$elemMatch", groupQuery));

        QueryResult<Document> queryResult = studyCollection.find(query, projection,
                filterOptions(options, FILTER_ROUTE_STUDIES + QueryParams.GROUPS.key() + "."));
        List<Study> studies = MongoDBUtils.parseStudies(queryResult);
        List<Group> groups = new ArrayList<>(1);
        studies.stream().filter(study -> study.getGroups() != null).forEach(study -> groups.addAll(study.getGroups()));
        return endQuery("getGroup", startTime, groups);
    }

    @Override
    public QueryResult<Group> getGroup(long studyId, @Nullable String groupId, List<String> userIds) throws CatalogDBException {
        long startTime = startQuery();
        checkId(studyId);
        for (String userId : userIds) {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkId(userId);
        }
        if (groupId != null && groupId.length() > 0 && !groupExists(studyId, groupId)) {
            throw new CatalogDBException("Group \"" + groupId + "\" does not exist in study " + studyId);
        }

        /*
        * List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.elemMatch(QueryParams.VARIABLE_SET.key(),
                Filters.eq(VariableSetParams.ID.key(), variableSetId))));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.VARIABLE_SET.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.VARIABLE_SET.key()));
        aggregation.add(Aggregates.match(Filters.eq(QueryParams.VARIABLE_SET_ID.key(), variableSetId)));
        QueryResult<VariableSet> queryResult = studyCollection.aggregate(aggregation, variableSetConverter, new QueryOptions());
        * */
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, studyId)));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.GROUPS.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.GROUPS.key()));

//        Document query = new Document(PRIVATE_ID, studyId);
//        List<Bson> groupQuery = new ArrayList<>();
        if (userIds.size() > 0) {
            aggregation.add(Aggregates.match(Filters.in(QueryParams.GROUP_USER_IDS.key(), userIds)));
//            groupQuery.add(Filters.in("userIds", userIds));
        }
        if (groupId != null && groupId.length() > 0) {
            aggregation.add(Aggregates.match(Filters.eq(QueryParams.GROUP_NAME.key(), groupId)));
//            groupQuery.add(Filters.eq("id", groupId));
        }

//        Bson projection = new Document(QueryParams.GROUPS.key(), new Document("$elemMatch", groupQuery));

        QueryResult<Document> queryResult = studyCollection.aggregate(aggregation, null);

//        QueryResult<Document> queryResult = studyCollection.find(query, projection, null);
        List<Study> studies = MongoDBUtils.parseStudies(queryResult);
        List<Group> groups = new ArrayList<>();
        studies.stream().filter(study -> study.getGroups() != null).forEach(study -> groups.addAll(study.getGroups()));
        return endQuery("getGroup", startTime, groups);
    }

    @Override
    public QueryResult<Group> setUsersToGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();

        // Check that the members exist.
        for (String member : members) {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkId(member);
        }

        // Check that the members do not belong to other group.
        List<Group> result = getGroup(studyId, null, members).getResult();
        if (result.size() > 0) {
            Set<String> usersSet = new HashSet<>(members.size());
            usersSet.addAll(members.stream().collect(Collectors.toList()));

            for (Group group : result) {
                // Remove the members that already existed in other groups different than the one to be set.
                if (!group.getName().equals(groupId)) {
                    List<String> usersToRemove = new ArrayList<>();
                    for (String userId : group.getUserIds()) {
                        if (usersSet.contains(userId)) {
                            usersToRemove.add(userId);
                        }
                    }
                    if (usersToRemove.size() > 0) {
                        removeUsersFromGroup(studyId, group.getName(), usersToRemove);
                    }
                }
            }
        }

        Document query = new Document(PRIVATE_ID, studyId).append(QueryParams.GROUP_NAME.key(), groupId);
        Document update = new Document("$set", new Document("groups.$.userIds", members));
        QueryResult<UpdateResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to set users to group " + groupId);
        }

        return endQuery("set users to group", startTime, getGroup(studyId, null, groupId, null));
    }

//    @Deprecated
//    @Override
//    public QueryResult<Role> getRole(long studyId, String userId, String groupId, String roleId, QueryOptions options)
//            throws CatalogDBException {
//        long startTime = startQuery();
//
//        Bson query = new Document(PRIVATE_ID, studyId);
//        List<Bson> roleQuery = new ArrayList<>();
//        List<String> userIds = new ArrayList<>();
//        if (userId != null) {
//            userIds.add(userId);
//        }
//        if (groupId != null) {
//            userIds.add(groupId);
//        }
//        if (userIds.size() > 0) {
//            roleQuery.add(Filters.in("userIds", userIds));
//        }
//        if (roleId != null) {
//            roleQuery.add(Filters.eq("id", roleId));
//        }
//        Bson projection = Projections.elemMatch(QueryParams.ROLES.key(), Filters.and(roleQuery));
//
//        QueryResult<Document> queryResult = studyCollection.find(query, projection,
//                filterOptions(options, FILTER_ROUTE_STUDIES + QueryParams.ROLES.key() + "."));
//        List<Study> studies = MongoDBUtils.parseStudies(queryResult);
//        List<Role> roles = new ArrayList<>(1);
////        studies.stream().filter(study -> study.getRoles() != null).forEach(study -> {
////            roles.addAll(study.getRoles());
////        });
//        return endQuery("getRole", startTime, roles);
//    }

    boolean groupExists(long studyId, String groupId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), studyId).append(QueryParams.GROUP_NAME.key(), groupId);
        return count(query).first() == 1;
    }

    @Override
    public QueryResult<Group> addUsersToGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();

        // Check that the members exist.
        for (String member : members) {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkId(member);
        }

        // Check that the members do not belong to other group.
        List<Group> result = getGroup(studyId, null, members).getResult();
        if (result.size() > 0) {
            Set<String> usersSet = new HashSet<>(members.size());
            usersSet.addAll(members.stream().collect(Collectors.toList()));

            for (Group group : result) {
                // Remove the members that already existed in other groups different than the one to be set.
                if (!group.getName().equals(groupId)) {
                    List<String> usersToRemove = new ArrayList<>();
                    for (String userId : group.getUserIds()) {
                        if (usersSet.contains(userId)) {
                            usersToRemove.add(userId);
                        }
                    }
                    if (usersToRemove.size() > 0) {
                        removeUsersFromGroup(studyId, group.getName(), usersToRemove);
                    }
                }
            }
        }

        Document query = new Document(PRIVATE_ID, studyId).append(QueryParams.GROUP_NAME.key(), groupId);
        Document update = new Document("$addToSet", new Document("groups.$.userIds", new Document("$each", members)));
        QueryResult<UpdateResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to add members to group " + groupId + ". Maybe the users already belong to the group?");
        }

        return endQuery("add users to group", startTime, getGroup(studyId, null, groupId, null));
    }

    @Override
    public void removeUsersFromGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        for (String member : members) {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkId(member);
        }

        Bson and = Filters.and(Filters.eq(PRIVATE_ID, studyId), Filters.eq("groups.name", groupId));
        Bson pull = Updates.pullAll("groups.$.userIds", members);
        QueryResult<UpdateResult> update = studyCollection.update(and, pull, null);
        if (update.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to remove members from group " + groupId);
        }
    }

    @Override
    public void deleteGroup(long studyId, String groupId) throws CatalogDBException {
        Bson queryBson = new Document()
                .append(PRIVATE_ID, studyId)
                .append(QueryParams.GROUP_NAME.key(), groupId);
        Document pull = new Document("$pull", new Document("groups", new Document("name", groupId)));
        QueryResult<UpdateResult> update = studyCollection.update(queryBson, pull, null);

        if (update.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Could not remove the group " + groupId);
        }
    }

    @Override
    public void updateSyncFromGroup(long studyId, String groupId, Group.Sync syncedFrom) throws CatalogDBException {
        Document mongoDBDocument = getMongoDBDocument(syncedFrom, "Group.Sync");

        Query query = new Query()
                .append(QueryParams.ID.key(), studyId)
                .append(QueryParams.GROUP_NAME.key(), groupId);

        Document updates = new Document("$set", new Document("groups.$.syncedFrom", mongoDBDocument));
        studyCollection.update(parseQuery(query, true), updates, null);
    }
//
//    @Override
//    @Deprecated
//    public QueryResult<StudyAclEntry> setStudyAcl(long studyId, StudyAclEntry studyAcl, boolean override) throws CatalogDBException {
//        long startTime = startQuery();
//
////        checkStudyId(studyId);
////        // Check that member (users) is correct and exist.
////        checkMember(dbAdaptorFactory, studyId, studyAcl.getMember());
//
//        // If the member is a group, we will obtain all the users pertaining to the groups and will check if any of them already have
//        // a special permission on their own. If this is the case, we will throw an exception.
//        String member = studyAcl.getMember();
//        if (member.startsWith("@")) {
//            Group group = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member, Collections.emptyList()).first();
//
//            QueryResult<StudyAclEntry> aclQueryResult = getAcl(studyId, group.getUserIds());
//            if (aclQueryResult.getNumResults() > 0) {
//                throw new CatalogDBException("The permissions could not be set. At least one user belonging to " + group.getName()
//                        + " already have permissions set on its own.");
//            }
//        } else {
//            QueryResult<StudyAclEntry> studyAcls = getAcl(studyId, Arrays.asList(member));
//
//            // Check if the user already has permissions
//            if (studyAcls.getNumResults() > 0 && override) {
//                unsetStudyAcl(studyId, Arrays.asList(member));
//            } else if (studyAcls.getNumResults() > 0 && !override) {
//                throw new CatalogDBException("setStudyAcl: " + member + " already had an Acl set. If you "
//                        + "still want to set the Acls, please use the override parameter.");
//            }
//        }
//
//        // Push the new acl to the list of acls.
//        Document queryDocument = new Document(PRIVATE_ID, studyId);
//        Document update = new Document("$push", new Document(QueryParams.ACL.key(), getMongoDBDocument(studyAcl, "StudyAcl")));
//        QueryResult<UpdateResult> updateResult = studyCollection.update(queryDocument, update, null);
//
//        if (updateResult.first().getModifiedCount() == 0) {
//            throw new CatalogDBException("setStudyAcl: An error occurred when trying to share study " + studyId
//                    + " with " + member);
//        }
//
//        return endQuery("setStudyAcl", startTime, Arrays.asList(studyAcl));
//    }

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

        Bson bsonQuery = Filters.eq(PRIVATE_ID, studyId);
        Bson update = Updates.push("variableSets", object);
        QueryResult<UpdateResult> queryResult = studyCollection.update(bsonQuery, update, null);

        if (queryResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("createVariableSet: Could not create a new variable set in study " + studyId);
        }

        return endQuery("createVariableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable) throws CatalogDBException {
        long startTime = startQuery();

        checkVariableSetExists(variableSetId);
        checkVariableNotInVariableSet(variableSetId, variable.getName());

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
        }
        return endQuery("Add field to variable set", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> renameFieldVariableSet(long variableSetId, String oldName, String newName) throws CatalogDBException {
        long startTime = startQuery();

        checkVariableSetExists(variableSetId);
        checkVariableInVariableSet(variableSetId, oldName);
        checkVariableNotInVariableSet(variableSetId, newName);

        // The field can be changed if we arrive to this point.
        // 1. we obtain the variable
        Variable variable = getVariable(variableSetId, oldName);

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

        return endQuery("Rename field in variableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name) throws CatalogDBException {
        long startTime = startQuery();

        try {
            checkVariableInVariableSet(variableSetId, name);
        } catch (CatalogDBException e) {
            checkVariableSetExists(variableSetId);
            throw e;
        }
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

        return endQuery("Remove field from Variable Set", startTime, getVariableSet(variableSetId, null));
    }

    /**
     * The method will return the variable object given variableSetId and the variableId.
     * @param variableSetId Id of the variableSet.
     * @param variableId Id of the variable inside the variableSet.
     * @return the variable object.
     */
    private Variable getVariable(long variableSetId, String variableId) throws CatalogDBException {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.elemMatch(QueryParams.VARIABLE_SET.key(),
                Filters.eq(VariableSetParams.ID.key(), variableSetId))));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.VARIABLE_SET.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.VARIABLE_SET.key()));
        aggregation.add(Aggregates.match(Filters.eq(QueryParams.VARIABLE_SET_ID.key(), variableSetId)));
        aggregation.add(Aggregates.unwind("$" + QueryParams.VARIABLE_SET.key() + "." + VariableSetParams.VARIABLE.key()));
        aggregation.add(Aggregates.match(
                Filters.eq(QueryParams.VARIABLE_SET.key() + "." + VariableSetParams.VARIABLE_NAME.key(), variableId)));

        QueryResult<Document> queryResult = studyCollection.aggregate(aggregation, new QueryOptions());

        Document variableSetDocument = (Document) queryResult.first().get(QueryParams.VARIABLE_SET.key());
        VariableSet variableSet = variableSetConverter.convertToDataModelType(variableSetDocument);
        Iterator<Variable> iterator = variableSet.getVariables().iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            // This error should never be raised.
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - Could not obtain variable object.");
        }
    }

    /**
     * Checks if the variable given is present in the variableSet.
     * @param variableSetId Identifier of the variableSet where it will be checked.
     * @param variableId VariableId that will be checked.
     * @throws CatalogDBException when the variableId is not present in the variableSet.
     */
    private void checkVariableInVariableSet(long variableSetId, String variableId) throws CatalogDBException {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.elemMatch(QueryParams.VARIABLE_SET.key(), Filters.and(
                Filters.eq(VariableSetParams.ID.key(), variableSetId),
                Filters.eq(VariableSetParams.VARIABLE_NAME.key(), variableId))
        )));

        if (studyCollection.aggregate(aggregation, new QueryOptions()).getNumResults() == 0) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "}. The variable {id: " + variableId + "} does not exist.");
        }
    }

    /**
     * Checks if the variable given is not present in the variableSet.
     * @param variableSetId Identifier of the variableSet where it will be checked.
     * @param variableId VariableId that will be checked.
     * @throws CatalogDBException when the variableId is present in the variableSet.
     */
    private void checkVariableNotInVariableSet(long variableSetId, String variableId) throws CatalogDBException {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.elemMatch(QueryParams.VARIABLE_SET.key(), Filters.and(
                Filters.eq(VariableSetParams.ID.key(), variableSetId),
                Filters.ne(VariableSetParams.VARIABLE_NAME.key(), variableId))
        )));

        if (studyCollection.aggregate(aggregation, new QueryOptions()).getNumResults() == 0) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "}. The variable {id: " + variableId + "} already exists.");
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

/*
        Bson query = Filters.eq("variableSets.id", variableSetId);
        Bson projection = Projections.elemMatch("variableSets", Filters.eq("id", variableSetId));
        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_STUDIES);

        QueryResult<Document> queryResult = studyCollection.find(query, projection, filteredOptions);
        List<Study> studies = parseStudies(queryResult);
        if (studies.isEmpty() || studies.get(0).getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist");
        }
*/
        return endQuery("", startTime, studyQueryResult.first().getVariableSets());
    }

    @Override
    public QueryResult<VariableSet> getVariableSets(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        List<Bson> mongoQueryList = new LinkedList<>();
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
            aggregation.add(Aggregates.match(Filters.and(mongoQueryList)));
        }

        QueryResult<Document> queryResult = studyCollection.aggregate(aggregation, filterOptions(queryOptions, FILTER_ROUTE_STUDIES));

        List<VariableSet> variableSets = parseObjects(queryResult, Study.class).stream().map(study -> study.getVariableSets().get(0))
                .collect(Collectors.toList());

        return endQuery("", startTime, variableSets);
    }

    @Override
    public QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkVariableSetInUse(variableSetId);
        long studyId = getStudyIdByVariableSetId(variableSetId);
        QueryResult<VariableSet> variableSet = getVariableSet(variableSetId, queryOptions);
        Bson query = Filters.eq(PRIVATE_ID, studyId);
        Bson operation = Updates.pull("variableSets", Filters.eq("id", variableSetId));
        QueryResult<UpdateResult> update = studyCollection.update(query, operation, null);

        if (update.first().getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
        return endQuery("Delete VariableSet", startTime, variableSet);
    }


    public void checkVariableSetInUse(long variableSetId) throws CatalogDBException {
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(
                new Query(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId), new QueryOptions());
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of samples : [";
            for (Sample sample : samples.getResult()) {
                msg += " { id: " + sample.getId() + ", name: \"" + sample.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Individual> individuals = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(
                new Query(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId), new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of individuals : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor().get(
                new Query(CohortDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId), new QueryOptions());
        if (cohorts.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of samples : [";
            for (Cohort cohort : cohorts.getResult()) {
                msg += " { id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" },";
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
        long studyId = study.getId();
        if (studyId <= 0 || options == null) {
            return;
        }

        if (options.getBoolean("includeFiles")) {
            study.setFiles(dbAdaptorFactory.getCatalogFileDBAdaptor().getAllInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeJobs")) {
            study.setJobs(dbAdaptorFactory.getCatalogJobDBAdaptor().getAllInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeSamples")) {
            study.setSamples(dbAdaptorFactory.getCatalogSampleDBAdaptor().getAllInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeIndividuals")) {
            study.setIndividuals(dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(
                    new Query(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), options).getResult());
        }
    }


    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return studyCollection.count(bson);
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
    public QueryResult<Study> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query, false);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        qOptions = filterOptions(qOptions, FILTER_ROUTE_STUDIES);
        QueryResult<Study> result = studyCollection.find(bson, studyConverter, qOptions);
        for (Study study : result.getResult()) {
            joinFields(study, options);
        }
        return endQuery("Get study", startTime, result.getResult());
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query, false);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_STUDIES);
        // Fixme: If necessary, include in the results also the files, jobs, individuals...
        return studyCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
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
    public QueryResult<Study> update(long id, ObjectMap parameters) throws CatalogDBException {

        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters);
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
//            dbAdaptorFactory.getCatalogFileDBAdaptor().delete(query, queryOptions);
//            dbAdaptorFactory.getCatalogJobDBAdaptor().delete(query, queryOptions);
//            dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(query, queryOptions);
//            dbAdaptorFactory.getCatalogIndividualDBAdaptor().delete(query, queryOptions);
//            dbAdaptorFactory.getCatalogCohortDBAdaptor().delete(query, queryOptions);
//            dbAdaptorFactory.getCatalogDatasetDBAdaptor().delete(query, queryOptions);
        }

        // Change the status of the project to deleted
        setStatus(id, Status.TRASHED);

        query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.TRASHED);

        return endQuery("Delete study", startTime, get(query, null));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    QueryResult<Study> setStatus(long studyId, String status) throws CatalogDBException {
        return update(studyId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
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
//        long startTime = startQuery();
//        query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
//
//        if (queryOptions == null) {
//            queryOptions = new QueryOptions();
//        }
//
//        List<Study> studies = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key())).getResult();
//        for (Study study : studies) {
//            try {
//                if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
//                    checkEmptyStudy(study.getId());
//                }
//            } catch ()
//
//            boolean success = true;
//
//            // Try to remove all the samples
//            if (study.getSamples().size() > 0) {
//                List<Long> sampleIds = new ArrayList<>(study.getSamples().size());
//                sampleIds.addAll(study.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
//                try {
//                    Long nDeleted = dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(
//                            new Query(CatalogSampleDBAdaptor.QueryParams.ID.key(), sampleIds), , force).first();
//                    if (nDeleted != sampleIds.size()) {
//                        success = false;
//                    }
//                } catch (CatalogDBException e) {
//                    logger.info("Delete Study: " + e.getMessage());
//                }
//            }
//
//            // Try to remove all the jobs.
//            if (study.getJobs().size() > 0) {
//                List<Long> jobIds = new ArrayList<>(study.getJobs().size());
//                jobIds.addAll(study.getJobs().stream().map(Job::getId).collect(Collectors.toList()));
//                try {
//                    Long nDeleted = dbAdaptorFactory.getCatalogJobDBAdaptor().delete(
//                            new Query(CatalogJobDBAdaptor.QueryParams.ID.key(), jobIds), , force).first();
//                    if (nDeleted != jobIds.size()) {
//                        success = false;
//                    }
//                } catch (CatalogDBException e) {
//                    logger.info("Delete Study: " + e.getMessage());
//                }
//            }
//
//            // Try to remove all the files.
//            if (study.getFiles().size() > 0) {
//                List<Long> fileIds = new ArrayList<>(study.getFiles().size());
//                fileIds.addAll(study.getFiles().stream().map((Function<File, Long>) File::getId).collect(Collectors.toList()));
//                try {
//                    Long nDeleted = dbAdaptorFactory.getCatalogFileDBAdaptor().delete(
//                            new Query(CatalogFileDBAdaptor.QueryParams.ID.key(), fileIds), , force).first();
//                    if (nDeleted != fileIds.size()) {
//                        success = false;
//                    }
//                } catch (CatalogDBException e) {
//                    logger.info("Delete Study: " + e.getMessage());
//                }
//            }
//
//            if (success || force) {
//                if (!success && force) {
//                    logger.error("Delete study: Force was true and success was false. This should not be happening.");
//                }
//                studiesToRemove.add(study.getId());
//            }
//        }
//
//        if (studiesToRemove.size() == 0) {
//            throw CatalogDBException.deleteError("Study");
//        }
//
//        Query queryDelete = new Query(QueryParams.ID.key(), studiesToRemove)
//                .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";" + Status.DELETED);
//        QueryResult<UpdateResult> deleted = studyCollection.update(parseQuery(queryDelete), Updates.combine(
//                Updates.set(CatalogUserDBAdaptor.QueryParams.STATUS_NAME.key(), Status.TRASHED),
//                Updates.set(CatalogUserDBAdaptor.QueryParams.STATUS_DATE.key(), TimeUtils.getTimeMillis())),
//                new QueryOptions());
//
//        if (deleted.first().getModifiedCount() == 0) {
//            throw CatalogDBException.deleteError("Delete");
//        } else {
//            return endQuery("Delete study", startTime, Collections.singletonList(deleted.first().getModifiedCount()));
//        }

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
    public DBIterator<Study> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        MongoCursor<Document> iterator = studyCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, studyConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        MongoCursor<Document> iterator = studyCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
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
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        DBIterator<Study> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

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
                    case NAME:
                    case ALIAS:
                    case CREATION_DATE:
                    case DESCRIPTION:
                    case CIPHER:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case LAST_MODIFIED:
                    case DATASTORES:
                    case SIZE:
                    case URI:
                    case ACL:
                    case ACL_MEMBER:
                    case ACL_PERMISSIONS:
                    case STATS:
                    case TYPE:
                    case GROUPS:
                    case GROUP_NAME:
                    case GROUP_USER_IDS:
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
