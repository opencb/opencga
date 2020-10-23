/*
 * Copyright 2015-2020 OpenCB
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
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.StudyConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.VariableSetConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.StudyCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.response.OpenCGAResult;
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
    private final MongoDBCollection deletedStudyCollection;
    private StudyConverter studyConverter;
    private VariableSetConverter variableSetConverter;

    public StudyMongoDBAdaptor(MongoDBCollection studyCollection, MongoDBCollection deletedStudyCollection,
                               MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(StudyMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.studyCollection = studyCollection;
        this.deletedStudyCollection = deletedStudyCollection;
        this.studyConverter = new StudyConverter();
        this.variableSetConverter = new VariableSetConverter();
    }

    public void checkId(ClientSession clientSession, long studyId) throws CatalogDBException {
        if (studyId < 0) {
            throw CatalogDBException.newInstance("Study id '{}' is not valid: ", studyId);
        }
        Long count = count(clientSession, new Query(QueryParams.UID.key(), studyId)).getNumMatches();
        if (count <= 0) {
            throw CatalogDBException.newInstance("Study id '{}' does not exist", studyId);
        } else if (count > 1) {
            throw CatalogDBException.newInstance("'{}' documents found with the Study id '{}'", count, studyId);
        }
    }

    private boolean studyIdExists(ClientSession clientSession, long projectId, String studyId) throws CatalogDBException {
        if (projectId < 0) {
            throw CatalogDBException.newInstance("Project id '{}' is not valid: ", projectId);
        }

        Query query = new Query(QueryParams.PROJECT_ID.key(), projectId).append(QueryParams.ID.key(), studyId);
        OpenCGAResult<Long> count = count(clientSession, query);
        return count.getNumMatches() != 0;
    }

    @Override
    public OpenCGAResult<Study> nativeInsert(Map<String, Object> study, String userId) throws CatalogDBException {
        Document studyDocument = getMongoDBDocument(study, "study");
        studyDocument.put(PRIVATE_OWNER_ID, userId);
        return new OpenCGAResult<>(studyCollection.insert(studyDocument, null));
    }

    @Override
    public OpenCGAResult<Study> insert(Project project, Study study, QueryOptions options) throws CatalogDBException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting study insert transaction for study id '{}'", study.getId());

                insert(clientSession, project, study);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create study {}: {}", study.getId(), e.getMessage());
            throw new CatalogDBException(e);
        }
    }

//    @Override
//    public OpenCGAResult<Study> insert(Project project, Study study, QueryOptions options) throws CatalogDBException {
//        ClientSession clientSession = getClientSession();
//        TransactionBody<OpenCGAResult> txnBody = () -> {
//            long tmpStartTime = startQuery();
//            logger.debug("Starting study insert transaction for study id '{}'", study.getId());
//
//            try {
//                insert(clientSession, project, study);
//                return endWrite(tmpStartTime, 1, 1, 0, 0, null, null);
//            } catch (CatalogDBException e) {
//                throw new CatalogDBRuntimeException("Could not create study " + study.getId(), e);
//            }
//        };
//
//        try {
//            return commitTransaction(clientSession, txnBody);
//        } catch (CatalogDBRuntimeException e) {
//            logger.error("Could not create study {}: {}", study.getId(), e.getMessage());
//            throw new CatalogDBException(e);
//        }
//    }

    Study insert(ClientSession clientSession, Project project, Study study)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (project.getUid() < 0) {
            throw CatalogDBException.uidNotFound("Project", project.getUid());
        }
        if (StringUtils.isEmpty(project.getId())) {
            throw CatalogDBException.idNotFound("Project", project.getId());
        }

        // Check if study.id already exists.
        if (studyIdExists(clientSession, project.getUid(), study.getId())) {
            throw new CatalogDBException("Study {id:\"" + study.getId() + "\"} already exists");
        }

        //Set new ID
        long studyUid = getNewUid();
        study.setUid(studyUid);

        if (StringUtils.isEmpty(study.getUuid())) {
            study.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.STUDY));
        }
        if (StringUtils.isEmpty(study.getCreationDate())) {
            study.setCreationDate(TimeUtils.getTime());
        }

        //Empty nested fields
        List<File> files = study.getFiles();
        study.setFiles(Collections.emptyList());

        List<Job> jobs = study.getJobs();
        study.setJobs(Collections.emptyList());

        List<Cohort> cohorts = study.getCohorts();
        study.setCohorts(Collections.emptyList());

        List<Panel> panels = study.getPanels();
        study.setPanels(Collections.emptyList());

        List<Family> families = study.getFamilies();
        study.setFamilies(Collections.emptyList());

        study.setFqn(project.getFqn() + ":" + study.getId());

        //Create DBObject
        Document studyObject = studyConverter.convertToStorageType(study);
        studyObject.put(PRIVATE_UID, studyUid);

        //Set ProjectId
        studyObject.put(PRIVATE_PROJECT, new Document()
                .append(PRIVATE_ID, project.getId())
                .append(PRIVATE_UID, project.getUid())
                .append(PRIVATE_UUID, project.getUuid())
        );
        studyObject.put(PRIVATE_OWNER_ID, StringUtils.split(project.getFqn(), "@")[0]);

        studyObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(study.getCreationDate()));
        studyObject.put(PRIVATE_MODIFICATION_DATE, studyObject.get(PRIVATE_CREATION_DATE));

        studyCollection.insert(clientSession, studyObject, null);

        for (File file : files) {
            dbAdaptorFactory.getCatalogFileDBAdaptor().insert(clientSession, study.getUid(), file, Collections.emptyList(),
                    Collections.emptyList(), Collections.emptyList());
        }

        for (Job job : jobs) {
            dbAdaptorFactory.getCatalogJobDBAdaptor().insert(clientSession, study.getUid(), job);
        }

        for (Cohort cohort : cohorts) {
            dbAdaptorFactory.getCatalogCohortDBAdaptor().insert(clientSession, study.getUid(), cohort, Collections.emptyList());
        }

        for (Panel panel : panels) {
            dbAdaptorFactory.getCatalogPanelDBAdaptor().insert(clientSession, study.getUid(), panel);
        }

        for (Family family : families) {
            dbAdaptorFactory.getCatalogFamilyDBAdaptor().insert(clientSession, study.getUid(), family, Collections.emptyList());
        }

        return study;
    }

    @Override
    public OpenCGAResult<Study> getAllStudiesInProject(long projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogProjectDbAdaptor().checkId(projectId);
        Query query = new Query(QueryParams.PROJECT_ID.key(), projectId);
        return endQuery(startTime, get(query, options));
    }

    @Override
    public boolean hasStudyPermission(long studyId, String user, StudyAclEntry.StudyPermissions permission) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), studyId);
        OpenCGAResult queryResult = nativeGet(query, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + studyId + " not found");
        }

        return checkStudyPermission((Document) queryResult.first(), user, permission.name());
    }

    @Override
    public long getId(long projectId, String studyAlias) throws CatalogDBException {
        Query query1 = new Query(QueryParams.PROJECT_ID.key(), projectId).append(QueryParams.ID.key(), studyAlias);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.UID.key());
        OpenCGAResult<Study> studyDataResult = get(query1, queryOptions);
        List<Study> studies = studyDataResult.getResults();
        return studies == null || studies.isEmpty() ? -1 : studies.get(0).getUid();
    }

    @Override
    public long getProjectUidByStudyUid(long studyUid) throws CatalogDBException {
        Document privateProjet = getPrivateProject(studyUid);
        Object id = privateProjet.get(PRIVATE_UID);
        return id instanceof Number ? ((Number) id).longValue() : Long.parseLong(id.toString());
    }

    @Override
    public String getProjectIdByStudyUid(long studyUid) throws CatalogDBException {
        Document privateProjet = getPrivateProject(studyUid);
        return privateProjet.getString(PRIVATE_ID);
    }

    int getCurrentRelease(ClientSession clientSession, long studyUid) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), studyUid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.FQN.key());
        OpenCGAResult<Study> studyResult = get(clientSession, query, options);

        if (studyResult.getNumResults() == 0) {
            throw new CatalogDBException("Study uid '" + studyUid + "' not found.");
        }

        String[] split = StringUtils.split(StringUtils.split(studyResult.first().getFqn(), ":")[0], "@");
        String userId = split[0];
        String projectId = split[1];

        query = new Query()
                .append(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId)
                .append(ProjectDBAdaptor.QueryParams.ID.key(), projectId);
        options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key());
        OpenCGAResult<Project> projectResult = dbAdaptorFactory.getCatalogProjectDbAdaptor().get(clientSession, query, options);
        if (projectResult.getNumResults() == 0) {
            throw new CatalogDBException("Project id '" + projectId + "' from user '" + userId + "' not found.");
        }

        return projectResult.first().getCurrentRelease();
    }

    private Document getPrivateProject(long studyUid) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), studyUid);
        QueryOptions queryOptions = new QueryOptions("include", FILTER_ROUTE_STUDIES + PRIVATE_PROJECT_UID);
        OpenCGAResult result = nativeGet(query, queryOptions);

        Document privateProjet;
        if (!result.getResults().isEmpty()) {
            Document study = (Document) result.getResults().get(0);
            privateProjet = study.get(PRIVATE_PROJECT, Document.class);
        } else {
            throw CatalogDBException.uidNotFound("Study", studyUid);
        }
        return privateProjet;
    }

    @Override
    public String getOwnerId(long studyId) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), studyId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, PRIVATE_OWNER_ID);
        OpenCGAResult<Document> documentDataResult = nativeGet(query, options);
        if (documentDataResult.getNumResults() == 0) {
            throw CatalogDBException.uidNotFound("Study", studyId);
        }
        return documentDataResult.first().getString(PRIVATE_OWNER_ID);
    }

    @Override
    public OpenCGAResult<Study> createGroup(long studyId, Group group) throws CatalogDBException {
        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.GROUP_ID.key(), new Document("$ne", group.getId()));
        Document update = new Document("$push", new Document(QueryParams.GROUPS.key(), getMongoDBDocument(group, "Group")));

        DataResult result = studyCollection.update(query, update, null);

        if (result.getNumUpdated() != 1) {
            OpenCGAResult<Group> group1 = getGroup(studyId, group.getId(), Collections.emptyList());
            if (group1.getNumResults() > 0) {
                throw new CatalogDBException("Unable to create the group " + group.getId() + ". Group already existed.");
            } else {
                throw new CatalogDBException("Unable to create the group " + group.getId() + ".");
            }
        }
        return new OpenCGAResult<>(result);
    }

    @Override
    public OpenCGAResult<Group> getGroup(long studyId, @Nullable String groupId, List<String> userIds) throws CatalogDBException {
        long startTime = startQuery();
        checkId(studyId);

        if (userIds == null) {
            userIds = Collections.emptyList();
        }

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_UID, studyId)));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.GROUPS.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.GROUPS.key()));

        if (userIds.size() > 0) {
            aggregation.add(Aggregates.match(Filters.in(QueryParams.GROUP_USER_IDS.key(), userIds)));
        }
        if (groupId != null && groupId.length() > 0) {
            aggregation.add(Aggregates.match(Filters.eq(QueryParams.GROUP_ID.key(), groupId)));
        }

        DataResult<Document> queryResult = studyCollection.aggregate(aggregation, null);

        List<Study> studies = MongoDBUtils.parseStudies(queryResult);
        List<Group> groups = new ArrayList<>();
        studies.stream().filter(study -> study.getGroups() != null).forEach(study -> groups.addAll(study.getGroups()));
        return endQuery(startTime, groups);
    }

    @Override
    public OpenCGAResult<Group> setUsersToGroup(long studyId, String groupId, List<String> members)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (members == null) {
            members = Collections.emptyList();
        }

        // Check that the members exist.
        if (members.size() > 0) {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkIds(members);
        }

        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.GROUP_ID.key(), groupId);
        Document update = new Document("$set", new Document("groups.$.userIds", members));
        DataResult result = studyCollection.update(query, update, null);

        if (result.getNumMatches() != 1) {
            throw new CatalogDBException("Unable to set users to group " + groupId + ". The group does not exist.");
        }
        return new OpenCGAResult<>(result);
    }

    void addUsersToGroup(long studyId, String groupId, List<String> members, ClientSession clientSession) throws CatalogDBException {
        if (ListUtils.isEmpty(members)) {
            return;
        }

        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.GROUP_ID.key(), groupId);
        Document update = new Document("$addToSet", new Document("groups.$.userIds", new Document("$each", members)));
        DataResult result = studyCollection.update(clientSession, query, update, null);

        if (result.getNumMatches() != 1) {
            throw new CatalogDBException("Unable to add members to group " + groupId + ". The group does not exist.");
        }
    }

    @Override
    public OpenCGAResult<Group> addUsersToGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        if (ListUtils.isEmpty(members)) {
            throw new CatalogDBException("List of 'members' is missing or empty.");
        }

        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.GROUP_ID.key(), groupId);
        Document update = new Document("$addToSet", new Document("groups.$.userIds", new Document("$each", members)));
        DataResult result = studyCollection.update(query, update, null);

        if (result.getNumMatches() != 1) {
            throw new CatalogDBException("Unable to add members to group " + groupId + ". The group does not exist.");
        }
        return new OpenCGAResult<>(result);
    }

    @Override
    public OpenCGAResult<Group> removeUsersFromGroup(long studyId, String groupId, List<String> members) throws CatalogDBException {
        if (members == null || members.size() == 0) {
            throw new CatalogDBException("Unable to remove members from group. List of members is empty");
        }

        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.GROUP_ID.key(), groupId);
        Bson pull = Updates.pullAll("groups.$.userIds", members);
        DataResult update = studyCollection.update(query, pull, null);
        if (update.getNumMatches() != 1) {
            throw new CatalogDBException("Unable to remove members from group " + groupId + ". The group does not exist.");
        }
        return new OpenCGAResult<>(update);
    }

    @Override
    public OpenCGAResult<Group> removeUsersFromAllGroups(long studyId, List<String> users)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (users == null || users.size() == 0) {
            throw new CatalogDBException("Unable to remove users from groups. List of users is empty");
        }

        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Removing list of users '{}' from all groups from study '{}'", users, studyId);

                Document query = new Document()
                        .append(PRIVATE_UID, studyId)
                        .append(QueryParams.GROUP_USER_IDS.key(), new Document("$in", users));
                Bson pull = Updates.pullAll("groups.$.userIds", users);

                // Pull those users while they are still there
                DataResult update;
                do {
                    update = studyCollection.update(clientSession, query, pull, null);
                } while (update.getNumUpdated() > 0);

                return endWrite(tmpStartTime, -1, -1, null);
            });
        } catch (Exception e) {
            logger.error("Could not remove users from all groups of the study. {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Group> deleteGroup(long studyId, String groupId) throws CatalogDBException {
        Bson queryBson = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.GROUP_ID.key(), groupId);
        Document pull = new Document("$pull", new Document("groups", new Document("id", groupId)));
        DataResult result = studyCollection.update(queryBson, pull, null);

        if (result.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not remove the group " + groupId);
        }
        return new OpenCGAResult<>(result);
    }

    @Override
    public OpenCGAResult<Group> syncGroup(long studyId, String groupId, Group.Sync syncedFrom) throws CatalogDBException {
        Document mongoDBDocument = getMongoDBDocument(syncedFrom, "Group.Sync");

        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.GROUP_ID.key(), groupId);
        Document updates = new Document("$set", new Document("groups.$.syncedFrom", mongoDBDocument));
        return new OpenCGAResult<>(studyCollection.update(query, updates, null));
    }

    // TODO: Make this transactional
    @Override
    public OpenCGAResult<Group> resyncUserWithSyncedGroups(String user, List<String> groupList, String authOrigin)
            throws CatalogDBException {
        if (StringUtils.isEmpty(user)) {
            throw new CatalogDBException("Missing user field");
        }

        // 1. Take the user out from all synced groups
        Document query = new Document()
                .append(QueryParams.GROUPS.key(), new Document("$elemMatch", new Document()
                        .append("userIds", user)
                        .append("syncedFrom.authOrigin", authOrigin)
                ));
        Bson pull = Updates.pull("groups.$.userIds", user);

        // Pull the user while it still belongs to a synced group
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        DataResult update;
        do {
            update = studyCollection.update(query, pull, multi);
        } while (update.getNumUpdated() > 0);

        // 2. Add user to all synced groups
        if (groupList != null && groupList.size() > 0) {
            // Add the user to all the synced groups matching
            query = new Document()
                    .append(QueryParams.GROUPS.key(), new Document("$elemMatch", new Document()
                            .append("userIds", new Document("$ne", user))
                            .append("syncedFrom.remoteGroup", new Document("$in", groupList))
                            .append("syncedFrom.authOrigin", authOrigin)
                    ));
            Document push = new Document("$addToSet", new Document("groups.$.userIds", user));
            do {
                update = studyCollection.update(query, push, multi);
            } while (update.getNumUpdated() > 0);

            // We need to be updated with the internal @members group, so we fetch all the studies where the user has been added
            // and attempt to add it to the each @members group
            query = new Document()
                    .append(QueryParams.GROUP_USER_IDS.key(), user)
                    .append(QueryParams.GROUP_SYNCED_FROM_AUTH_ORIGIN.key(), authOrigin);
            DataResult<Study> studyDataResult = studyCollection.find(query, studyConverter, new QueryOptions(QueryOptions.INCLUDE,
                    QueryParams.UID.key()));
            for (Study study : studyDataResult.getResults()) {
                addUsersToGroup(study.getUid(), "@members", Arrays.asList(user));
            }
        }

        return OpenCGAResult.empty();
    }

    @Override
    public OpenCGAResult<PermissionRule> createPermissionRule(long studyId, Enums.Entity entry, PermissionRule permissionRule)
            throws CatalogDBException {
        if (entry == null) {
            throw new CatalogDBException("Missing entry parameter");
        }

        // Get permission rules from study
        OpenCGAResult<PermissionRule> permissionRulesResult = getPermissionRules(studyId, entry);

        List<Document> permissionDocumentList = new ArrayList<>();
        if (permissionRulesResult.getNumResults() > 0) {
            for (PermissionRule rule : permissionRulesResult.getResults()) {
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
        Query query = new Query(QueryParams.UID.key(), studyId);
        Document update = new Document("$set", new Document(QueryParams.PERMISSION_RULES.key() + "." + entry, permissionDocumentList));
        DataResult result = studyCollection.update(parseQuery(query), update, QueryOptions.empty());

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Unexpected error occurred when adding new permission rules to study");
        }
        return new OpenCGAResult<>(result);
    }

    @Override
    public OpenCGAResult<PermissionRule> markDeletedPermissionRule(long studyId, Enums.Entity entry, String permissionRuleId,
                                                                   PermissionRule.DeleteAction deleteAction) throws CatalogDBException {
        if (entry == null) {
            throw new CatalogDBException("Missing entry parameter");
        }

        String newPermissionRuleId = permissionRuleId + INTERNAL_DELIMITER + "DELETE_" + deleteAction.name();

        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(QueryParams.PERMISSION_RULES.key() + "." + entry + ".id", permissionRuleId);
        // Change permissionRule id
        Document update = new Document("$set", new Document(QueryParams.PERMISSION_RULES.key() + "." + entry + ".$.id",
                newPermissionRuleId));

        logger.debug("Mark permission rule for deletion: Query {}, Update {}",
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        DataResult result = studyCollection.update(query, update, QueryOptions.empty());
        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Permission rule " + permissionRuleId + " not found");
        }

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Unexpected error: Permission rule " + permissionRuleId + " could not be marked for deletion");
        }

        return new OpenCGAResult<>(result);
    }

    @Override
    public OpenCGAResult<PermissionRule> getPermissionRules(long studyId, Enums.Entity entry) throws CatalogDBException {
        // Get permission rules from study
        Query query = new Query(QueryParams.UID.key(), studyId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.PERMISSION_RULES.key());

        OpenCGAResult<Study> studyDataResult = get(query, options);
        if (studyDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Unexpected error: Study " + studyId + " not found");
        }

        List<PermissionRule> permissionRules = studyDataResult.first().getPermissionRules().get(entry);
        if (permissionRules == null) {
            permissionRules = Collections.emptyList();
        }

        // Remove all permission rules that are pending of some actions such as deletion
        permissionRules.removeIf(permissionRule ->
                StringUtils.splitByWholeSeparatorPreserveAllTokens(permissionRule.getId(), INTERNAL_DELIMITER, 2).length == 2);

        return new OpenCGAResult<>(studyDataResult.getTime(), Collections.emptyList(), permissionRules.size(), permissionRules,
                permissionRules.size(), new ObjectMap());
    }

    /*
     * Variables Methods
     * ***************************
     */

    @Override
    public Long variableSetExists(long variableSetId) {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.elemMatch(QueryParams.VARIABLE_SET.key(),
                Filters.eq(VariableSetParams.UID.key(), variableSetId))));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.VARIABLE_SET.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.VARIABLE_SET.key()));
        aggregation.add(Aggregates.match(Filters.eq(QueryParams.VARIABLE_SET_UID.key(), variableSetId)));
        DataResult<VariableSet> queryResult = studyCollection.aggregate(aggregation, variableSetConverter, new QueryOptions());

        return (long) queryResult.getResults().size();
    }

    @Override
    public OpenCGAResult<VariableSet> createVariableSet(long studyId, VariableSet variableSet) throws CatalogDBException {
        if (variableSetExists(variableSet.getId(), studyId) > 0) {
            throw new CatalogDBException("VariableSet { name: '" + variableSet.getId() + "'} already exists.");
        }

        long variableSetId = getNewUid();
        variableSet.setUid(variableSetId);
        Document object = getMongoDBDocument(variableSet, "VariableSet");
        object.put(PRIVATE_UID, variableSetId);

        Bson bsonQuery = Filters.eq(PRIVATE_UID, studyId);
        Bson update = Updates.push("variableSets", object);
        DataResult result = studyCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("createVariableSet: Could not create a new variable set in study " + studyId);
        }

        return new OpenCGAResult<>(result);
    }

    @Override
    public OpenCGAResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        OpenCGAResult<VariableSet> variableSet = getVariableSet(variableSetId, new QueryOptions(), user);
        checkVariableNotInVariableSet(variableSet.first(), variable.getId());

        Bson bsonQuery = Filters.eq(QueryParams.VARIABLE_SET_UID.key(), variableSetId);
        Bson update = Updates.push(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(),
                getMongoDBDocument(variable, "variable"));
        DataResult result = studyCollection.update(bsonQuery, update, null);
        if (result.getNumUpdated() == 0) {
            throw CatalogDBException.updateError("VariableSet", variableSetId);
        }
        if (variable.isRequired()) {
            dbAdaptorFactory.getCatalogSampleDBAdaptor().addVariableToAnnotations(variableSetId, variable);
            dbAdaptorFactory.getCatalogCohortDBAdaptor().addVariableToAnnotations(variableSetId, variable);
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().addVariableToAnnotations(variableSetId, variable);
            dbAdaptorFactory.getCatalogFamilyDBAdaptor().addVariableToAnnotations(variableSetId, variable);
            dbAdaptorFactory.getCatalogFileDBAdaptor().addVariableToAnnotations(variableSetId, variable);
        }

        return new OpenCGAResult<>(result);
    }

    @Override
    public OpenCGAResult<VariableSet> renameFieldVariableSet(long variableSetId, String oldName, String newName, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        // TODO
        throw new UnsupportedOperationException("Operation not yet supported");
//        long startTime = startQuery();
//
//        OpenCGAResult<VariableSet> variableSet = getVariableSet(variableSetId, new QueryOptions(), user);
//        checkVariableNotInVariableSet(variableSet.first(), newName);
//
//        // The field can be changed if we arrive to this point.
//        // 1. we obtain the variable
//        Variable variable = getVariable(variableSet.first(), oldName);
//        if (variable == null) {
//            throw new CatalogDBException("VariableSet {id: " + variableSet.getId() + "}. The variable {id: " + oldName + "} does not "
//                    + "exist.");
//        }
//
//        // 2. we take it out from the array.
//        Bson bsonQuery = Filters.eq(QueryParams.VARIABLE_SET_UID.key(), variableSetId);
//        Bson update = Updates.pull(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(),
// Filters.eq("name", oldName));
//        OpenCGAResult<UpdateResult> queryResult = studyCollection.update(bsonQuery, update, null);
//
//        if (queryResult.first().getModifiedCount() == 0) {
//            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - Could not rename the field " + oldName);
//        }
//        if (queryResult.first().getModifiedCount() > 1) {
//            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - An unexpected error happened when extracting the "
//                    + "variable from the variableSet to do the rename. Please, report this error to the OpenCGA developers.");
//        }
//
//        // 3. we change the name in the variable object and push it again in the array.
//        variable.setName(newName);
//        update = Updates.push(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(),
//                getMongoDBDocument(variable, "Variable"));
//        queryResult = studyCollection.update(bsonQuery, update, null);
//
//        if (queryResult.first().getModifiedCount() != 1) {
//            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - A critical error happened when trying to rename one "
//                    + "of the variables of the variableSet object. Please, report this error to the OpenCGA developers.");
//        }
//
//        // 4. Change the field id in the annotations
//        dbAdaptorFactory.getCatalogSampleDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);
//        dbAdaptorFactory.getCatalogCohortDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);
//        dbAdaptorFactory.getCatalogFamilyDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);
//        dbAdaptorFactory.getCatalogIndividualDBAdaptor().renameAnnotationField(variableSetId, oldName, newName);
//
//        return endQuery("Rename field in variableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public OpenCGAResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        OpenCGAResult<VariableSet> variableSet = getVariableSet(variableSetId, new QueryOptions(), user);
        checkVariableInVariableSet(variableSet.first(), name);

        Bson bsonQuery = Filters.eq(QueryParams.VARIABLE_SET_UID.key(), variableSetId);
        Bson update = Updates.pull(QueryParams.VARIABLE_SET.key() + ".$." + VariableSetParams.VARIABLE.key(),
                Filters.eq("id", name));
        DataResult result = studyCollection.update(bsonQuery, update, null);
        if (result.getNumUpdated() != 1) {
            throw new CatalogDBException("Remove field from Variable Set. Could not remove the field " + name
                    + " from the variableSet id " + variableSetId);
        }

        // Remove all the annotations from that field
        dbAdaptorFactory.getCatalogSampleDBAdaptor().removeAnnotationField(variableSetId, name);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().removeAnnotationField(variableSetId, name);
        dbAdaptorFactory.getCatalogIndividualDBAdaptor().removeAnnotationField(variableSetId, name);
        dbAdaptorFactory.getCatalogFamilyDBAdaptor().removeAnnotationField(variableSetId, name);
        dbAdaptorFactory.getCatalogFileDBAdaptor().removeAnnotationField(variableSetId, name);

        return new OpenCGAResult<>(result);
    }

    private Variable getVariable(VariableSet variableSet, String variableId) throws CatalogDBException {
        for (Variable variable : variableSet.getVariables()) {
            if (variable.getId().equals(variableId)) {
                return variable;
            }
        }
        return null;
    }

    /**
     * Checks if the variable given is present in the variableSet.
     *
     * @param variableSet Variable set.
     * @param variableId  VariableId that will be checked.
     * @throws CatalogDBException when the variableId is not present in the variableSet.
     */
    private void checkVariableInVariableSet(VariableSet variableSet, String variableId) throws CatalogDBException {
        if (getVariable(variableSet, variableId) == null) {
            throw new CatalogDBException("VariableSet {id: " + variableSet.getUid() + "}. The variable {id: " + variableId + "} does not "
                    + "exist.");
        }
    }

    /**
     * Checks if the variable given is not present in the variableSet.
     *
     * @param variableSet Variable set.
     * @param variableId  VariableId that will be checked.
     * @throws CatalogDBException when the variableId is present in the variableSet.
     */
    private void checkVariableNotInVariableSet(VariableSet variableSet, String variableId) throws CatalogDBException {
        if (getVariable(variableSet, variableId) != null) {
            throw new CatalogDBException("VariableSet {id: " + variableSet.getUid() + "}. The variable {id: " + variableId + "} already "
                    + "exists.");
        }
    }

    @Override
    public OpenCGAResult<VariableSet> getVariableSet(long variableSetUid, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query(QueryParams.VARIABLE_SET_UID.key(), variableSetUid);
        Bson projection = Projections.elemMatch("variableSets", Filters.eq(PRIVATE_UID, variableSetUid));
        if (options == null) {
            options = new QueryOptions();
        }
        QueryOptions qOptions = new QueryOptions(options);
        qOptions.put(MongoDBCollection.ELEM_MATCH, projection);
        OpenCGAResult<Study> studyDataResult = get(query, qOptions);

        if (studyDataResult.getResults().isEmpty() || studyDataResult.first().getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {uid: " + variableSetUid + "} does not exist.");
        }

        return endQuery(startTime, studyDataResult.first().getVariableSets());
    }

    @Override
    public OpenCGAResult<VariableSet> getVariableSet(long variableSetId, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        Bson query = new Document("variableSets", new Document("$elemMatch", new Document(PRIVATE_UID, variableSetId)));
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "variableSets.$,_ownerId,groups,_acl");
        DataResult<Document> studyDataResult = studyCollection.find(query, qOptions);

        if (studyDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Variable set not found.");
        }
        if (!checkCanViewStudy(studyDataResult.first(), user)) {
            throw CatalogAuthorizationException.deny(user, "view", "VariableSet", variableSetId, "");
        }
        Study study = studyConverter.convertToDataModelType(studyDataResult.first());
        if (study.getVariableSets() == null || study.getVariableSets().isEmpty()) {
            throw new CatalogDBException("Variable set not found.");
        }
        // Check if it is confidential
        if (study.getVariableSets().get(0).isConfidential()) {
            if (!checkStudyPermission(studyDataResult.first(), user,
                    StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString())) {
                throw CatalogAuthorizationException.deny(user, StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString(),
                        "VariableSet", variableSetId, "");
            }
        }

        return endQuery(startTime, study.getVariableSets());
    }

    @Override
    public OpenCGAResult<VariableSet> getVariableSet(long studyUid, String variableSetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query()
                .append(QueryParams.VARIABLE_SET_ID.key(), variableSetId)
                .append(QueryParams.UID.key(), studyUid);
        Bson projection = Projections.elemMatch("variableSets", Filters.eq("id", variableSetId));
        if (options == null) {
            options = new QueryOptions();
        }
        QueryOptions qOptions = new QueryOptions(options);
        qOptions.put(MongoDBCollection.ELEM_MATCH, projection);
        OpenCGAResult<Study> studyDataResult = get(query, qOptions);

        if (studyDataResult.getResults().isEmpty() || studyDataResult.first().getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist.");
        }

        return endQuery(startTime, studyDataResult.first().getVariableSets());
    }

    @Override
    public OpenCGAResult<VariableSet> getVariableSets(Query query, QueryOptions queryOptions) throws CatalogDBException {
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
                    case STUDY_UID:
                        studyId = query.getLong(VariableSetParams.STUDY_UID.key());
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
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_UID, studyId)));
        aggregation.add(Aggregates.project(Projections.include("variableSets")));
        aggregation.add(Aggregates.unwind("$variableSets"));
        if (mongoQueryList.size() > 0) {
            List<Bson> bsonList = new ArrayList<>(mongoQueryList.size());
            bsonList.addAll(mongoQueryList);
            aggregation.add(Aggregates.match(Filters.and(bsonList)));
        }

        DataResult<Document> queryResult = studyCollection.aggregate(aggregation, filterOptions(queryOptions, FILTER_ROUTE_STUDIES));

        List<VariableSet> variableSets = parseObjects(queryResult, Study.class).stream().map(study -> study.getVariableSets().get(0))
                .collect(Collectors.toList());

        return endQuery(startTime, variableSets);
    }

    @Override
    public OpenCGAResult<VariableSet> getVariableSets(Query query, QueryOptions queryOptions, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        List<Document> mongoQueryList = new LinkedList<>();
        long studyUid = -1;

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
                    case STUDY_UID:
                        studyUid = query.getLong(VariableSetParams.STUDY_UID.key());
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

        if (studyUid == -1) {
            throw new CatalogDBException("Cannot look for variable sets if studyUid is not passed");
        }

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_UID, studyUid)));
        aggregation.add(Aggregates.unwind("$variableSets"));
        if (mongoQueryList.size() > 0) {
            List<Bson> bsonList = new ArrayList<>(mongoQueryList.size());
            bsonList.addAll(mongoQueryList);
            aggregation.add(Aggregates.match(Filters.and(bsonList)));
        }

        DataResult<Document> queryResult = studyCollection.aggregate(aggregation, filterOptions(queryOptions, FILTER_ROUTE_STUDIES));
        if (queryResult.getNumResults() == 0) {
            return endQuery(startTime, Collections.emptyList());
        }

        if (!checkCanViewStudy(queryResult.first(), user)) {
            throw new CatalogAuthorizationException("Permission denied: " + user + " cannot see any variable set");
        }

        boolean hasConfidentialPermission = checkStudyPermission(queryResult.first(), user,
                StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString());
        List<VariableSet> variableSets = new ArrayList<>();
        for (Document studyDocument : queryResult.getResults()) {
            Study study = studyConverter.convertToDataModelType(studyDocument);
            VariableSet vs = study.getVariableSets().get(0);
            if (!vs.isConfidential() || hasConfidentialPermission) {
                variableSets.add(vs);
            }
        }

        return endQuery(startTime, variableSets);
    }

    @Override
    public OpenCGAResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkVariableSetInUse(variableSetId);

        Bson query = Filters.eq(QueryParams.VARIABLE_SET_UID.key(), variableSetId);
        Bson operation = Updates.pull("variableSets", Filters.eq(PRIVATE_UID, variableSetId));
        DataResult result = studyCollection.update(query, operation, null);

        if (result.getNumUpdated() == 0) {
            throw CatalogDBException.uidNotFound("VariableSet", variableSetId);
        }
        return new OpenCGAResult<>(result);
    }

    public void checkVariableSetInUse(long variableSetId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        OpenCGAResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(
                new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId), new QueryOptions());
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of samples : [";
            for (Sample sample : samples.getResults()) {
                msg += " { id: " + sample.getUid() + ", name: \"" + sample.getId() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        OpenCGAResult<Individual> individuals = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(
                new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId),
                new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of individuals : [";
            for (Individual individual : individuals.getResults()) {
                msg += " { id: " + individual.getUid() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        OpenCGAResult<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor().get(
                new Query(CohortDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId), new QueryOptions());
        if (cohorts.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of cohorts : [";
            for (Cohort cohort : cohorts.getResults()) {
                msg += " { id: " + cohort.getUid() + ", name: \"" + cohort.getId() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        OpenCGAResult<Family> families = dbAdaptorFactory.getCatalogFamilyDBAdaptor().get(
                new Query(FamilyDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId), new QueryOptions());
        if (cohorts.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of families : [";
            for (Family family : families.getResults()) {
                msg += " { id: " + family.getUid() + ", name: \"" + family.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
    }


    @Override
    public long getStudyIdByVariableSetId(long variableSetId) throws CatalogDBException {
//        DBObject query = new BasicDBObject("variableSets.id", variableSetId);
        Bson query = Filters.eq("variableSets." + PRIVATE_UID, variableSetId);
        Bson projection = Projections.include(PRIVATE_UID);

//        DataResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject(PRIVATE_UID, true), null);
        DataResult<Document> queryResult = studyCollection.find(query, projection, null);

        if (!queryResult.getResults().isEmpty()) {
            Object id = queryResult.getResults().get(0).get(PRIVATE_UID);
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.uidNotFound("VariableSet", variableSetId);
        }
    }


    @Override
    public OpenCGAResult<Study> getStudiesFromUser(String userId, QueryOptions queryOptions) throws CatalogDBException {
        OpenCGAResult<Study> result = OpenCGAResult.empty();

        OpenCGAResult<Project> allProjects = dbAdaptorFactory.getCatalogProjectDbAdaptor().get(userId, new QueryOptions());
        if (allProjects.getNumResults() == 0) {
            return result;
        }

        for (Project project : allProjects.getResults()) {
            OpenCGAResult<Study> allStudiesInProject = getAllStudiesInProject(project.getUid(), queryOptions);
            if (allStudiesInProject.getNumResults() > 0) {
                result.getResults().addAll(allStudiesInProject.getResults());
                result.setTime(result.getTime() + allStudiesInProject.getTime());
            }
        }

        result.setNumMatches(result.getResults().size());
        result.setNumResults(result.getResults().size());

        return result;
    }

    /*
     * Helper methods
     ********************/

    private void joinFields(Study study, QueryOptions options) throws CatalogDBException {
        try {
            joinFields(study, options, null);
        } catch (CatalogAuthorizationException | CatalogParameterException e) {
            throw new CatalogDBException(e);
        }
    }

    private void joinFields(Study study, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long studyId = study.getUid();
        if (studyId <= 0 || options == null) {
            return;
        }

        if (options.getBoolean("includeFiles")) {
            if (StringUtils.isEmpty(user)) {
                study.setFiles(dbAdaptorFactory.getCatalogFileDBAdaptor().getAllInStudy(studyId, options).getResults());
            } else {
                Query query = new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
                study.setFiles(dbAdaptorFactory.getCatalogFileDBAdaptor().get(studyId, query, options, user).getResults());
            }
        }
        if (options.getBoolean("includeJobs")) {
            if (StringUtils.isEmpty(user)) {
                study.setJobs(dbAdaptorFactory.getCatalogJobDBAdaptor().getAllInStudy(studyId, options).getResults());
            } else {
                Query query = new Query(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
                study.setJobs(dbAdaptorFactory.getCatalogJobDBAdaptor().get(studyId, query, options, user).getResults());
            }
        }
        if (options.getBoolean("includeSamples")) {
            if (StringUtils.isEmpty(user)) {
                study.setSamples(dbAdaptorFactory.getCatalogSampleDBAdaptor().getAllInStudy(studyId, options).getResults());
            } else {
                Query query = new Query(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
                study.setSamples(dbAdaptorFactory.getCatalogSampleDBAdaptor().get(studyId, query, options, user).getResults());
            }
        }
        if (options.getBoolean("includeIndividuals")) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
            if (StringUtils.isEmpty(user)) {
                study.setIndividuals(dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(query, options).getResults());
            } else {
                study.setIndividuals(dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(studyId, query, options, user).getResults());
            }
        }
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    public OpenCGAResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(studyCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission) throws CatalogDBException {
        throw new NotImplementedException("Count not implemented for study collection");
    }

    @Override
    public OpenCGAResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult(studyCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    void updateProjectId(ClientSession clientSession, long projectUid, String newProjectId) throws CatalogDBException {
        Query query = new Query(QueryParams.PROJECT_UID.key(), projectUid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                QueryParams.FQN.key(), QueryParams.UID.key()
        ));
        DBIterator<Study> studyIterator = iterator(clientSession, query, options);

        while (studyIterator.hasNext()) {
            Study study = studyIterator.next();
            String[] split = study.getFqn().split("@");
            String[] split1 = split[1].split(":");
            String newFqn = split[0] + "@" + newProjectId + ":" + split1[1];

            // Update the internal project id and fqn
            Bson update = new Document("$set", new Document()
                    .append(QueryParams.FQN.key(), newFqn)
                    .append(PRIVATE_PROJECT_ID, newProjectId)
            );
            Bson bsonQuery = Filters.eq(QueryParams.UID.key(), study.getUid());

            DataResult result = studyCollection.update(clientSession, bsonQuery, update, null);
            if (result.getNumUpdated() == 0) {    //Check if the the project id was modified
                throw new CatalogDBException("Could not update new project id references in study " + study.getFqn());
            }
        }
    }

    @Override
    public OpenCGAResult update(long studyUid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.PROJECT_UID.key()));
        OpenCGAResult<Study> studyResult = get(studyUid, options);
        if (studyResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update study. Study uid '" + studyUid + "' not found.");
        }
        String studyId = studyResult.first().getId();

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, studyResult.first(), parameters));
        } catch (CatalogDBException e) {
            logger.error("Could not update study {}: {}", studyId, e.getMessage(), e);
            throw new CatalogDBException("Could not update study '" + studyId + "': " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        Document updateParams = getDocumentUpdateParams(parameters);
        if (updateParams.isEmpty() && !parameters.containsKey(QueryParams.ID.key())) {
            throw new CatalogDBException("Nothing to update");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.PROJECT_UID.key()));
        DBIterator<Study> iterator = iterator(query, options);

        OpenCGAResult<Study> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Study study = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, study, parameters)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update study {}: {}", study.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, study.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Study study, ObjectMap parameters) throws CatalogDBException {
        long tmpStartTime = startQuery();

        Document updateParams = getDocumentUpdateParams(parameters);
        if (updateParams.isEmpty() && !parameters.containsKey(QueryParams.ID.key())) {
            throw new CatalogDBException("Nothing to update");
        }

        if (parameters.containsKey(QueryParams.ID.key())) {
            editId(clientSession, study.getUid(), parameters.getString(QueryParams.ID.key()));
        }
        List<Event> events = new ArrayList<>();
        if (!updateParams.isEmpty()) {
            Document updates = new Document("$set", updateParams);

            Query tmpQuery = new Query(QueryParams.UID.key(), study.getUid());
            Bson finalQuery = parseQuery(tmpQuery);

            logger.debug("Update study. Query: {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    updates.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            DataResult result = studyCollection.update(clientSession, finalQuery, updates, null);

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Study " + study.getId() + " not found");
            }
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, study.getId(), "Study was already updated"));
            }
        }

        logger.debug("Study {} successfully updated", study.getId());
        return endWrite(tmpStartTime, 1, 1, events);
    }

    static Document getDocumentUpdateParams(ObjectMap parameters) throws CatalogDBException {
        Document studyParameters = new Document();

        String[] acceptedParams = {QueryParams.ALIAS.key(), QueryParams.NAME.key(), QueryParams.CREATION_DATE.key(),
                QueryParams.DESCRIPTION.key(), };
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {QueryParams.SIZE.key()};
        filterLongParams(parameters, studyParameters, acceptedLongParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.STATUS.key()};
        filterObjectParams(parameters, studyParameters, acceptedObjectParams);
        if (studyParameters.containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), studyParameters);
        }

        if (parameters.containsKey(QueryParams.URI.key())) {
            URI uri = parameters.get(QueryParams.URI.key(), URI.class);
            studyParameters.put(QueryParams.URI.key(), uri.toString());
        }

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_NAME.key())) {
            studyParameters.put(QueryParams.INTERNAL_STATUS_NAME.key(), parameters.get(QueryParams.INTERNAL_STATUS_NAME.key()));
            studyParameters.put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.NOTIFICATION_WEBHOOK.key())) {
            Object value = parameters.get(QueryParams.NOTIFICATION_WEBHOOK.key());
            studyParameters.put(QueryParams.NOTIFICATION_WEBHOOK.key(), value);
        }

        if (!studyParameters.isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            studyParameters.put(QueryParams.MODIFICATION_DATE.key(), time);
            studyParameters.put(PRIVATE_MODIFICATION_DATE, date);
        }
        return studyParameters;
    }

    private void editId(ClientSession clientSession, long studyUid, String newId) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), studyUid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(QueryParams.FQN.key(), QueryParams.ID.key()));

        OpenCGAResult<Study> studyDataResult = get(clientSession, query, options);
        if (studyDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Cannot update study id. Study " + studyUid + " not found");
        }

        String oldId = studyDataResult.first().getId();
        String newFqn = studyDataResult.first().getFqn().replace(oldId, newId);

        Bson bsonQuery = parseQuery(query);
        Bson update = Updates.combine(
                Updates.set(QueryParams.ID.key(), newId),
                Updates.set(QueryParams.FQN.key(), newFqn)
        );
        DataResult writeResult = studyCollection.update(bsonQuery, update, null);
        if (writeResult.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not update study id");
        }
    }

    @Deprecated
    @Override
    public OpenCGAResult delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Use other delete method");
//        checkId(id);
//        // Check the study is active
//        Query query = new Query(QueryParams.UID.key(), id);
//        if (count(query).first() == 0) {
//            query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
//            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_NAME.key());
//            Study study = get(query, options).first();
//            throw new CatalogDBException("The study {" + id + "} was already " + study.getStatus().getName());
//        }
//
//        // If we don't find the force parameter, we check first if the user does not have an active project.
//        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
//            checkCanDelete(id);
//        }
//
//        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
//            // Delete the active studies (if any)
//            query = new Query(PRIVATE_STUDY_UID, id);
//            dbAdaptorFactory.getCatalogFileDBAdaptor().setStatus(query, Status.DELETED);
//            dbAdaptorFactory.getCatalogJobDBAdaptor().setStatus(query, Status.DELETED);
//            dbAdaptorFactory.getCatalogSampleDBAdaptor().setStatus(query, Status.DELETED);
//            dbAdaptorFactory.getCatalogIndividualDBAdaptor().setStatus(query, Status.DELETED);
//            dbAdaptorFactory.getCatalogCohortDBAdaptor().setStatus(query, Status.DELETED);
//        }
//
//        // Change the status of the project to deleted
//        return setStatus(id, Status.DELETED);
    }

    @Deprecated
    @Override
    public OpenCGAResult delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return delete(query);
    }

    @Override
    public OpenCGAResult delete(Study study) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            Query query = new Query(QueryParams.UID.key(), study.getUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find study " + study.getId() + " with uid " + study.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete study {}: {}", study.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete study " + study.getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        DBIterator<Document> iterator = nativeIterator(query, QueryOptions.empty());

        OpenCGAResult<Study> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Document study = iterator.next();
            String studyId = study.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, study)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete study {}: {}", studyId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, studyId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document studyDocument) throws CatalogDBException {
        long tmpStartTime = startQuery();

        String studyId = studyDocument.getString(QueryParams.ID.key());
        long studyUid = studyDocument.getLong(PRIVATE_UID);
        long projectUid = studyDocument.getEmbedded(Arrays.asList(PRIVATE_PROJECT, PRIVATE_UID), -1L);

        logger.debug("Deleting study {} ({})", studyId, studyUid);

        // TODO: In the future, we will want to delete also all the files, samples, cohorts... associated

        // Add status DELETED
        studyDocument.put(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new Status(Status.DELETED), "status"));

        // Upsert the document into the DELETED collection
        Bson query = new Document()
                .append(QueryParams.ID.key(), studyId)
                .append(PRIVATE_PROJECT_UID, projectUid);
        deletedStudyCollection.update(clientSession, query, new Document("$set", studyDocument),
                new QueryOptions(MongoDBCollection.UPSERT, true));

        // Delete the document from the main STUDY collection
        query = new Document()
                .append(PRIVATE_UID, studyUid)
                .append(PRIVATE_PROJECT_UID, projectUid);
        DataResult remove = studyCollection.remove(clientSession, query, null);
        if (remove.getNumMatches() == 0) {
            throw new CatalogDBException("Study " + studyId + " not found");
        }
        if (remove.getNumDeleted() == 0) {
            throw new CatalogDBException("Study " + studyId + " could not be deleted");
        }
        logger.debug("Study {} successfully deleted", studyId);

        return endWrite(tmpStartTime, 1, 0, 0, 1, null);
    }

//    /**
//     * Checks whether the studyId has any active document in the study.
//     *
//     * @param studyId study id.
//     * @throws CatalogDBException when the study has active documents.
//     */
//    private void checkCanDelete(long studyId) throws CatalogDBException {
//        checkId(studyId);
//        Query query = new Query(PRIVATE_STUDY_UID, studyId)
//                .append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
//
//        Long count = dbAdaptorFactory.getCatalogFileDBAdaptor().count(query).first();
//        if (count > 0) {
//            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
//                    + " files in use.");
//        }
//        count = dbAdaptorFactory.getCatalogJobDBAdaptor().count(query).first();
//        if (count > 0) {
//            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
//                    + " jobs in use.");
//        }
//        count = dbAdaptorFactory.getCatalogSampleDBAdaptor().count(query).first();
//        if (count > 0) {
//            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
//                    + " samples in use.");
//        }
//        count = dbAdaptorFactory.getCatalogIndividualDBAdaptor().count(query).first();
//        if (count > 0) {
//            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
//                    + " individuals in use.");
//        }
//        count = dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).first();
//        if (count > 0) {
//            throw new CatalogDBException("The study {" + studyId + "} cannot be deleted. The study has " + count
//                    + " cohorts in use.");
//        }
//    }

    @Override
    public OpenCGAResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult<Study> get(long studyId, QueryOptions options) throws CatalogDBException {
        checkId(studyId);
        Query query = new Query(QueryParams.UID.key(), studyId);
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Study> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    OpenCGAResult<Study> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        OpenCGAResult<Study> studyDataResult;
        try (DBIterator<Study> dbIterator = iterator(clientSession, query, options)) {
            studyDataResult = endQuery(startTime, dbIterator);
        }
        for (Study study : studyDataResult.getResults()) {
            joinFields(study, options);
        }
        return studyDataResult;
    }

    @Override
    public OpenCGAResult<Study> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        OpenCGAResult<Study> studyDataResult;
        try (DBIterator<Study> dbIterator = iterator(query, options, user)) {
            studyDataResult = endQuery(startTime, dbIterator);
        }
        for (Study study : studyDataResult.getResults()) {
            joinFields(study, options, user);
        }
        return studyDataResult;
    }

    @Override
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        return nativeGet(null, query, options);
    }

    OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return nativeGet(null, query, options, user);
    }

    OpenCGAResult nativeGet(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Study> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    private DBIterator<Study> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new StudyCatalogMongoDBIterator<>(mongoCursor, options, studyConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new StudyCatalogMongoDBIterator<>(mongoCursor, options);
    }

    @Override
    public DBIterator<Study> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options);
        Function<Document, Boolean> iteratorFilter = (d) -> checkCanViewStudy(d, user);
        return new StudyCatalogMongoDBIterator<>(mongoCursor, options, studyConverter, iteratorFilter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return nativeIterator(null, query, options, user);
    }


    public DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        Function<Document, Boolean> iteratorFilter = (d) -> checkCanViewStudy(d, user);
        return new StudyCatalogMongoDBIterator<Document>(mongoCursor, options, iteratorFilter);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        QueryOptions qOptions = new QueryOptions(options);
        if (qOptions.containsKey(QueryOptions.INCLUDE)) {
            List<String> includeList = new ArrayList<>(qOptions.getAsStringList(QueryOptions.INCLUDE));
            includeList.add("_ownerId");
            includeList.add("_acl");
            includeList.add(QueryParams.GROUPS.key());
            qOptions.put(QueryOptions.INCLUDE, includeList);
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_STUDIES);

        Bson bson = parseQuery(query);

        logger.debug("Study native get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return studyCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedStudyCollection.iterator(clientSession, bson, null, null, qOptions);
        }
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(studyCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(studyCollection, bsonQuery, field, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(studyCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
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

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        // Flag indicating whether and OR between ID and ALIAS has been performed and already added to the andBsonList object
        boolean idOrAliasFlag = false;

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PROJECT_UID:
                        addAutoOrQuery(PRIVATE_PROJECT_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PROJECT_ID:
                        addAutoOrQuery(PRIVATE_PROJECT_ID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PROJECT_UUID:
                        addAutoOrQuery(PRIVATE_PROJECT_UUID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case ALIAS:
                        // We perform an OR if both ID and ALIAS are present in the query and both have the exact same value
                        if (StringUtils.isNotEmpty(queryCopy.getString(QueryParams.ID.key()))
                                && StringUtils.isNotEmpty(queryCopy.getString(QueryParams.ALIAS.key()))
                                && queryCopy.getString(QueryParams.ID.key()).equals(queryCopy.getString(QueryParams.ALIAS.key()))) {
                            if (!idOrAliasFlag) {
                                List<Document> orList = Arrays.asList(
                                        new Document(QueryParams.ID.key(), queryCopy.getString(QueryParams.ID.key())),
                                        new Document(QueryParams.ALIAS.key(), queryCopy.getString(QueryParams.ALIAS.key()))
                                );
                                andBsonList.add(new Document("$or", orList));
                                idOrAliasFlag = true;
                            }
                        } else {
                            addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        }
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(Status.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(StudyDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), queryParam.key(), queryCopy,
                                StudyDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_NAME:
                        addAutoOrQuery(StudyDBAdaptor.QueryParams.STATUS_NAME.key(), queryParam.key(), queryCopy,
                                StudyDBAdaptor.QueryParams.STATUS_NAME.type(), andBsonList);
                        break;
                    case FQN:
                    case UUID:
                    case NAME:
                    case DESCRIPTION:
                    case INTERNAL_STATUS_DATE:
                    case DATASTORES:
                    case SIZE:
                    case URI:
                    case GROUPS:
                    case GROUP_ID:
                    case GROUP_USER_IDS:
                    case RELEASE:
                    case COHORTS:
                    case VARIABLE_SET:
                    case VARIABLE_SET_UID:
                    case VARIABLE_SET_ID:
                    case VARIABLE_SET_NAME:
                    case VARIABLE_SET_DESCRIPTION:
                    case OWNER:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
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
     * @param clientSession Client session.
     * @param studyId   Study Identifier
     * @param size disk usage of a new created, updated or deleted file belonging to studyId. This argument
     *                  will be > 0 to increment the size field in the study collection or < 0 to decrement it.
     * @throws CatalogDBException An exception is launched when the update crashes.
     */
    public void updateDiskUsage(ClientSession clientSession, long studyId, long size) throws CatalogDBException {
        Bson query = new Document(QueryParams.UID.key(), studyId);
        Bson update = Updates.inc(QueryParams.SIZE.key(), size);
        if (studyCollection.update(clientSession, query, update, null).getNumMatches() == 0) {
            throw new CatalogDBException("CatalogMongoStudyDBAdaptor updateDiskUsage: Couldn't update the size field of"
                    + " the study " + studyId);
        }
    }
}
