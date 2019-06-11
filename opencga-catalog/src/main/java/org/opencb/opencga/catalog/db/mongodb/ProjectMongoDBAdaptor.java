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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ProjectConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by imedina on 08/01/16.
 */
public class ProjectMongoDBAdaptor extends MongoDBAdaptor implements ProjectDBAdaptor {

    private final MongoDBCollection userCollection;
    private ProjectConverter projectConverter;

    public ProjectMongoDBAdaptor(MongoDBCollection userCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(ProjectMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.userCollection = userCollection;
        this.projectConverter = new ProjectConverter();
    }

    @Override
    public boolean exists(long projectId) {
        QueryResult<Long> count = userCollection.count(new Document(UserDBAdaptor.QueryParams.PROJECTS_UID.key(), projectId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public void nativeInsert(Map<String, Object> project, String userId) throws CatalogDBException {
        Bson query = Filters.and(Filters.eq(UserDBAdaptor.QueryParams.ID.key(), userId),
                Filters.ne(UserDBAdaptor.QueryParams.PROJECTS_ID.key(), project.get(QueryParams.ID.key())));
        Bson update = Updates.push("projects", getMongoDBDocument(project, "project"));

        //Update object
        QueryResult<UpdateResult> queryResult = userCollection.update(query, update, null);

        if (queryResult.getResult().get(0).getModifiedCount() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {" + project.get(QueryParams.ID.key()) + "\"} already exists for this user");
        }
    }

    @Override
    public QueryResult<Project> insert(Project project, String userId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<Study> studies = project.getStudies();
        if (studies == null) {
            studies = Collections.emptyList();
        }
        project.setStudies(Collections.emptyList());


        Bson countQuery = Filters.and(Filters.eq(UserDBAdaptor.QueryParams.ID.key(), userId),
                Filters.eq(UserDBAdaptor.QueryParams.PROJECTS_ID.key(), project.getId()));
        QueryResult<Long> count = userCollection.count(countQuery);
        if (count.getResult().get(0) != 0) {
            throw new CatalogDBException("Project {id:\"" + project.getId() + "\"} already exists for this user");
        }
        long projectUid = dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
        project.setUid(projectUid);
        project.setFqn(userId + "@" + project.getId());
        if (StringUtils.isEmpty(project.getUuid())) {
            project.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PROJECT));
        }

        Document projectDocument = projectConverter.convertToStorageType(project);
        if (StringUtils.isNotEmpty(project.getCreationDate())) {
            projectDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(project.getCreationDate()));
        } else {
            projectDocument.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }

        Bson update = Updates.push("projects", projectDocument);

        //Update object
        Bson query = Filters.and(Filters.eq(UserDBAdaptor.QueryParams.ID.key(), userId),
                Filters.ne(UserDBAdaptor.QueryParams.PROJECTS_ID.key(), project.getId()));
        QueryResult<UpdateResult> queryResult = userCollection.update(query, update, null);

        if (queryResult.getResult().get(0).getModifiedCount() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {id:\"" + project.getId() + "\"} already exists for this user");
        }

        String errorMsg = "";
        for (Study study : studies) {
            String studyErrorMsg = dbAdaptorFactory.getCatalogStudyDBAdaptor().insert(project, study, options)
                    .getErrorMsg();
            if (studyErrorMsg != null && !studyErrorMsg.isEmpty()) {
                errorMsg += ", " + study.getId() + ":" + studyErrorMsg;
            }
        }
        List<Project> result = get(project.getUid(), null).getResult();
        return endQuery("Create Project", startTime, result, errorMsg, null);
    }

    @Override
    public QueryResult<Integer> incrementCurrentRelease(long projectId) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.UID.key(), projectId);
        Bson update = new Document("$inc", new Document("projects.$." + QueryParams.CURRENT_RELEASE.key(), 1));

        QueryResult<UpdateResult> updateQR = userCollection.update(parseQuery(query), update, null);
        if (updateQR == null || updateQR.first().getMatchedCount() == 0) {
            throw new CatalogDBException("Could not increment release number. Project id " + projectId + " not found");
        } else if (updateQR.first().getModifiedCount() == 0) {
            throw new CatalogDBException("Internal error. Current release number could not be incremented.");
        }

        QueryResult<Project> projectQueryResult = get(projectId, new QueryOptions(QueryOptions.INCLUDE, QueryParams.CURRENT_RELEASE.key()));
        return endQuery(Long.toString(projectId), startTime, Arrays.asList(projectQueryResult.first().getCurrentRelease()));
    }

    @Override
    public void editId(String owner, long projectUid, String oldId, String newId) throws CatalogDBException {
        if (!exists(projectUid)) {
            logger.error("Project {} not found", projectUid);
            throw new CatalogDBException("Project not found.");
        }

        Bson query = Filters.and(
                Filters.eq(UserDBAdaptor.QueryParams.PROJECTS_UID.key(), projectUid),
                Filters.ne(UserDBAdaptor.QueryParams.PROJECTS_ID.key(), newId)
        );
        Bson update = new Document("$set", new Document()
                .append("projects.$." + QueryParams.ID.key(), newId)
                .append("projects.$." + QueryParams.FQN.key(), owner + "@" + newId)
        );
        QueryResult<UpdateResult> result = userCollection.update(query, update, null);
        if (result.getResult().get(0).getModifiedCount() == 0) {    //Check if the the project id was modified
            throw new CatalogDBException("Project {id:\"" + newId + "\"} already exists");
        }

        // Update all the internal project ids stored in the study documents
        dbAdaptorFactory.getCatalogStudyDBAdaptor().updateProjectId(projectUid, newId);
    }

    @Override
    public long getId(final String userId, final String projectIdStr) throws CatalogDBException {

        String projectId = projectIdStr;

        if (projectId.contains("@")) {
            projectId = projectId.split("@", 2)[1];
        }

        QueryResult<Document> queryResult = userCollection.find(
                new BsonDocument(UserDBAdaptor.QueryParams.PROJECTS_ID.key(), new BsonString(projectId))
                        .append(UserDBAdaptor.QueryParams.ID.key(), new BsonString(userId)),
                Projections.fields(Projections.include(UserDBAdaptor.QueryParams.PROJECTS_UID.key()),
                        Projections.elemMatch("projects", Filters.eq(QueryParams.ID.key(), projectId))),
                null);
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            return -1;
        } else {
            return user.getProjects().get(0).getUid();
        }
    }

    @Override
    public String getOwnerId(long projectId) throws CatalogDBException {
//        DBObject query = new BasicDBObject(UserDBAdaptor.QueryParams.PROJECTS_UID.key(), projectId);
        Bson query = Filters.eq(UserDBAdaptor.QueryParams.PROJECTS_UID.key(), projectId);

//        DBObject projection = new BasicDBObject(UserDBAdaptor.QueryParams.ID.key(), "true");
        Bson projection = Projections.include(UserDBAdaptor.QueryParams.ID.key());

//        QueryResult<DBObject> result = userCollection.find(query, projection, null);
        QueryResult<Document> result = userCollection.find(query, projection, null);

        if (result.getResult().isEmpty()) {
            throw CatalogDBException.uidNotFound("Project", projectId);
        } else {
            return result.getResult().get(0).get(UserDBAdaptor.QueryParams.ID.key()).toString();
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return userCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission) throws CatalogDBException {
        throw new NotImplementedException("Count not implemented for projects");
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return userCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        Document projectParameters = new Document();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.CREATION_DATE.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.ORGANIZATION.key(), QueryParams.LAST_MODIFIED.key(), QueryParams.ORGANISM_SCIENTIFIC_NAME.key(),
                QueryParams.ORGANISM_COMMON_NAME.key(), QueryParams.ORGANISM_ASSEMBLY.key(), };
        for (String s : acceptedParams) {
            if (parameters.containsKey(s)) {
                projectParameters.put("projects.$." + s, parameters.getString(s));
            }
        }
        String[] acceptedIntParams = {QueryParams.SIZE.key(), QueryParams.ORGANISM_TAXONOMY_CODE.key(), };
        for (String s : acceptedIntParams) {
            if (parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if (anInt != Integer.MIN_VALUE) {
                    projectParameters.put("projects.$." + s, anInt);
                }
            }
        }
        Map<String, Object> attributes = parameters.getMap(QueryParams.ATTRIBUTES.key());
        if (attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                projectParameters.put("projects.$.attributes." + entry.getKey(), entry.getValue());
            }
//            projectParameters.put("projects.$.attributes", attributes);
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            projectParameters.put("projects.$." + QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            projectParameters.put("projects.$." + QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        QueryResult<UpdateResult> updateResult = new QueryResult<>();
        if (!projectParameters.isEmpty()) {

            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            projectParameters.put(QueryParams.MODIFICATION_DATE.key(), time);
            projectParameters.put(PRIVATE_MODIFICATION_DATE, date);

            Bson bsonQuery = parseQuery(query);
            Bson updates = new Document("$set", projectParameters);
            // Fixme: Updates
                    /*
            BasicDBObject query = new BasicDBObject(UserDBAdaptor.QueryParams.PROJECTS_UID.key(), projectId);
            BasicDBObject updates = new BasicDBObject("$set", projectParameters);
            */
            updateResult = userCollection.update(bsonQuery, updates, null);
        }
        return endQuery("Update project", startTime, Collections.singletonList(updateResult.first().getModifiedCount()));
    }

    @Override
    public QueryResult<Project> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        checkId(id);
        QueryResult<Long> update = update(new Query(QueryParams.UID.key(), id), parameters, QueryOptions.empty());
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update project with id " + id);
        }
        return endQuery("Update project", startTime, get(id, null));
    }

    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);

        Document pull = new Document("$pull", new Document("projects", new Document(QueryParams.UID.key(), id)));
        QueryResult<UpdateResult> update = userCollection.update(parseQuery(query), pull, null);

        if (update.first().getModifiedCount() != 1) {
            throw CatalogDBException.deleteError("Project " + id);
        }
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        throw new NotImplementedException("Delete not implemented for projects");
    }

    @Deprecated
    @Override
    public QueryResult<Project> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check the project is active
        Query query = new Query(QueryParams.UID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.READY);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_NAME.key());
            Project project = get(query, options).first();
            throw new CatalogDBException("The project {" + id + "} was already " + project.getStatus().getName());
        }

        // If we don't find the force parameter, we check first if the user does not have an active project.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            // Delete the active studies (if any)
            query = new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), id);
            dbAdaptorFactory.getCatalogStudyDBAdaptor().delete(query, queryOptions);
        }

        // Change the status of the project to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);

        return endQuery("Delete project", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_NAME.key(), Status.READY);
        QueryResult<Project> projectQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.UID.key()));
        for (Project project : projectQueryResult.getResult()) {
            delete(project.getUid(), queryOptions);
        }
        return endQuery("Delete project", startTime, Collections.singletonList(projectQueryResult.getNumTotalResults()));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    private QueryResult<Project>  setStatus(long projectId, String status) throws CatalogDBException {
        return update(projectId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    @Override
    public QueryResult<Project> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return endQuery("Restore projects", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Project> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The project {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.UID.key(), id);

        return endQuery("Restore project", startTime, get(query, null));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
//    @Override
//    public QueryResult<Integer> deleteProject(int projectId) throws CatalogDBException {
//        long startTime = startQuery();
//        DBObject query = new BasicDBObject(UserDBAdaptor.QueryParams.PROJECTS_UID.key(), projectId);
//        DBObject pull = new BasicDBObject("$pull",
//                new BasicDBObject("projects",
//                        new BasicDBObject(UserDBAdaptor.QueryParams.ID.key(), projectId)));
//
//        QueryResult<WriteResult> update = userCollection.update(query, pull, null);
//        List<Integer> deletes = new LinkedList<>();
//        if (update.getResult().get(0).getN() == 0) {
//            throw CatalogDBException.idNotFound("Project", projectId);
//        } else {
//            deletes.add(update.getResult().get(0).getN());
//            return endQuery("delete project", startTime, deletes);
//        }
//    }

    @Override
    public QueryResult<Project> get(String userId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.USER_ID.key(), userId);
        return endQuery("User projects list", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<Project> get(long projectId, QueryOptions options) throws CatalogDBException {
        checkId(projectId);
        Query query = new Query(QueryParams.UID.key(), projectId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
//        // Fixme: Check the code below
//        List<Project> projects = user.getProjects();
//        joinFields(projects.get(0), options);
//
//        return endQuery("Get project", startTime, projects);
    }

    @Override
    public QueryResult<Project> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Project> documentList = new ArrayList<>();
        QueryResult<Project> queryResult;
        try (DBIterator<Project> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        if (options == null || !options.containsKey(QueryOptions.EXCLUDE)
                || (!options.getAsStringList(QueryOptions.EXCLUDE).contains("projects.studies")
                && !options.getAsStringList(QueryOptions.EXCLUDE).contains("studies"))) {
            for (Project project : queryResult.getResult()) {
                Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), project.getUid());
                try {
                    QueryResult<Study> studyQueryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(studyQuery, options);
                    project.setStudies(studyQueryResult.getResult());
                } catch (CatalogDBException e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        }

        return queryResult;
    }

    @Override
    public QueryResult<Project> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Project> documentList = new ArrayList<>();
        QueryResult<Project> queryResult;
        try (DBIterator<Project> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        if (options == null || !options.containsKey(QueryOptions.EXCLUDE)
                || (!options.getAsStringList(QueryOptions.EXCLUDE).contains("projects.studies")
                && !options.getAsStringList(QueryOptions.EXCLUDE).contains("studies"))) {
            for (Project project : queryResult.getResult()) {
                Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), project.getUid());
                try {
                    QueryResult<Study> studyQueryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(studyQuery, options, user);
                    project.setStudies(studyQueryResult.getResult());
                } catch (CatalogDBException e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        }

        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
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
    public DBIterator<Project> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor, projectConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new MongoDBIterator<>(mongoCursor);
    }

    @Override
    public DBIterator<Project> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, user);
        return new MongoDBIterator<>(mongoCursor, projectConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions, user);
        return new MongoDBIterator<>(mongoCursor);
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {

        // Fetch all the studies that the user can see
        List<Long> projectUids = query.getAsLongList(QueryParams.UID.key());
        Query studyQuery = new Query();
        if (projectUids != null && projectUids.size() > 0) {
            studyQuery.append(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), projectUids);
        }
        List<String> projectIds = query.getAsStringList(QueryParams.ID.key());
        if (projectIds != null && projectIds.size() > 0) {
            studyQuery.append(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectIds);
        }

        studyQuery.putIfNotEmpty(StudyDBAdaptor.QueryParams.UID.key(), query.getString(QueryParams.STUDY_UID.key()));
        studyQuery.putIfNotEmpty(StudyDBAdaptor.QueryParams.ID.key(), query.getString(QueryParams.STUDY_ID.key()));
        studyQuery.putIfNotEmpty(StudyDBAdaptor.QueryParams.OWNER.key(), query.getString(QueryParams.USER_ID.key()));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, new QueryOptions(), user);

        query.remove(QueryParams.STUDY_UID.key());
        query.remove(QueryParams.STUDY_ID.key());

        // We build a map of projectId - list<studies>
//        Map<Long, List<Study>> studyMap = new HashMap<>();
//        StudyConverter studyConverter = new StudyConverter();
//        for (Document studyDocument : queryResult.getResult()) {
//            Long projectId = studyDocument.getLong("_projectId");
//            if (!studyMap.containsKey(projectId)) {
//                studyMap.put(projectId, new ArrayList<>());
//            }
//            studyMap.get(projectId).add(studyConverter.convertToDataModelType(studyDocument));
//        }

        if (queryResult.getNumResults() == 0) {
            // It might be that the owner of the study is asking for its own projects but no studies have been created yet. Just in case,
            // we check if any study matches the query. If that's the case, the user does not have proper permissions. Otherwise, he might
            // be the owner...
            if (dbAdaptorFactory.getCatalogStudyDBAdaptor().count(studyQuery).first() == 0) {
                if (!StringUtils.isEmpty(query.getString(QueryParams.USER_ID.key()))
                        && !user.equals(query.getString(QueryParams.USER_ID.key()))) {
                    // User does not have proper permissions
                    throw CatalogAuthorizationException.deny(user, "view", "project", -1, "");
                }
                query.put(QueryParams.USER_ID.key(), user);
            } else {
                // User does not have proper permissions
                throw CatalogAuthorizationException.deny(user, "view", "project", -1, "");
            }
        } else {
            // We get all the projects the user can see
            projectUids = queryResult.getResult().stream()
                    .map(document -> ((Document) document.get("_project")).getLong("uid"))
                    .collect(Collectors.toList());
        }
        if (projectUids != null && !projectUids.isEmpty()) {
            query.put(QueryParams.UID.key(), projectUids);
        }

        return getMongoCursor(query, options);
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {

        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
        List<Bson> aggregates = new ArrayList<>();

        Bson bsonQuery = parseQuery(query);
        aggregates.add(Aggregates.match(bsonQuery));
        aggregates.add(Aggregates.unwind("$projects"));
        aggregates.add(Aggregates.match(bsonQuery));

        // Check include
        List<String> includeList = new ArrayList<>();
        if (options != null && options.get(QueryOptions.INCLUDE) != null) {
            List<String> optionsAsStringList = options.getAsStringList(QueryOptions.INCLUDE);
            includeList.addAll(optionsAsStringList.stream().collect(Collectors.toList()));
            if (!includeList.contains(QueryParams.UID.key())) {
                includeList.add(QueryParams.UID.key());
            }

            // Check if they start with projects.
            for (int i = 0; i < includeList.size(); i++) {
                if (!includeList.get(i).startsWith("projects.")) {
                    String param = "projects." + includeList.get(i);
                    includeList.set(i, param);
                }
            }
        }

        for (Bson aggregate : aggregates) {
            logger.debug("Get project: Aggregate : {}", aggregate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        }

        QueryOptions qOptions = new QueryOptions();
        if (!includeList.isEmpty()) {
            qOptions.put(QueryOptions.INCLUDE, includeList);
        }

        return userCollection.nativeQuery().aggregate(aggregates, qOptions).iterator();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        throw new NotImplementedException("Rank project is not implemented");
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        throw new NotImplementedException("GroupBy in project is not implemented");
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        throw new NotImplementedException("GroupBy in project is not implemented");
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
        try (DBIterator<Project> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);

        for (Map.Entry<String, Object> entry : query.entrySet()) {
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
                        addAutoOrQuery("projects." + queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case USER_ID:
                        addAutoOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery("projects." + entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = "projects." + entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = "projects." + entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Status.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery("projects." + queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case NAME:
                    case UUID:
                    case ID:
                    case FQN:
                    case DESCRIPTION:
                    case ORGANIZATION:
                    case ORGANISM:
                    case ORGANISM_SCIENTIFIC_NAME:
                    case ORGANISM_COMMON_NAME:
                    case ORGANISM_TAXONOMY_CODE:
                    case ORGANISM_ASSEMBLY:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case LAST_MODIFIED:
                    case SIZE:
                    case DATASTORES:
                    case ACL_USER_ID:
                        addAutoOrQuery("projects." + queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
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

    /**
     * Checks whether the projectId has any active study.
     *
     * @param projectId project id.
     * @throws CatalogDBException when the project has active studies. Studies must be deleted first.
     */
    private void checkCanDelete(long projectId) throws CatalogDBException {
        checkId(projectId);
        Query query = new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId)
                .append(StudyDBAdaptor.QueryParams.STATUS_NAME.key(), Status.READY);
        Long count = dbAdaptorFactory.getCatalogStudyDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The project {" + projectId + "} cannot be deleted. The project has " + count
                    + " studies in use.");
        }
    }
}
