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
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ProjectConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ProjectDBAdaptor.QueryParams.INTERNAL_STATUS_NAME;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by imedina on 08/01/16.
 */
public class ProjectMongoDBAdaptor extends MongoDBAdaptor implements ProjectDBAdaptor {

    private final MongoDBCollection userCollection;
    private final MongoDBCollection deletedUserCollection;
    private ProjectConverter projectConverter;

    public ProjectMongoDBAdaptor(MongoDBCollection userCollection, MongoDBCollection deletedUserCollection, Configuration configuration,
                                 MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(ProjectMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.userCollection = userCollection;
        this.deletedUserCollection = deletedUserCollection;
        this.projectConverter = new ProjectConverter();
    }

    @Override
    public boolean exists(long projectId) {
        DataResult<Long> count = userCollection.count(new Document(UserDBAdaptor.QueryParams.PROJECTS_UID.key(), projectId));
        return count.getNumMatches() != 0;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> project, String userId) throws CatalogDBException {
        Bson query = Filters.and(Filters.eq(UserDBAdaptor.QueryParams.ID.key(), userId),
                Filters.ne(UserDBAdaptor.QueryParams.PROJECTS_ID.key(), project.get(QueryParams.ID.key())));
        Bson update = Updates.push("projects", getMongoDBDocument(project, "project"));

        //Update object
        DataResult result = userCollection.update(query, update, null);
        if (result.getNumInserted() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {" + project.get(QueryParams.ID.key()) + "\"} already exists for this user");
        }
        return new OpenCGAResult(result);
    }

    @Override
    public OpenCGAResult insert(Project project, String userId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting project insert transaction for project id '{}'", project.getId());

            insert(clientSession, project, userId);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create project {}: {}", project.getId(), e.getMessage()));
    }

    Project insert(ClientSession clientSession, Project project, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (project.getStudies() != null && !project.getStudies().isEmpty()) {
            throw new CatalogParameterException("Creating project and studies in a single transaction is forbidden");
        }
        project.setStudies(Collections.emptyList());

        Bson countQuery = Filters.and(Filters.eq(UserDBAdaptor.QueryParams.ID.key(), userId),
                Filters.eq(UserDBAdaptor.QueryParams.PROJECTS_ID.key(), project.getId()));
        DataResult<Long> count = userCollection.count(clientSession, countQuery);
        if (count.getNumMatches() != 0) {
            throw new CatalogDBException("Project {id:\"" + project.getId() + "\"} already exists for this user");
        }

        long projectUid = getNewUid();
        project.setUid(projectUid);
        project.setFqn(userId + "@" + project.getId());
        if (StringUtils.isEmpty(project.getUuid())) {
            project.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PROJECT));
        }
        if (StringUtils.isEmpty(project.getCreationDate())) {
            project.setCreationDate(TimeUtils.getTime());
        }

        Document projectDocument = projectConverter.convertToStorageType(project);
        projectDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(project.getCreationDate()));
        projectDocument.put(PRIVATE_MODIFICATION_DATE, projectDocument.get(PRIVATE_CREATION_DATE));

        Bson update = Updates.push("projects", projectDocument);

        //Update object
        Bson query = Filters.eq(UserDBAdaptor.QueryParams.ID.key(), userId);

        logger.debug("Inserting project. Query: {}, update: {}",
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        userCollection.update(clientSession, query, update, null);

        return project;
    }

    @Override
    public OpenCGAResult incrementCurrentRelease(long projectId) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.UID.key(), projectId);
        Bson update = new Document("$inc", new Document("projects.$." + QueryParams.CURRENT_RELEASE.key(), 1));

        DataResult updateQR = userCollection.update(parseQuery(query), update, null);
        if (updateQR == null || updateQR.getNumMatches() == 0) {
            throw new CatalogDBException("Could not increment release number. Project id " + projectId + " not found");
        } else if (updateQR.getNumUpdated() == 0) {
            throw new CatalogDBException("Internal error. Current release number could not be incremented.");
        }
        return new OpenCGAResult(updateQR);
    }

    private void editId(ClientSession clientSession, String owner, long projectUid, String newId) throws CatalogDBException {
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
        DataResult result = userCollection.update(clientSession, query, update, null);
        if (result.getNumUpdated() == 0) {    //Check if the the project id was modified
            throw new CatalogDBException("Project {id:\"" + newId + "\"} already exists");
        }

        // Update all the internal project ids stored in the study documents
        dbAdaptorFactory.getCatalogStudyDBAdaptor().updateProjectId(clientSession, projectUid, newId);
    }

    @Override
    public long getId(final String userId, final String projectIdStr) throws CatalogDBException {

        String projectId = projectIdStr;

        if (projectId.contains("@")) {
            projectId = projectId.split("@", 2)[1];
        }

        DataResult<Document> queryResult = userCollection.find(
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

//        DataResult<DBObject> result = userCollection.find(query, projection, null);
        DataResult<Document> result = userCollection.find(query, projection, null);

        if (result.getResults().isEmpty()) {
            throw CatalogDBException.uidNotFound("Project", projectId);
        } else {
            return result.getResults().get(0).get(UserDBAdaptor.QueryParams.ID.key()).toString();
        }
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(userCollection.count(bson));
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission) throws CatalogDBException {
        throw new NotImplementedException("Count not implemented for projects");
    }

    @Override
    public OpenCGAResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult(userCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult update(long projectUid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(),
                QueryParams.FQN.key()));
        OpenCGAResult<Project> projectDataResult = get(projectUid, options);

        if (projectDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update project. Project uid '" + projectUid + "' not found.");
        }

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, projectDataResult.first(), parameters));
        } catch (CatalogDBException e) {
            logger.error("Could not update project {}: {}", projectDataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update project '" + projectDataResult.first().getId() + "': " + e.getMessage(),
                    e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(),
                QueryParams.FQN.key()));
        DBIterator<Project> iterator = iterator(query, options);

        OpenCGAResult<Project> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Project project = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, project, parameters)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update project {}: {}", project.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, project.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Project project, ObjectMap parameters) throws CatalogDBException {
        Document updateParams = getDocumentUpdateParams(parameters);

        if (updateParams.isEmpty() && !parameters.containsKey(QueryParams.ID.key())) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        Document updates = new Document("$set", updateParams);

        long tmpStartTime = startQuery();
        if (parameters.containsKey(QueryParams.ID.key())) {
            logger.debug("Update project id '{}'({}) to new id '{}'", project.getId(), project.getUid(),
                    parameters.getString(QueryParams.ID.key()));
            editId(clientSession, project.getFqn().split("@")[0], project.getUid(), parameters.getString(QueryParams.ID.key()));
        }

        if (!updateParams.isEmpty()) {
            Query tmpQuery = new Query(QueryParams.UID.key(), project.getUid());
            Bson finalQuery = parseQuery(tmpQuery);

            logger.debug("Update project. Query: {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    updates.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            DataResult result = userCollection.update(clientSession, finalQuery, updates, null);

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Project " + project.getId() + " not found");
            }
            List<Event> events = new ArrayList<>();
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, project.getId(), "Project was already updated"));
            }
        }
        logger.debug("Project {} successfully updated", project.getId());

        return endWrite(tmpStartTime, 1, 1, null);
    }

    Document getDocumentUpdateParams(ObjectMap parameters) throws CatalogDBException {
        Document projectParameters = new Document();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.CREATION_DATE.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.ORGANISM_SCIENTIFIC_NAME.key(), QueryParams.ORGANISM_COMMON_NAME.key(), QueryParams.ORGANISM_ASSEMBLY.key(), };
        for (String s : acceptedParams) {
            if (parameters.containsKey(s)) {
                projectParameters.put("projects.$." + s, parameters.getString(s));
            }
        }
        Map<String, Object> attributes = parameters.getMap(QueryParams.ATTRIBUTES.key());
        if (attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                projectParameters.put("projects.$.attributes." + entry.getKey(), entry.getValue());
            }
        }

        Object datastores = parameters.get(QueryParams.INTERNAL_DATASTORES_VARIANT.key());
        if (datastores != null) {
            if (datastores instanceof DataStore) {
                datastores = getMongoDBDocument(datastores, "Datastore");
            }
            projectParameters.put("projects.$." + QueryParams.INTERNAL_DATASTORES_VARIANT.key(), datastores);
        } else {
            datastores = parameters.get(QueryParams.INTERNAL_DATASTORES.key());
            if (datastores instanceof DataStore) {
                datastores = getMongoDBDocument(datastores, "Datastore");
            }
            if (datastores != null) {
                projectParameters.put("projects.$." + QueryParams.INTERNAL_DATASTORES.key(), datastores);
            }
        }

        if (parameters.containsKey(INTERNAL_STATUS_NAME.key())) {
            projectParameters.put("projects.$." + INTERNAL_STATUS_NAME.key(),
                    parameters.get(INTERNAL_STATUS_NAME.key()));
            projectParameters.put("projects.$." + QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!projectParameters.isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            projectParameters.put(QueryParams.MODIFICATION_DATE.key(), time);
            projectParameters.put(PRIVATE_MODIFICATION_DATE, date);
        }

        return projectParameters;
    }

    @Deprecated
    @Override
    public OpenCGAResult delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Use other delete method");
//        checkId(id);
//        // Check the project is active
//        Query query = new Query(QueryParams.UID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.READY);
//        if (count(query).first() == 0) {
//            query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
//            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_NAME.key());
//            Project project = get(query, options).first();
//            throw new CatalogDBException("The project {" + id + "} was already " + project.getStatus().getName());
//        }
//
//        // If we don't find the force parameter, we check first if the user does not have an active project.
//        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
//            checkCanDelete(id);
//        }
//
//        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
//            // Delete the active studies (if any)
//            query = new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), id);
//            dbAdaptorFactory.getCatalogStudyDBAdaptor().delete(query, queryOptions);
//        }
//
//        // Change the status of the project to deleted
//        return setStatus(id, Status.DELETED);
    }

    @Override
    public OpenCGAResult delete(Project project) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateDelete(clientSession, project));
        } catch (CatalogDBException e) {
            logger.error("Could not delete project {}: {}", project.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete project '" + project.getId() + "': " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        query.append(INTERNAL_STATUS_NAME.key(), Status.READY);
        OpenCGAResult<Project> projectDataResult = get(query, new QueryOptions(QueryOptions.INCLUDE, QueryParams.UID.key()));
        OpenCGAResult writeResult = new OpenCGAResult();
        for (Project project : projectDataResult.getResults()) {
            writeResult.append(delete(project.getUid(), queryOptions));
        }
        return writeResult;
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key()));
        DBIterator<Project> iterator = iterator(query, options);

        OpenCGAResult<Project> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Project project = iterator.next();

            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, project)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete project {}: {}", project.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, project.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Project> privateDelete(ClientSession clientSession, Project project) throws CatalogDBException {
        long tmpStartTime = startQuery();
        logger.debug("Deleting project {} ({})", project.getId(), project.getUid());

        StudyMongoDBAdaptor studyDBAdaptor = dbAdaptorFactory.getCatalogStudyDBAdaptor();

        // First, we delete the studies
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), project.getUid());
        List<Document> studyList = studyDBAdaptor.nativeGet(studyQuery, QueryOptions.empty()).getResults();
        if (studyList != null) {
            for (Document study : studyList) {
                studyDBAdaptor.privateDelete(clientSession, study);
            }
        }

        String deleteSuffix = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

        // Mark the study as deleted
        ObjectMap updateParams = new ObjectMap()
                .append(INTERNAL_STATUS_NAME.key(), Status.DELETED)
                .append(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime())
                .append(QueryParams.ID.key(), project.getId() + deleteSuffix);

        Bson bsonQuery = parseQuery(studyQuery);
        Document updateDocument = getDocumentUpdateParams(updateParams);

        logger.debug("Delete project {}: Query: {}, update: {}", project.getId(),
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = userCollection.update(clientSession, bsonQuery, updateDocument, QueryOptions.empty());

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Project " + project.getId() + " not found");
        }
        List<Event> events = new ArrayList<>();
        if (result.getNumUpdated() == 0) {
            events.add(new Event(Event.Type.WARNING, project.getId(), "Project was already deleted"));
        }
        logger.debug("Project {} successfully deleted", project.getId());

        return endWrite(tmpStartTime, 1, 0, 0, 1, events);
    }

    OpenCGAResult setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(INTERNAL_STATUS_NAME.key(), status), QueryOptions.empty());
    }

    private OpenCGAResult setStatus(long projectId, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(projectId, new ObjectMap(INTERNAL_STATUS_NAME.key(), status), QueryOptions.empty());
    }

    @Override
    public OpenCGAResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public OpenCGAResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        query.put(INTERNAL_STATUS_NAME.key(), Status.DELETED);
        return setStatus(query, Status.READY);
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(INTERNAL_STATUS_NAME.key(), Status.DELETED);
        if (count(query).getNumMatches() == 0) {
            throw new CatalogDBException("The project {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        return setStatus(id, Status.READY);
    }

    @Override
    public OpenCGAResult<Project> get(String userId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.USER_ID.key(), userId);
        return endQuery(startTime, get(query, options));
    }

    @Override
    public OpenCGAResult<Project> get(long projectId, QueryOptions options) throws CatalogDBException {
        checkId(projectId);
        Query query = new Query(QueryParams.UID.key(), projectId).append(INTERNAL_STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
//        // Fixme: Check the code below
//        List<Project> projects = user.getProjects();
//        joinFields(projects.get(0), options);
//
//        return endQuery("Get project", startTime, projects);
    }

    @Override
    public OpenCGAResult<Project> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    OpenCGAResult<Project> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        OpenCGAResult<Project> queryResult;
        try (DBIterator<Project> dbIterator = iterator(clientSession, query, options)) {
            queryResult = endQuery(startTime, dbIterator);
        }

        if (options == null || !options.containsKey(QueryOptions.EXCLUDE)
                || (!options.getAsStringList(QueryOptions.EXCLUDE).contains("projects.studies")
                && !options.getAsStringList(QueryOptions.EXCLUDE).contains("studies"))) {
            QueryOptions studyOptions = options == null ? QueryOptions.empty() : extractNestedOptions(options, "studies");
            for (Project project : queryResult.getResults()) {
                Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), project.getUid());
                try {
                    OpenCGAResult<Study> studyDataResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(clientSession, studyQuery,
                            studyOptions);
                    project.setStudies(studyDataResult.getResults());
                } catch (CatalogDBException e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        }

        return queryResult;
    }

    @Override
    public OpenCGAResult<Project> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogParameterException {
        long startTime = startQuery();
        OpenCGAResult<Project> queryResult;
        try (DBIterator<Project> dbIterator = iterator(query, options, user)) {
            queryResult = endQuery(startTime, dbIterator);
        } catch (CatalogAuthorizationException e) {
            // We don't want to raise permission exceptions in methods where general lookups are done. That should only apply if you specify
            queryResult = OpenCGAResult.empty(Project.class);
            queryResult.setEvents(Collections.singletonList(new Event(Event.Type.ERROR, e.getMessage())));
        }

        if (options == null || !options.containsKey(QueryOptions.EXCLUDE)
                || (!options.getAsStringList(QueryOptions.EXCLUDE).contains("projects.studies")
                && !options.getAsStringList(QueryOptions.EXCLUDE).contains("studies"))) {
            for (Project project : queryResult.getResults()) {
                Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), project.getUid());
                try {
                    OpenCGAResult<Study> studyDataResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(studyQuery, options, user);
                    project.setStudies(studyDataResult.getResults());
                } catch (CatalogDBException | CatalogAuthorizationException e) {
                    logger.error("{}", e.getMessage(), e);
                    queryResult.setEvents(Collections.singletonList(new Event(Event.Type.WARNING, e.getMessage())));
                }
            }
        }

        return queryResult;
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Project> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    public DBIterator<Project> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, projectConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions);
        return new CatalogMongoDBIterator<>(mongoCursor);
    }

    @Override
    public DBIterator<Project> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        return new CatalogMongoDBIterator<>(mongoCursor, projectConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, user);
        return new CatalogMongoDBIterator(mongoCursor);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
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
        OpenCGAResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery,
                new QueryOptions(), user);

        query.remove(QueryParams.STUDY_UID.key());
        query.remove(QueryParams.STUDY_ID.key());

        // We build a map of projectId - list<studies>
//        Map<Long, List<Study>> studyMap = new HashMap<>();
//        StudyConverter studyConverter = new StudyConverter();
//        for (Document studyDocument : queryResult.getResults()) {
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
            if (dbAdaptorFactory.getCatalogStudyDBAdaptor().count(clientSession, studyQuery).getNumMatches() == 0) {
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
            projectUids = queryResult.getResults().stream()
                    .map(document -> ((Document) document.get("_project")).getLong("uid"))
                    .collect(Collectors.toList());
        }
        if (projectUids != null && !projectUids.isEmpty()) {
            query.put(QueryParams.UID.key(), projectUids);
        }

        return getMongoCursor(clientSession, query, options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {

        if (!query.containsKey(INTERNAL_STATUS_NAME.key())) {
            query.append(INTERNAL_STATUS_NAME.key(), "!=" + Status.DELETED);
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

        return userCollection.iterator(clientSession, aggregates, qOptions);
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) {
        throw new NotImplementedException("Rank project is not implemented");
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options) {
        throw new NotImplementedException("GroupBy in project is not implemented");
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) {
        throw new NotImplementedException("GroupBy in project is not implemented");
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
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Status.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery("projects." + INTERNAL_STATUS_NAME.key(), queryParam.key(), query, INTERNAL_STATUS_NAME.type(),
                                andBsonList);
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
                    case ORGANISM_ASSEMBLY:
                    case INTERNAL_STATUS_MSG:
                    case INTERNAL_STATUS_DATE:
                    case INTERNAL_DATASTORES:
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
                .append(StudyDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Status.READY);
        Long count = dbAdaptorFactory.getCatalogStudyDBAdaptor().count(query).getNumMatches();
        if (count > 0) {
            throw new CatalogDBException("The project {" + projectId + "} cannot be deleted. The project has " + count
                    + " studies in use.");
        }
    }
}
