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

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ProjectConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ProjectCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.api.ProjectDBAdaptor.QueryParams.INTERNAL_STATUS_ID;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by imedina on 08/01/16.
 */
public class ProjectMongoDBAdaptor extends MongoDBAdaptor implements ProjectDBAdaptor {

    private final MongoDBCollection projectCollection;
    private final MongoDBCollection deletedProjectCollection;
    private ProjectConverter projectConverter;

    public ProjectMongoDBAdaptor(MongoDBCollection projectCollection, MongoDBCollection deletedProjectCollection,
                                 Configuration configuration, OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(ProjectMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.projectCollection = projectCollection;
        this.deletedProjectCollection = deletedProjectCollection;
        this.projectConverter = new ProjectConverter();
    }

    @Override
    public boolean exists(long projectUid) {
        DataResult<Long> count = projectCollection.count(new Document(QueryParams.UID.key(), projectUid));
        return count.getNumMatches() != 0;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> project, String userId) throws CatalogDBException {
        Document projectDocument = getMongoDBDocument(project, "project");
        return new OpenCGAResult(projectCollection.insert(projectDocument, null));
    }

    @Override
    public OpenCGAResult<Project> insert(Project project, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting project insert transaction for project id '{}'", project.getId());

            insert(clientSession, project);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create project {}: {}", project.getId(), e.getMessage()));
    }

    Project insert(ClientSession clientSession, Project project)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (project.getStudies() != null && !project.getStudies().isEmpty()) {
            throw new CatalogParameterException("Creating project and studies in a single transaction is forbidden");
        }
        project.setStudies(Collections.emptyList());

        Bson countQuery = Filters.eq(QueryParams.ID.key(), project.getId());
        DataResult<Long> count = projectCollection.count(clientSession, countQuery);
        if (count.getNumMatches() != 0) {
            throw new CatalogDBException("Project {id:\"" + project.getId() + "\"} already exists in this organization");
        }

        long projectUid = getNewUid(clientSession);
        project.setUid(projectUid);
        if (StringUtils.isEmpty(project.getUuid())) {
            project.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PROJECT));
        }

        Document projectDocument = projectConverter.convertToStorageType(project);
        projectDocument.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(project.getCreationDate()) ? TimeUtils.toDate(project.getCreationDate()) : TimeUtils.getDate());
        projectDocument.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(project.getModificationDate())
                ? TimeUtils.toDate(project.getModificationDate()) : TimeUtils.getDate());

        projectCollection.insert(clientSession, projectDocument, null);
        return project;
    }

    @Override
    public OpenCGAResult incrementCurrentRelease(long projectId) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), projectId);
        Bson update = new Document("$inc", new Document(QueryParams.CURRENT_RELEASE.key(), 1));

        DataResult updateQR = projectCollection.update(parseQuery(query), update, null);
        if (updateQR == null || updateQR.getNumMatches() == 0) {
            throw new CatalogDBException("Could not increment release number. Project id " + projectId + " not found");
        } else if (updateQR.getNumUpdated() == 0) {
            throw new CatalogDBException("Internal error. Current release number could not be incremented.");
        }
        return new OpenCGAResult(updateQR);
    }

    private void editId(ClientSession clientSession, String organizationId, long projectUid, String newId) throws CatalogDBException {
//        // Check new id is not in use
        Query query = new Query(QueryParams.ID.key(), newId);
        if (count(clientSession, query).getNumMatches() > 0) {
            throw new CatalogDBException("Project {id:\"" + newId + "\"} already exists");
        }

        Bson bsonQuery = Filters.eq(QueryParams.UID.key(), projectUid);
        Bson update = new Document("$set", new Document()
                .append(QueryParams.ID.key(), newId)
                .append(QueryParams.FQN.key(), FqnUtils.buildFqn(organizationId, newId))
        );
        DataResult result = projectCollection.update(clientSession, bsonQuery, update, null);
        if (result.getNumUpdated() == 0) {    //Check if the the project id was modified
            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Project {uid:\"" + projectUid + "\"} not found.");
            } else {
                throw new CatalogDBException("Project {id:\"" + newId + "\"} already exists");
            }
        }

        // Update all the internal project ids stored in the study documents
        dbAdaptorFactory.getCatalogStudyDBAdaptor().updateProjectId(clientSession, projectUid, newId);
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(projectCollection.count(bson));
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(projectCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user, StudyPermissions.Permissions studyPermission) throws CatalogDBException {
        throw new NotImplementedException("Count not implemented for projects");
    }

    @Override
    public OpenCGAResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult(projectCollection.distinct(field, bson));
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
        UpdateDocument updateParams = getDocumentUpdateParams(parameters);
        Document finalUpdateDocument = updateParams.toFinalUpdateDocument();

        if (finalUpdateDocument.isEmpty() && !parameters.containsKey(QueryParams.ID.key())) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        long tmpStartTime = startQuery();
        if (parameters.containsKey(QueryParams.ID.key())) {
            logger.debug("Update project id '{}'({}) to new id '{}'", project.getId(), project.getUid(),
                    parameters.getString(QueryParams.ID.key()));
            FqnUtils.FQN fqn = FqnUtils.parse(project.getFqn());
            editId(clientSession, fqn.getOrganization(), project.getUid(), parameters.getString(QueryParams.ID.key()));
        }

        if (!finalUpdateDocument.isEmpty()) {
            Query tmpQuery = new Query(QueryParams.UID.key(), project.getUid());
            Bson finalQuery = parseQuery(tmpQuery);

            logger.debug("Update project. Query: {}, update: {}", finalQuery.toBsonDocument(), finalUpdateDocument.toBsonDocument());
            DataResult result = projectCollection.update(clientSession, finalQuery, finalUpdateDocument, null);

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

    UpdateDocument getDocumentUpdateParams(ObjectMap parameters) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.ORGANISM_SCIENTIFIC_NAME.key(), QueryParams.ORGANISM_COMMON_NAME.key(), QueryParams.ORGANISM_ASSEMBLY.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.INTERNAL_STATUS.key(), QueryParams.CELLBASE.key(),
                QueryParams.INTERNAL_DATASTORES_VARIANT.key(), QueryParams.INTERNAL_DATASTORES.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(QueryParams.MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.ORGANISM_SCIENTIFIC_NAME.key(), QueryParams.ORGANISM_COMMON_NAME.key(), QueryParams.ORGANISM_ASSEMBLY.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.CELLBASE.key(), QueryParams.INTERNAL_DATASTORES_VARIANT.key(),
                QueryParams.INTERNAL_DATASTORES.key(), QueryParams.INTERNAL_STATUS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(QueryParams.MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
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
        query.append(INTERNAL_STATUS_ID.key(), InternalStatus.READY);
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
        List<Document> studyList = studyDBAdaptor.nativeGet(clientSession, studyQuery, QueryOptions.empty()).getResults();
        if (studyList != null) {
            for (Document study : studyList) {
                studyDBAdaptor.privateDelete(clientSession, study);
            }
        }

        String deleteSuffix = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

        // Mark the study as deleted
        ObjectMap updateParams = new ObjectMap()
                .append(INTERNAL_STATUS_ID.key(), InternalStatus.DELETED)
                .append(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime())
                .append(QueryParams.ID.key(), project.getId() + deleteSuffix);

        DataResult result = privateUpdate(clientSession, project, updateParams);
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
        return update(query, new ObjectMap(INTERNAL_STATUS_ID.key(), status), QueryOptions.empty());
    }

    private OpenCGAResult setStatus(long projectId, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(projectId, new ObjectMap(INTERNAL_STATUS_ID.key(), status), QueryOptions.empty());
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
        query.put(INTERNAL_STATUS_ID.key(), InternalStatus.DELETED);
        return setStatus(query, InternalStatus.READY);
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(INTERNAL_STATUS_ID.key(), InternalStatus.DELETED);
        if (count(query).getNumMatches() == 0) {
            throw new CatalogDBException("The project {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        return setStatus(id, InternalStatus.READY);
    }

    @Override
    public OpenCGAResult<Project> get(long projectId, QueryOptions options) throws CatalogDBException {
        checkId(projectId);
        Query query = new Query(QueryParams.UID.key(), projectId);
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
//            queryResult.setEvents(Collections.singletonList(new Event(Event.Type.ERROR, e.getMessage())));
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
        return new ProjectCatalogMongoDBIterator<>(mongoCursor, clientSession, projectConverter, dbAdaptorFactory, options, null);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions);
        return new ProjectCatalogMongoDBIterator<>(mongoCursor, null, null, dbAdaptorFactory, options, null);
    }

    @Override
    public DBIterator<Project> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        return new ProjectCatalogMongoDBIterator<>(mongoCursor, null, projectConverter, dbAdaptorFactory, options, user);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, user);
        return new ProjectCatalogMongoDBIterator<>(mongoCursor, null, null, dbAdaptorFactory, options, user);
    }

//    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
//            throws CatalogDBException, CatalogAuthorizationException {
//        List<Long> studyUids = query.getAsLongList(QueryParams.STUDY_UID.key());
//        if (!studyUids.isEmpty()) {
//            query.remove(QueryParams.STUDY_UID.key());
//            Query studyQuery = new Query(PRIVATE_UID, studyUids);
//            QueryOptions studyOptions = new QueryOptions(QueryOptions.INCLUDE, PRIVATE_PROJECT);
//            OpenCGAResult<Document> result = dbAdaptorFactory.getCatalogStudyDBAdaptor()
//                    .nativeGet(clientSession, studyQuery, studyOptions, user);
//            if (result.getNumResults() == 0) {
//                return new MongoDBIterator<>(MongoDBIterator.EMPTY_MONGO_CURSOR_ITERATOR, 0);
//            }
//            // Add all project uids to the main query parameter
//            query.put(PRIVATE_UID,
//                    result.getResults()
//                            .stream()
//                            .map(x -> x.get(PRIVATE_PROJECT, Document.class))
//                            .map(x -> x.get(PRIVATE_UID, Long.class))
//                            .distinct()
//                            .collect(Collectors.toList()));
//        }
//
//        String requestedUser = query.getString(QueryParams.USER_ID.key());
//        if (StringUtils.isNotEmpty(requestedUser)) {
//            if (requestedUser.equals(user)) {
//                // My own projects (no permissions check)
//                return getMongoCursor(clientSession, query, options);
//            } else {
//                // Other user projects (permissions check)
//                List<Long> projectUids = query.getAsLongList(QueryParams.UID.key());
//                Query studyQuery = new Query();
//                if (projectUids != null && projectUids.size() > 0) {
//                    studyQuery.append(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), projectUids);
//                }
//                List<String> projectIds = query.getAsStringList(QueryParams.ID.key());
//                if (projectIds != null && projectIds.size() > 0) {
//                    studyQuery.append(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectIds);
//                }
//
//                studyQuery.putIfNotEmpty(StudyDBAdaptor.QueryParams.UID.key(), query.getString(QueryParams.STUDY_UID.key()));
//                studyQuery.putIfNotEmpty(StudyDBAdaptor.QueryParams.ID.key(), query.getString(QueryParams.STUDY_ID.key()));
//                studyQuery.putIfNotEmpty(StudyDBAdaptor.QueryParams.OWNER.key(), query.getString(QueryParams.USER_ID.key()));
//                OpenCGAResult<Document> studiesResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery,
//                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), user);
//
//                if (studiesResult.getNumResults() > 0) {
//                    Set<String> projectFqn = new HashSet<>();
//                    for (Document study : studiesResult.getResults()) {
//                        String studyFqn = study.getString(StudyDBAdaptor.QueryParams.FQN.key());
//                        projectFqn.add(FqnUtils.toProjectFqn(studyFqn));
//                    }
//                    query.put(QueryParams.FQN.key(), new ArrayList<>(projectFqn));
//                    return getMongoCursor(clientSession, query, options);
//                } else {
//                    return new MongoDBIterator<>(MongoDBIterator.EMPTY_MONGO_CURSOR_ITERATOR, 0);
//                }
//
//            }
//        } else {
//            // 1. Get all projects matching the query and extract own projects and external projects
//            MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query,
//                    new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(QueryParams.FQN.key(), QueryParams.UID.key())));
//            ProjectCatalogMongoDBIterator<Project> iterator = new ProjectCatalogMongoDBIterator<>(mongoCursor, clientSession,
//                    projectConverter, dbAdaptorFactory, options, user);
//            List<String> ownerFqns = new ArrayList<>();  // FQNs from projects owned by the user "user"
//            List<Long> externalUids = new ArrayList<>(); // Project uids from projects owned by a user other than "user"
//            while (iterator.hasNext()) {
//                Project project = iterator.next();
//                if (FqnUtils.getUser(project.getFqn()).equals(user)) {
//                    ownerFqns.add(project.getFqn());
//                } else {
//                    externalUids.add(project.getUid());
//                }
//            }
//
//            // 2. Extract external projects and check permissions by querying their studies
//            List<String> externalFqns = new ArrayList<>();
//            if (!externalUids.isEmpty()) {
//                Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), externalUids);
//                OpenCGAResult<Document> studiesResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery,
//                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), user);
//                if (studiesResult.getNumResults() > 0) {
//                    Set<String> projectFqn = new HashSet<>();
//                    for (Document study : studiesResult.getResults()) {
//                        String studyFqn = study.getString(StudyDBAdaptor.QueryParams.FQN.key());
//                        projectFqn.add(FqnUtils.toProjectFqn(studyFqn));
//                    }
//                    externalFqns.addAll(projectFqn);
//                }
//            }
//
//            // 3. Query based on the final projects the user can see
//            List<String> allFqns = new ArrayList<>(ownerFqns.size() + externalFqns.size());
//            allFqns.addAll(ownerFqns);
//            allFqns.addAll(externalFqns);
//            query.put(QueryParams.FQN.key(), allFqns);
//
//            return getMongoCursor(clientSession, query, options);
//        }
//    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        List<Long> studyUids = query.getAsLongList(QueryParams.STUDY_UID.key());
        if (!studyUids.isEmpty()) {
            query.remove(QueryParams.STUDY_UID.key());
            Query studyQuery = new Query(PRIVATE_UID, studyUids);
            QueryOptions studyOptions = new QueryOptions(QueryOptions.INCLUDE, PRIVATE_PROJECT);
            OpenCGAResult<Document> result = dbAdaptorFactory.getCatalogStudyDBAdaptor()
                    .nativeGet(clientSession, studyQuery, studyOptions, user);
            if (result.getNumResults() == 0) {
                return new MongoDBIterator<>(MongoDBIterator.EMPTY_MONGO_CURSOR_ITERATOR, 0);
            }
            // Add all project uids to the main query parameter
            query.put(PRIVATE_UID,
                    result.getResults()
                            .stream()
                            .map(x -> x.get(PRIVATE_PROJECT, Document.class))
                            .map(x -> x.get(PRIVATE_UID, Long.class))
                            .distinct()
                            .collect(Collectors.toList()));
        }

        options = filterQueryOptionsToIncludeKeys(options, Arrays.asList(QueryParams.ID.key(), QueryParams.FQN.key()));

        // 0. Check if the user is the owner or one of the organization admins
        boolean isOwnerOrAdmin = dbAdaptorFactory.getCatalogOrganizationDBAdaptor().isOwnerOrAdmin(clientSession, user);

        if (isOwnerOrAdmin) {
            return getMongoCursor(clientSession, query, options);
        } else {
            // 1. Get all projects matching the query
            List<Long> projectUids = new ArrayList<>();
            try (DBIterator<Project> iterator = iterator(clientSession, query,
                    new QueryOptions(QueryOptions.INCLUDE, Collections.singletonList(QueryParams.UID.key())))) {
                while (iterator.hasNext()) {
                    Project project = iterator.next();
                    projectUids.add(project.getUid());
                }
            }

            // 2. Extract project fqns that are allowed to see by the user
            List<String> allowedFqns = new ArrayList<>();
            if (!projectUids.isEmpty()) {
                Query studyQuery = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), projectUids);
                OpenCGAResult<Document> studiesResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery,
                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), user);
                if (studiesResult.getNumResults() > 0) {
                    Set<String> projectFqn = new HashSet<>();
                    for (Document study : studiesResult.getResults()) {
                        String studyFqn = study.getString(StudyDBAdaptor.QueryParams.FQN.key());
                        projectFqn.add(FqnUtils.toProjectFqn(studyFqn));
                    }
                    allowedFqns.addAll(projectFqn);
                }
            }

            // 3. Query based on the final projects the user can see
            if (!allowedFqns.isEmpty()) {
                query.put(QueryParams.FQN.key(), allowedFqns);
                return getMongoCursor(clientSession, query, options);
            } else {
                return new MongoDBIterator<>(MongoDBIterator.EMPTY_MONGO_CURSOR_ITERATOR, 0);
            }
        }
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);

        // Check include
        QueryOptions qOptions = filterQueryOptionsToIncludeKeys(options, Arrays.asList(QueryParams.UID.key(), QueryParams.FQN.key()));

        MongoDBCollection collection = getQueryCollection(query, projectCollection, null, deletedProjectCollection);
        logger.debug("Project query: {}", bsonQuery.toBsonDocument());
        return collection.iterator(clientSession, bsonQuery, null, null, qOptions);
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
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(InternalStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(INTERNAL_STATUS_ID.key(), queryParam.key(), query, INTERNAL_STATUS_ID.type(), andBsonList);
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
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
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
                .append(StudyDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), InternalStatus.READY);
        Long count = dbAdaptorFactory.getCatalogStudyDBAdaptor().count(query).getNumMatches();
        if (count > 0) {
            throw new CatalogDBException("The project {" + projectId + "} cannot be deleted. The project has " + count
                    + " studies in use.");
        }
    }
}
