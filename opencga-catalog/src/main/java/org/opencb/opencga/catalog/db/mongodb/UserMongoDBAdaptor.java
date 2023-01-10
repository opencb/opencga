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
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.UserConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserFilter;
import org.opencb.opencga.core.models.user.UserStatus;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.UserDBAdaptor.QueryParams.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserMongoDBAdaptor extends MongoDBAdaptor implements UserDBAdaptor {

    private final MongoDBCollection userCollection;
    private final MongoDBCollection deletedUserCollection;
    private UserConverter userConverter;

    private static final String PRIVATE_PASSWORD = "_password";

    public UserMongoDBAdaptor(MongoDBCollection userCollection, MongoDBCollection deletedUserCollection, Configuration configuration,
                              MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(UserMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.userCollection = userCollection;
        this.deletedUserCollection = deletedUserCollection;
        this.userConverter = new UserConverter();
    }

    /*
     * *************************
     * User methods
     * ***************************
     */

    boolean exists(ClientSession clientSession, String userId) throws CatalogDBException {
        return count(clientSession, new Query(QueryParams.ID.key(), userId)).getNumMatches() > 0;
    }

    @Override
    public OpenCGAResult insert(User user, String password, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();

            logger.debug("Starting user insert transaction for user id '{}'", user.getId());
            insert(clientSession, user, password);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create user {}: {}", user.getId(), e.getMessage()));
    }

    private void insert(ClientSession clientSession, User user, String password) throws CatalogDBException, CatalogParameterException {
        checkParameter(user, "user");
        if (exists(clientSession, user.getId())) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        if (user.getProjects() != null && !user.getProjects().isEmpty()) {
            throw new CatalogParameterException("Creating user and projects in a single transaction is forbidden");
        }
        user.setProjects(Collections.emptyList());

        Document userDocument = userConverter.convertToStorageType(user);
        userDocument.append(ID, user.getId());
        userDocument.append(PRIVATE_PASSWORD, encryptPassword(password));

        userCollection.insert(clientSession, userDocument, null);
    }

    @Override
    public OpenCGAResult<User> get(String userId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.ID.key(), userId);
        return get(query, options);
    }

    @Override
    public OpenCGAResult changePassword(String userId, String oldPassword, String newPassword)
            throws CatalogDBException, CatalogAuthenticationException {
        Document bson = new Document(ID, userId)
                .append(PRIVATE_PASSWORD, encryptPassword(oldPassword));
        Bson set = Updates.set(PRIVATE_PASSWORD, encryptPassword(newPassword));

        DataResult result = userCollection.update(bson, set, null);
        if (result.getNumUpdated() == 0) {  //0 query matches.
            throw CatalogAuthenticationException.incorrectUserOrPassword("Internal");
        }
        return new OpenCGAResult(result);
    }

    @Override
    public void authenticate(String userId, String password) throws CatalogAuthenticationException {
        Document bson;
        try {
            bson = new Document()
                    .append(ID, userId)
                    .append(PRIVATE_PASSWORD, encryptPassword(password));
        } catch (CatalogDBException e) {
            throw new CatalogAuthenticationException("Could not encrypt password: " + e.getMessage(), e);
        }
        if (userCollection.count(bson).getNumMatches() == 0) {
            throw CatalogAuthenticationException.incorrectUserOrPassword("Internal");
        }
    }

    @Override
    public OpenCGAResult resetPassword(String userId, String email, String newPassword) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), userId);
        query.append(QueryParams.EMAIL.key(), email);
        Bson bson = parseQuery(query);

        Bson set = Updates.set(PRIVATE_PASSWORD, encryptPassword(newPassword));

        DataResult result = userCollection.update(bson, set, null);
        if (result.getNumUpdated() == 0) {  //0 query matches.
            throw new CatalogDBException("Bad user or email");
        }
        return new OpenCGAResult(result);
    }

    @Override
    public OpenCGAResult setConfig(String userId, String name, Map<String, Object> config) throws CatalogDBException {
        // Set the config
        Bson bsonQuery = Filters.eq(QueryParams.ID.key(), userId);
        Bson filterDocument = getMongoDBDocument(config, "Config");
        Bson update = Updates.set(QueryParams.CONFIGS.key() + "." + name, filterDocument);

        DataResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not create " + name + " configuration ");
        }
        return new OpenCGAResult(result);
    }

    @Override
    public OpenCGAResult deleteConfig(String userId, String name) throws CatalogDBException {
        // Insert the config
        Bson bsonQuery = Filters.and(
                Filters.eq(QueryParams.ID.key(), userId),
                Filters.exists(QueryParams.CONFIGS.key() + "." + name)
        );
        Bson update = Updates.unset(QueryParams.CONFIGS.key() + "." + name);

        DataResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not delete " + name + " configuration ");
        }
        return new OpenCGAResult(result);
    }

    @Override
    public OpenCGAResult addFilter(String userId, UserFilter filter) throws CatalogDBException {
        // Insert the filter
        Bson bsonQuery = Filters.and(
                Filters.eq(QueryParams.ID.key(), userId),
                Filters.ne(QueryParams.FILTERS_ID.key(), filter.getId())
        );
        Bson filterDocument = getMongoDBDocument(filter, "Filter");
        Bson update = Updates.push(QueryParams.FILTERS.key(), filterDocument);

        DataResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() != 1) {
            if (result.getNumUpdated() == 0) {
                throw new CatalogDBException("Internal error: The filter could not be stored.");
            } else {
                // This error should NEVER be raised.
                throw new CatalogDBException("User: There was a critical error when storing the filter. Is has been inserted "
                        + result.getNumUpdated() + " times.");
            }
        }
        return new OpenCGAResult(result);
    }

    @Override
    public OpenCGAResult updateFilter(String userId, String name, ObjectMap params) throws CatalogDBException {
        if (params.isEmpty()) {
            throw new CatalogDBException("Nothing to be updated. No parameters were passed.");
        }

        final String prefixUpdate = FILTERS.key() + ".$.";
        Document parameters = new Document();

        if (params.get(FilterParams.DESCRIPTION.key()) != null) {
            parameters.put(prefixUpdate + FilterParams.DESCRIPTION.key(), params.get(FilterParams.DESCRIPTION.key()));
        }

        if (params.get(FilterParams.RESOURCE.key()) != null) {
            parameters.put(prefixUpdate + FilterParams.RESOURCE.key(), params.get(FilterParams.RESOURCE.key()).toString());
        }

        if (params.get(FilterParams.QUERY.key()) != null) {
            parameters.put(prefixUpdate + FilterParams.QUERY.key(), getMongoDBDocument(params.get(FilterParams.QUERY.key()), "Query"));
        }

        if (params.get(FilterParams.OPTIONS.key()) != null) {
            parameters.put(prefixUpdate + FilterParams.OPTIONS.key(), getMongoDBDocument(params.get(FilterParams.OPTIONS.key()),
                    "Options"));
        }

        if (parameters.isEmpty()) {
            throw new CatalogDBException("Nothing to be updated. Parameters were not recognised.");
        }

        Query query = new Query()
                .append(ID, userId)
                .append(FILTERS_ID.key(), name);
        return new OpenCGAResult(userCollection.update(parseQuery(query), new Document("$set", parameters), null));
    }

    @Override
    public OpenCGAResult deleteFilter(String userId, String name) throws CatalogDBException {
        // Delete the filter
        Bson bsonQuery = Filters.and(
                Filters.eq(QueryParams.ID.key(), userId),
                Filters.eq(QueryParams.FILTERS_ID.key(), name)
        );
        Bson update = Updates.pull(QueryParams.FILTERS.key(), new Document(FilterParams.ID.key(), name));
        DataResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Internal error: Filter " + name + " could not be removed");
        }

        return new OpenCGAResult(result);
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        logger.debug("User count: {}", bsonDocument.toBsonDocument());
        return new OpenCGAResult<>(userCollection.count(clientSession, bsonDocument));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<User> get(Query query, QueryOptions options) throws CatalogDBException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        Bson bson = parseQuery(query);
        QueryOptions userOptions;
        if (includeProjects(options)) {
            userOptions = filterQueryOptions(options, Arrays.asList(ID, PROJECTS_UID.key()));
        } else {
            userOptions = filterQueryOptions(options, Collections.singletonList(ID));
        }
        DataResult<User> userDataResult = userCollection.find(bson, null, userConverter, userOptions);

        if (includeStudies(options)) {
            for (User user : userDataResult.getResults()) {
                if (user.getProjects() != null) {
                    for (Project project : user.getProjects()) {
                        Query query1 = new Query(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), project.getUid());
                        QueryOptions studyOptions = extractNestedOptions(options, PROJECTS.key() + ".studies.");
                        OpenCGAResult<Study> studyResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(query1, studyOptions);
                        project.setStudies(studyResult.getResults());
                    }
                }
            }
        }

        if (includeSharedProjects(options)) {
            QueryOptions sharedProjectOptions = extractNestedOptions(options, SHARED_PROJECTS.key());
            sharedProjectOptions = filterQueryOptions(sharedProjectOptions, Arrays.asList(ProjectDBAdaptor.QueryParams.FQN.key(),
                    "studies." + StudyDBAdaptor.QueryParams.FQN.key(), "studies." + StudyDBAdaptor.QueryParams.GROUPS.key()));
            extractSharedProjects(userDataResult, sharedProjectOptions);
        }

        return new OpenCGAResult<>(userDataResult);
    }

    private boolean includeProjects(QueryOptions options) {
        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
        List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);

        if (!includeList.isEmpty()) {
            for (String includeKey : includeList) {
                if (includeKey.startsWith(PROJECTS.key() + ".")) {
                    return true;
                }
            }
            return false;
        } else if (!excludeList.isEmpty()) {
            for (String excludeKey : excludeList) {
                if (excludeKey.equals(PROJECTS.key())) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean includeSharedProjects(QueryOptions options) {
        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
        List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);

        if (!includeList.isEmpty()) {
            for (String includeKey : includeList) {
                if (includeKey.startsWith(SHARED_PROJECTS.key() + ".")) {
                    return true;
                }
            }
            return false;
        } else if (!excludeList.isEmpty()) {
            for (String excludeKey : excludeList) {
                if (excludeKey.equals(SHARED_PROJECTS.key())) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean includeStudies(QueryOptions options) {
        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
        List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);

        if (!includeList.isEmpty()) {
            for (String includeKey : includeList) {
                if (includeKey.startsWith(PROJECTS.key() + ".studies.")) {
                    return true;
                }
            }
            return false;
        } else if (!excludeList.isEmpty()) {
            for (String excludeKey : excludeList) {
                if (excludeKey.equals(PROJECTS.key() + ".studies")) {
                    return false;
                }
            }
        }

        return true;
    }

    private void extractSharedProjects(DataResult<User> userDataResult, QueryOptions options) throws CatalogDBException {
        Set<String> users = userDataResult.getResults().stream().map(User::getId).collect(Collectors.toSet());

        Map<String, Project> projectMap = new HashMap<>();
        Map<String, Study> studyMap = new HashMap<>();
        Map<String, String> studyProjectMap = new HashMap<>();
        Map<String, List<String>> userStudyMap = new HashMap<>();
        OpenCGAResult<Project> result = dbAdaptorFactory.getCatalogProjectDbAdaptor().get(new Query(), options);
        for (Project project : result.getResults()) {
            projectMap.put(project.getFqn(), project);
            if (project.getStudies() != null) {
                for (Study study : project.getStudies()) {
                    studyMap.put(study.getFqn(), study);
                    studyProjectMap.put(study.getFqn(), project.getFqn());

                    String owner = study.getFqn().split("@")[0];

                    if (study.getGroups() != null) {
                        for (Group group : study.getGroups()) {
                            if (StudyManager.MEMBERS.equals(group.getId())) {
                                // Add all the users that should be able to see the study to the map
                                for (String userId : group.getUserIds()) {
                                    // Exclude owner of the project
                                    if (!owner.equals(userId)) {
                                        if (users.contains(userId)) {
                                            if (!userStudyMap.containsKey(userId)) {
                                                userStudyMap.put(userId, new ArrayList<>());
                                            }
                                            userStudyMap.get(userId).add(study.getFqn());
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Add SharedProject information
        for (User user : userDataResult.getResults()) {
            if (userStudyMap.containsKey(user.getId())) {
                Map<String, List<Study>> projectStudyMap = new HashMap<>();

                for (String studyFqn : userStudyMap.get(user.getId())) {
                    // Obtain the project fqn where the study belongs
                    String projectFqn = studyProjectMap.get(studyFqn);
                    if (!projectStudyMap.containsKey(projectFqn)) {
                        projectStudyMap.put(projectFqn, new ArrayList<>());
                    }
                    projectStudyMap.get(projectFqn).add(studyMap.get(studyFqn));
                }

                List<Project> projectList = new ArrayList<>(projectStudyMap.size());
                for (Map.Entry<String, List<Study>> entry : projectStudyMap.entrySet()) {
                    Project project = new Project(projectMap.get(entry.getKey()));
                    project.setStudies(entry.getValue());
                    projectList.add(project);
                }

                user.setSharedProjects(projectList);
            }
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.INTERNAL_STATUS_ID.key())) {
            query.append(QueryParams.INTERNAL_STATUS_ID.key(), "!=" + InternalStatus.DELETED);
        }
        Bson bson = parseQuery(query);
        DataResult<Document> queryResult = userCollection.find(bson, options);

        for (Document user : queryResult.getResults()) {
            ArrayList<Document> projects = (ArrayList<Document>) user.get("projects");
            if (projects.size() > 0) {
                List<Document> projectsTmp = new ArrayList<>(projects.size());
                for (Document project : projects) {
                    Query query1 = new Query(ProjectDBAdaptor.QueryParams.UID.key(), project.get(ProjectDBAdaptor
                            .QueryParams.UID.key()));
                    OpenCGAResult<Document> queryResult1 = dbAdaptorFactory.getCatalogProjectDbAdaptor().nativeGet(query1, options);
                    projectsTmp.add(queryResult1.first());
                }
                user.remove("projects");
                user.append("projects", projectsTmp);
            }
        }

        return new OpenCGAResult(queryResult);
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        Map<String, Object> userParameters = new HashMap<>();

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.EMAIL.key(), QueryParams.ORGANIZATION.key()};
        filterStringParams(parameters, userParameters, acceptedParams);

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_ID.key())) {
            userParameters.put(QueryParams.INTERNAL_STATUS_ID.key(), parameters.get(QueryParams.INTERNAL_STATUS_ID.key()));
            userParameters.put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        final String[] acceptedLongParams = {QueryParams.QUOTA.key(), QueryParams.SIZE.key()};
        filterLongParams(parameters, userParameters, acceptedLongParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, userParameters, acceptedMapParams);

        if (!userParameters.isEmpty()) {
            return new OpenCGAResult(userCollection.update(parseQuery(query), new Document("$set", userParameters), null));
        }

        return OpenCGAResult.empty();
    }

    @Override
    public OpenCGAResult delete(User user) throws CatalogDBException {
        throw new NotImplementedException("Delete not implemented");
//        Query query = new Query(QueryParams.ID.key(), id);
//        delete(query);
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        throw new NotImplementedException("Delete not implemented");
//        OpenCGAResult<DeleteResult> remove = userCollection.remove(parseQuery(query), null);
//
//        if (remove.first().getDeletedCount() == 0) {
//            throw CatalogDBException.deleteError("User");
//        }
    }

    @Override
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Update user by int id. The id should be a string.");
    }

    public OpenCGAResult update(String userId, ObjectMap parameters)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(userId);
        Query query = new Query(QueryParams.ID.key(), userId);
        OpenCGAResult update = update(query, parameters, QueryOptions.empty());
        if (update.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not update user " + userId);
        }
        return update;
    }

    OpenCGAResult setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.INTERNAL_STATUS_ID.key(), status), QueryOptions.empty());
    }

    public OpenCGAResult setStatus(String userId, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(userId, new ObjectMap(QueryParams.INTERNAL_STATUS_ID.key(), status));
    }

    @Override
    public OpenCGAResult delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new CatalogDBException("Delete user by int id. The id should be a string.");
    }

    public OpenCGAResult delete(String id, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();

        checkId(id);
        // Check the user is active or banned
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.INTERNAL_STATUS_ID.key(), UserStatus.READY + "," + UserStatus.BANNED);
        if (count(query).getNumMatches() == 0) {
            query.put(QueryParams.INTERNAL_STATUS_ID.key(), UserStatus.DELETED);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.INTERNAL_STATUS_ID.key());
            User user = get(query, options).first();
            throw new CatalogDBException("The user {" + id + "} was already " + user.getInternal().getStatus().getId());
        }

        // If we don't find the force parameter, we check first if the user does not have an active project.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            // Delete the active projects (if any)
            query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), id);
            dbAdaptorFactory.getCatalogProjectDbAdaptor().delete(query, queryOptions);
        }

        // Change the status of the user to deleted
        return setStatus(id, UserStatus.DELETED);
    }

    /**
     * Checks whether the userId has any active project.
     *
     * @param userId user id.
     * @throws CatalogDBException when the user has active projects. Projects must be deleted first.
     */
    private void checkCanDelete(String userId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(userId);
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId)
                .append(ProjectDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), InternalStatus.READY);
        Long count = dbAdaptorFactory.getCatalogProjectDbAdaptor().count(query).getNumMatches();
        if (count > 0) {
            throw new CatalogDBException("The user {" + userId + "} cannot be deleted. The user has " + count + " projects in use.");
        }
    }

    @Override
    public OpenCGAResult delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
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
        query.put(QueryParams.INTERNAL_STATUS_ID.key(), InternalStatus.DELETED);
        return setStatus(query, InternalStatus.READY);
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new CatalogDBException("Delete user by int id. The id should be a string.");
    }

    public OpenCGAResult restore(String id, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(id);
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.INTERNAL_STATUS_ID.key(), InternalStatus.DELETED);
        if (count(query).getNumMatches() == 0) {
            throw new CatalogDBException("The user {" + id + "} is not deleted");
        }

        return setStatus(id, InternalStatus.READY);
    }

    /***
     * Removes completely the user from the database.
     * @param id User id to be removed from the database.
     * @return a OpenCGAResult object with the user removed.
     * @throws CatalogDBException when there is any problem during the removal.
     */
    public OpenCGAResult clean(String id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        Bson bson = parseQuery(query);

        DataResult result = userCollection.remove(bson, new QueryOptions());
        if (result.getNumDeleted() == 0) {
            throw CatalogDBException.idNotFound("User", query.getString(QueryParams.ID.key()));
        } else {
            return new OpenCGAResult(result);
        }
    }

    @Override
    public DBIterator<User> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoDBIterator<Document> iterator = userCollection.iterator(bson, options);
        return new CatalogMongoDBIterator<>(iterator, userConverter);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoDBIterator<Document> iterator = userCollection.iterator(bson, options);
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(userCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(userCollection, bsonQuery, field, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(userCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<User> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    public static void main(String[] args) throws CatalogDBException {
        System.out.println(encryptPassword("admin"));
    }

    private static String encryptPassword(String password) throws CatalogDBException {
        if (StringUtils.isNotEmpty(password)) {
            if (password.matches("^[a-fA-F0-9]{40}$")) {
                // Password already cyphered
                return password;
            }
            try {
                return CryptoUtils.sha1(password);
            } catch (NoSuchAlgorithmException e) {
                throw new CatalogDBException("Could not encrypt password", e);
            }
        } else {
            // Password will be empty when the user comes from an external authentication system
            return "";
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
                    case ID:
                        addAutoOrQuery(ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(UserStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case NAME:
                    case EMAIL:
                    case ORGANIZATION:
                    case INTERNAL_STATUS_DATE:
                    case SIZE:
                    case QUOTA:
                    case ACCOUNT_TYPE:
                    case ACCOUNT_AUTHENTICATION_ID:
                    case ACCOUNT_CREATION_DATE:
                    case PROJECTS:
                    case PROJECTS_UID:
                    case PROJECT_NAME:
                    case PROJECTS_ID:
                    case PROJECT_ORGANIZATION:
                    case PROJECT_STATUS:
                    case TOOL_ID:
                    case TOOL_NAME:
                    case TOOL_ALIAS:
                    case CONFIGS:
                    case FILTERS:
                    case FILTERS_ID:
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

    public MongoDBCollection getUserCollection() {
        return userCollection;
    }
}
