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
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
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
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
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

import javax.annotation.Nullable;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.UserDBAdaptor.QueryParams.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserMongoDBAdaptor extends CatalogMongoDBAdaptor implements UserDBAdaptor {

    private final MongoDBCollection userCollection;
    private final MongoDBCollection deletedUserCollection;
    private UserConverter userConverter;

    // --- Password constants ---
    public static final String HASH = "hash";
    public static final String SALT = "salt";

    public static final String PRIVATE_PASSWORD = "_password";

    public static final String CURRENT = "current";
    private static final String PRIVATE_PASSWORD_CURRENT = "_password." + CURRENT;
    private static final String PRIVATE_PASSWORD_CURRENT_HASH = PRIVATE_PASSWORD_CURRENT + "." + HASH;
    private static final String PRIVATE_PASSWORD_CURRENT_SALT = PRIVATE_PASSWORD_CURRENT + "." + SALT;

    public static final String ARCHIVE = "archive";
    public static final String PRIVATE_PASSWORD_ARCHIVE = "_password." + ARCHIVE;
    private static final String PRIVATE_PASSWORD_ARCHIVE_HASH = PRIVATE_PASSWORD_ARCHIVE + "." + HASH;
    private static final String PRIVATE_PASSWORD_ARCHIVE_SALT = PRIVATE_PASSWORD_ARCHIVE + "." + SALT;
    // --------------------------

    public UserMongoDBAdaptor(MongoDBCollection userCollection, MongoDBCollection deletedUserCollection, Configuration configuration,
                              OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
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
    public OpenCGAResult insert(User user, String password, QueryOptions options) throws CatalogException {
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

        Document privatePassword = new Document();
        if (StringUtils.isNotEmpty(password)) {
            String salt = PasswordUtils.getStrongRandomSalt();
            String hash = encryptPassword(password, salt);
            Document passwordDoc = new Document()
                    .append(HASH, hash)
                    .append(SALT, salt);
            privatePassword.put(CURRENT, passwordDoc);
            privatePassword.put(ARCHIVE, Collections.singletonList(passwordDoc));
        }
        userDocument.put(PRIVATE_PASSWORD, privatePassword);

        userCollection.insert(clientSession, userDocument, null);
    }

    @Override
    public OpenCGAResult<User> get(String userId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.ID.key(), userId);
        return get(query, options);
    }

    @Override
    public OpenCGAResult<User> changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        return setPassword(userId, oldPassword, newPassword, 0);
    }

    @Override
    public void authenticate(String userId, String password) throws CatalogDBException, CatalogAuthenticationException {
        Bson query = Filters.and(
                Filters.eq(QueryParams.ID.key(), userId),
                // TODO: Deprecated. Remove Filters.or using the deprecated account authentication id
                Filters.or(
                        Filters.eq(DEPRECATED_ACCOUNT_AUTHENTICATION_ID.key(), AuthenticationOrigin.AuthenticationType.OPENCGA),
                        Filters.eq(INTERNAL_ACCOUNT_AUTHENTICATION_ID.key(), AuthenticationOrigin.AuthenticationType.OPENCGA)
                )
        );
        Bson projection = Projections.include(PRIVATE_PASSWORD);
        DataResult<Document> dataResult = userCollection.find(query, projection, QueryOptions.empty());
        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("User " + userId + " not found");
        }
        Document userDocument = dataResult.first();
        Object rootPasswordObject = userDocument.get(PRIVATE_PASSWORD);
        Document rootPasswordDoc;
        // TODO: Remove this block of code in the future when all users have been migrated
        if (rootPasswordObject instanceof String) {
            if (ParamConstants.OPENCGA_USER_ID.equals(userId)) {
                logger.warn("User {} is using the deprecated password format. Please, migrate your code as soon as possible.", userId);
                if (!encryptPassword(password, "").equals(rootPasswordObject)) {
                    throw CatalogAuthenticationException.incorrectUserOrPassword(AuthenticationOrigin.AuthenticationType.OPENCGA.name());
                }
                return;
            } else {
                throw new CatalogDBException("User '" + userId + "' is using the deprecated password format. Please, ask your"
                        + " administrator to run the pending migrations to fix this issue.");
            }
        } else {
            rootPasswordDoc = (Document) rootPasswordObject;
        }
        // TODO: End of block of code to remove (and replace using commented code below)
//        Document rootPasswordDoc = userDocument.get(PRIVATE_PASSWORD, Document.class);
        if (rootPasswordDoc == null) {
            throw new CatalogDBException("Critical error. User '" + userId + "' does not have any password set. Please, contact"
                    + " with the developers.");
        }
        Document passwordDoc = rootPasswordDoc.get(CURRENT, Document.class);
        if (passwordDoc == null) {
            throw new CatalogDBException("Critical error. User '" + userId + "' does not have any password set. Please, contact"
                    + " with the developers.");
        }

        String salt = passwordDoc.getString(SALT);
        String hash = encryptPassword(password, salt);
        if (!hash.equals(passwordDoc.getString(HASH))) {
            throw CatalogAuthenticationException.incorrectUserOrPassword(AuthenticationOrigin.AuthenticationType.OPENCGA.name());
        }
    }

    @Override
    public OpenCGAResult<User> resetPassword(String userId, String email, String newPassword) throws CatalogException {
        return setPassword(userId, null, newPassword, 1); // Expire password in 1 day so the user needs to change it manually
    }

    public OpenCGAResult<User> setPassword(String userId, @Nullable String oldPassword, String newPassword, int expirationDays)
            throws CatalogException {
        String prefixErrorMsg = "Could not update the password. ";
        return runTransaction(clientSession -> {
            // 1. Obtain archived passwords
            Bson query = Filters.eq(QueryParams.ID.key(), userId);
            Bson projection = Projections.include(PRIVATE_PASSWORD);
            DataResult<Document> userQueryResult = userCollection.find(clientSession, query, projection, QueryOptions.empty());
            if (userQueryResult.getNumResults() == 0) {
                throw new CatalogDBException(prefixErrorMsg + "User " + userId + " not found.");
            }
            Document userDoc = userQueryResult.first();
            Document passwordDoc = userDoc.get(PRIVATE_PASSWORD, Document.class);

            // 1.1. Check oldPassword
            if (StringUtils.isNotEmpty(oldPassword)) {
                Document currentPasswordDoc = passwordDoc.get(CURRENT, Document.class);
                String currentSalt = currentPasswordDoc.getString(SALT);
                String currentHash = encryptPassword(oldPassword, currentSalt);
                if (!currentHash.equals(currentPasswordDoc.getString(HASH))) {
                    throw new CatalogAuthenticationException(prefixErrorMsg + "Please, verify that the current password is correct.");
                }
            }

            // 2. Check new password has not been used before
            for (Document document : passwordDoc.getList(ARCHIVE, Document.class)) {
                String hashValue = document.getString(HASH);
                String saltValue = document.getString(SALT);
                String encryptedPassword = encryptPassword(newPassword, saltValue);
                if (encryptedPassword.equals(hashValue)) {
                    throw new CatalogAuthenticationException(prefixErrorMsg + "The new password has already been used."
                            + " Please, use a different one.");
                }
            }

            // 3. Generate new salt for current password
            String newSalt = PasswordUtils.getStrongRandomSalt();
            String newHash = encryptPassword(newPassword, newSalt);

            // 4. Generate update document
            UpdateDocument updateDocument = new UpdateDocument();
            // add to current
            updateDocument.getSet().put(PRIVATE_PASSWORD_CURRENT_HASH, newHash);
            updateDocument.getSet().put(PRIVATE_PASSWORD_CURRENT_SALT, newSalt);

            // add to archive
            Document document = new Document()
                    .append(HASH, newHash)
                    .append(SALT, newSalt);
            updateDocument.getPush().put(PRIVATE_PASSWORD_ARCHIVE, document);

            updateDocument.getSet().put(INTERNAL_ACCOUNT_PASSWORD_LAST_MODIFIED.key(), TimeUtils.getTime());
            if (expirationDays > 0) {
                Date date = TimeUtils.addDaysToCurrentDate(expirationDays);
                String stringDate = TimeUtils.getTime(date);
                updateDocument.getSet().put(INTERNAL_ACCOUNT_PASSWORD_EXPIRATION_DATE.key(), stringDate);
            } else if (configuration.getAccount().getPasswordExpirationDays() > 0) {
                Date date = TimeUtils.addDaysToCurrentDate(configuration.getAccount().getPasswordExpirationDays());
                String stringDate = TimeUtils.getTime(date);
                updateDocument.getSet().put(INTERNAL_ACCOUNT_PASSWORD_EXPIRATION_DATE.key(), stringDate);
            }
            Document update = updateDocument.toFinalUpdateDocument();

            logger.debug("Change password: query '{}'; update: '{}'", query.toBsonDocument(), update);
            DataResult<?> result = userCollection.update(clientSession, query, update, null);
            if (result.getNumUpdated() == 0) {
                throw new CatalogAuthenticationException("Could not update the password. Please, verify that the current password is"
                        + " correct.");
            }
            return new OpenCGAResult(result);
        }, e -> logger.error("User {}: {}", userId, e.getMessage()));
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
        QueryOptions userOptions = filterQueryOptionsToIncludeKeys(options, Collections.singletonList(ID));
        DataResult<User> userDataResult = userCollection.find(bson, null, userConverter, userOptions);

        if (includeProjects(options)) {
            QueryOptions sharedProjectOptions = extractNestedOptions(options, PROJECTS.key());
            sharedProjectOptions = filterQueryOptionsToIncludeKeys(sharedProjectOptions,
                    Arrays.asList(ProjectDBAdaptor.QueryParams.FQN.key(), "studies." + StudyDBAdaptor.QueryParams.FQN.key(),
                            "studies." + StudyDBAdaptor.QueryParams.GROUPS.key()));
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

    private void extractSharedProjects(DataResult<User> userDataResult, QueryOptions options) throws CatalogDBException {
        Set<String> users = userDataResult.getResults().stream().map(User::getId).collect(Collectors.toSet());

        Map<String, Project> projectMap = new HashMap<>();
        Map<String, Study> studyMap = new HashMap<>();
        Map<String, String> studyProjectMap = new HashMap<>();
        Map<String, List<String>> userStudyMap = new HashMap<>();
        OpenCGAResult<Project> result = dbAdaptorFactory.getCatalogProjectDBAdaptor().get(new Query(), options);
        for (Project project : result.getResults()) {
            projectMap.put(project.getFqn(), project);
            if (project.getStudies() != null) {
                for (Study study : project.getStudies()) {
                    studyMap.put(study.getFqn(), study);
                    studyProjectMap.put(study.getFqn(), project.getFqn());

                    if (study.getGroups() != null) {
                        for (Group group : study.getGroups()) {
                            if (StudyManager.MEMBERS.equals(group.getId())) {
                                // Add all the users that should be able to see the study to the map
                                for (String userId : group.getUserIds()) {
                                    if (users.contains(userId)) {
                                        if (!userStudyMap.containsKey(userId)) {
                                            userStudyMap.put(userId, new ArrayList<>());
                                        }
                                        userStudyMap.get(userId).add(study.getFqn());
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Add project information
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

                user.setProjects(projectList);
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
            if (CollectionUtils.isNotEmpty(projects)) {
                List<Document> projectsTmp = new ArrayList<>(projects.size());
                for (Document project : projects) {
                    Query query1 = new Query(ProjectDBAdaptor.QueryParams.UID.key(), project.get(ProjectDBAdaptor
                            .QueryParams.UID.key()));
                    OpenCGAResult<Document> queryResult1 = dbAdaptorFactory.getCatalogProjectDBAdaptor().nativeGet(query1, options);
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
        UpdateDocument document = new UpdateDocument();

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.EMAIL.key(), INTERNAL_ACCOUNT_EXPIRATION_DATE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_ID.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_ID.key(), parameters.get(QueryParams.INTERNAL_STATUS_ID.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        final String[] acceptedIntParams = {INTERNAL_ACCOUNT_FAILED_ATTEMPTS.key()};
        filterIntParams(parameters, document.getSet(), acceptedIntParams);

        final String[] acceptedObjectParams = {QueryParams.QUOTA.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            document.getSet().put(INTERNAL_LAST_MODIFIED, TimeUtils.getTime());
        }

        Document userUpdate = document.toFinalUpdateDocument();
        if (userUpdate.isEmpty()) {
            throw new CatalogDBException("Nothing to be updated.");
        }

        return new OpenCGAResult(userCollection.update(parseQuery(query), userUpdate, null));
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
        return update(query, parameters, QueryOptions.empty());
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

        // Change the status of the user to deleted
        return setStatus(id, UserStatus.DELETED);
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

    private static String encryptPassword(String password, String salt) throws CatalogDBException {
        if (StringUtils.isNotEmpty(password)) {
            try {
                return CryptoUtils.sha1(password + salt);
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
                    case INTERNAL_ACCOUNT_AUTHENTICATION_ID:
                    case INTERNAL_ACCOUNT_CREATION_DATE:
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
