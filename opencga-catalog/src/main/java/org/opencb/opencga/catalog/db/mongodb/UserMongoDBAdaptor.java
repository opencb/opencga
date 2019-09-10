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
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.UserConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.api.UserDBAdaptor.QueryParams.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UserMongoDBAdaptor extends MongoDBAdaptor implements UserDBAdaptor {

    private final MongoDBCollection userCollection;
    private UserConverter userConverter;

    public UserMongoDBAdaptor(MongoDBCollection userCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(UserMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.userCollection = userCollection;
        this.userConverter = new UserConverter();
    }

    /*
     * *************************
     * User methods
     * ***************************
     */

    boolean exists(ClientSession clientSession, String userId) throws CatalogDBException {
        return count(clientSession, new Query(QueryParams.ID.key(), userId)).getResult().get(0) > 0;
    }

    @Override
    public WriteResult insert(User user, QueryOptions options) throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody<WriteResult> txnBody = () -> {
            long tmpStartTime = startQuery();

            logger.debug("Starting user insert transaction for user id '{}'", user.getId());

            try {
                insert(clientSession, user);
                return endWrite(tmpStartTime, 1, 1, null, null);
            } catch (CatalogDBException e) {
                logger.error("Could not create user {}: {}", user.getId(), e.getMessage());
                clientSession.abortTransaction();
                return endWrite(tmpStartTime, 1, 0, null,
                        Collections.singletonList(new WriteResult.Fail(user.getId(), e.getMessage())));
            }
        };

        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumInserted() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
        return result;
    }

    private void insert(ClientSession clientSession, User user) throws CatalogDBException {
        checkParameter(user, "user");
        if (exists(clientSession, user.getId())) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        List<Project> projects = user.getProjects();
        user.setProjects(Collections.emptyList());

        user.setLastModified(TimeUtils.getTimeMillis());
        Document userDocument = userConverter.convertToStorageType(user);
        userDocument.append(PRIVATE_ID, user.getId());

        userCollection.insert(clientSession, userDocument, null);

        if (projects != null) {
            for (Project p : projects) {
                dbAdaptorFactory.getCatalogProjectDbAdaptor().insert(clientSession, p, user.getId());
            }
        }
    }

    @Override
    public QueryResult<User> get(String userId, QueryOptions options, String lastModified) throws CatalogDBException {

        checkId(userId);
        Query query = new Query(QueryParams.ID.key(), userId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        if (lastModified != null && !lastModified.isEmpty()) {
            query.append(QueryParams.LAST_MODIFIED.key(), "!=" + lastModified);
        }
        return get(query, options);
    }

    @Override
    public WriteResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), userId);
        query.append(QueryParams.PASSWORD.key(), oldPassword);
        Bson bson = parseQuery(query);

        Bson set = Updates.set("password", newPassword);

        WriteResult result = userCollection.update(bson, set, null);
        if (result.getNumUpdated() == 0) {  //0 query matches.
            throw new CatalogDBException("Bad user or password");
        }
        return result;
    }

    @Override
    public WriteResult updateUserLastModified(String userId) throws CatalogDBException {
        return update(userId, new ObjectMap("lastModified", TimeUtils.getTimeMillis()));
    }

    @Override
    public WriteResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), userId);
        query.append(QueryParams.EMAIL.key(), email);
        Bson bson = parseQuery(query);

        Bson set = Updates.set("password", new Document("password", newCryptPass));

        WriteResult result = userCollection.update(bson, set, null);
        if (result.getNumUpdated() == 0) {  //0 query matches.
            throw new CatalogDBException("Bad user or email");
        }
        return result;
    }

    @Override
    public WriteResult setConfig(String userId, String name, Map<String, Object> config) throws CatalogDBException {
        // Set the config
        Bson bsonQuery = Filters.eq(QueryParams.ID.key(), userId);
        Bson filterDocument = getMongoDBDocument(config, "Config");
        Bson update = Updates.set(QueryParams.CONFIGS.key() + "." + name, filterDocument);

        WriteResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not create " + name + " configuration ");
        }
        return result;
    }

    @Override
    public WriteResult deleteConfig(String userId, String name) throws CatalogDBException {
        // Insert the config
        Bson bsonQuery = Filters.and(
                Filters.eq(QueryParams.ID.key(), userId),
                Filters.exists(QueryParams.CONFIGS.key() + "." + name)
        );
        Bson update = Updates.unset(QueryParams.CONFIGS.key() + "." + name);

        WriteResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not delete " + name + " configuration ");
        }
        return result;
    }

    @Override
    public WriteResult addFilter(String userId, User.Filter filter) throws CatalogDBException {
        // Insert the filter
        Bson bsonQuery = Filters.and(
                Filters.eq(QueryParams.ID.key(), userId),
                Filters.ne(QueryParams.CONFIGS_FILTERS_NAME.key(), filter.getName())
        );
        Bson filterDocument = getMongoDBDocument(filter, "Filter");
        Bson update = Updates.push(QueryParams.CONFIGS_FILTERS.key(), filterDocument);

        WriteResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() != 1) {
            if (result.getNumUpdated() == 0) {
                throw new CatalogDBException("Internal error: The filter could not be stored.");
            } else {
                // This error should NEVER be raised.
                throw new CatalogDBException("User: There was a critical error when storing the filter. Is has been inserted "
                        + result.getNumUpdated() + " times.");
            }
        }
        return result;
    }

    @Override
    public WriteResult updateFilter(String userId, String name, ObjectMap params) throws CatalogDBException {
        if (params.isEmpty()) {
            throw new CatalogDBException("Nothing to be updated. No parameters were passed.");
        }

        final String prefixUpdate = CONFIGS_FILTERS.key() + ".$.";
        Document parameters = new Document();

        if (params.get(FilterParams.DESCRIPTION.key()) != null) {
            parameters.put(prefixUpdate + FilterParams.DESCRIPTION.key(), params.get(FilterParams.DESCRIPTION.key()));
        }

        if (params.get(FilterParams.BIOFORMAT.key()) != null) {
            parameters.put(prefixUpdate + FilterParams.BIOFORMAT.key(), params.get(FilterParams.BIOFORMAT.key()).toString());
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
                .append(ID.key(), userId)
                .append(CONFIGS_FILTERS_NAME.key(), name);
        return userCollection.update(parseQuery(query), new Document("$set", parameters), null);
    }

    @Override
    public WriteResult deleteFilter(String userId, String name) throws CatalogDBException {
        // Delete the filter
        Bson bsonQuery = Filters.and(
                Filters.eq(QueryParams.ID.key(), userId),
                Filters.eq(QueryParams.CONFIGS_FILTERS_NAME.key(), name)
        );
        Bson update = Updates.pull(QueryParams.CONFIGS_FILTERS.key(), new Document(FilterParams.NAME.key(), name));
        WriteResult result = userCollection.update(bsonQuery, update, null);

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Internal error: Filter " + name + " could not be removed");
        }

        return result;
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    QueryResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        logger.debug("User count: {}", bsonDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return userCollection.count(clientSession, bsonDocument);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission) throws CatalogDBException {
        throw new NotImplementedException("Count not implemented for users");
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return userCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<User> get(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query);
        QueryResult<User> userQueryResult = userCollection.find(bson, null, userConverter, options);

        for (User user : userQueryResult.getResult()) {
            if (user.getProjects() != null) {
                List<Project> projects = new ArrayList<>(user.getProjects().size());
                for (Project project : user.getProjects()) {
                    Query query1 = new Query(ProjectDBAdaptor.QueryParams.UID.key(), project.getUid());
                    QueryResult<Project> projectQueryResult = dbAdaptorFactory.getCatalogProjectDbAdaptor().get(query1, options);
                    projects.add(projectQueryResult.first());
                }
                user.setProjects(projects);
            }
        }
        return userQueryResult;
    }

    @Override
    public QueryResult<User> get(Query query, QueryOptions options, String user) throws CatalogDBException {
        throw new NotImplementedException("Get not implemented for user");
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query);
        QueryResult<Document> queryResult = userCollection.find(bson, options);

        for (Document user : queryResult.getResult()) {
            ArrayList<Document> projects = (ArrayList<Document>) user.get("projects");
            if (projects.size() > 0) {
                List<Document> projectsTmp = new ArrayList<>(projects.size());
                for (Document project : projects) {
                    Query query1 = new Query(ProjectDBAdaptor.QueryParams.UID.key(), project.get(ProjectDBAdaptor
                            .QueryParams.UID.key()));
                    QueryResult<Document> queryResult1 = dbAdaptorFactory.getCatalogProjectDbAdaptor().nativeGet(query1, options);
                    projectsTmp.add(queryResult1.first());
                }
                user.remove("projects");
                user.append("projects", projectsTmp);
            }
        }

        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        throw new NotImplementedException("Get not implemented for user");
    }

    @Override
    public WriteResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        Map<String, Object> userParameters = new HashMap<>();

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.EMAIL.key(), QueryParams.ORGANIZATION.key(),
                QueryParams.LAST_MODIFIED.key(), };
        filterStringParams(parameters, userParameters, acceptedParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            userParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            userParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        final String[] acceptedLongParams = {QueryParams.QUOTA.key(), QueryParams.SIZE.key()};
        filterLongParams(parameters, userParameters, acceptedLongParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, userParameters, acceptedMapParams);

        if (!userParameters.isEmpty()) {
            return userCollection.update(parseQuery(query), new Document("$set", userParameters), null);
        }

        return WriteResult.empty();
    }

    @Override
    public WriteResult delete(long id) throws CatalogDBException {
        throw new NotImplementedException("Delete not implemented");
//        Query query = new Query(QueryParams.ID.key(), id);
//        delete(query);
    }

    @Override
    public WriteResult delete(Query query) throws CatalogDBException {
        throw new NotImplementedException("Delete not implemented");
//        QueryResult<DeleteResult> remove = userCollection.remove(parseQuery(query), null);
//
//        if (remove.first().getDeletedCount() == 0) {
//            throw CatalogDBException.deleteError("User");
//        }
    }

    @Override
    public WriteResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Update user by int id. The id should be a string.");
    }

    public WriteResult update(String userId, ObjectMap parameters) throws CatalogDBException {
        checkId(userId);
        Query query = new Query(QueryParams.ID.key(), userId);
        WriteResult update = update(query, parameters, QueryOptions.empty());
        if (update.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not update user " + userId);
        }
        return update;
    }

    WriteResult setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    public WriteResult setStatus(String userId, String status) throws CatalogDBException {
        return update(userId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    @Override
    public WriteResult delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new CatalogDBException("Delete user by int id. The id should be a string.");
    }

    public WriteResult delete(String id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check the user is active or banned
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), User.UserStatus.READY + "," + User.UserStatus.BANNED);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_NAME.key(), User.UserStatus.DELETED);
            QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.STATUS_NAME.key());
            User user = get(query, options).first();
            throw new CatalogDBException("The user {" + id + "} was already " + user.getStatus().getName());
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
        return setStatus(id, User.UserStatus.DELETED);
    }

    /**
     * Checks whether the userId has any active project.
     *
     * @param userId user id.
     * @throws CatalogDBException when the user has active projects. Projects must be deleted first.
     */
    private void checkCanDelete(String userId) throws CatalogDBException {
        checkId(userId);
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId)
                .append(ProjectDBAdaptor.QueryParams.STATUS_NAME.key(), Status.READY);
        Long count = dbAdaptorFactory.getCatalogProjectDbAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The user {" + userId + "} cannot be deleted. The user has " + count + " projects in use.");
        }
    }

    @Override
    public WriteResult delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");

    }

    @Override
    public WriteResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public WriteResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public WriteResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return setStatus(query, Status.READY);
    }

    @Override
    public WriteResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new CatalogDBException("Delete user by int id. The id should be a string.");
    }

    public WriteResult restore(String id, QueryOptions queryOptions) throws CatalogDBException {
        checkId(id);
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The user {" + id + "} is not deleted");
        }

        return setStatus(id, Status.READY);
    }

    /***
     * Removes completely the user from the database.
     * @param id User id to be removed from the database.
     * @return a WriteResult object with the user removed.
     * @throws CatalogDBException when there is any problem during the removal.
     */
    public WriteResult clean(String id) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.ID.key(), id);
        Bson bson = parseQuery(query);

        QueryResult<User> userQueryResult = get(query, new QueryOptions());
        WriteResult result = userCollection.remove(bson, new QueryOptions());
        if (result.getNumDeleted() == 0) {
            throw CatalogDBException.idNotFound("User", query.getString(QueryParams.ID.key()));
        } else {
            return result;
        }
    }

    @Override
    public DBIterator<User> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = userCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, userConverter);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = userCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
    }

    @Override
    public DBIterator<User> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(userCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(userCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(userCollection, bsonQuery, fields, "name", options);
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
        try (DBIterator<User> catalogDBIterator = iterator(query, options)) {
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
                    case ID:
                        addAutoOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(User.UserStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case NAME:
                    case EMAIL:
                    case PASSWORD:
                    case ORGANIZATION:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case LAST_MODIFIED:
                    case SIZE:
                    case QUOTA:
                    case PROJECTS:
                    case PROJECTS_UID:
                    case PROJECT_NAME:
                    case PROJECTS_ID:
                    case PROJECT_ORGANIZATION:
                    case PROJECT_STATUS:
                    case PROJECT_LAST_MODIFIED:
                    case TOOL_ID:
                    case TOOL_NAME:
                    case TOOL_ALIAS:
                    case CONFIGS:
                    case CONFIGS_FILTERS:
                    case CONFIGS_FILTERS_NAME:
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
