/*
 * Copyright 2015 OpenCB
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

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.UserConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoUserDBAdaptor extends CatalogMongoDBAdaptor implements CatalogUserDBAdaptor {

    private final MongoDBCollection userCollection;
    private UserConverter userConverter;

    public CatalogMongoUserDBAdaptor(MongoDBCollection userCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoUserDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.userCollection = userCollection;
        this.userConverter = new UserConverter();
    }

    /*
     * *************************
     * User methods
     * ***************************
     */

    @Override
    public boolean checkUserCredentials(String userId, String sessionId) {
        return false;
    }

    @Override
    public boolean userExists(String userId) {
        QueryResult<Long> count = userCollection.count(new BasicDBObject(PRIVATE_ID, userId));
        long l = count.getResult().get(0);
        return l != 0;
    }

    @Override
    public void checkUserExists(String userId) throws CatalogDBException {
        checkUserExist(userId, userCollection);
    }

    @Override
    public QueryResult<User> createUser(String userId, String userName, String email, String password,
                                        String organization, QueryOptions options) throws CatalogDBException {
        checkParameter(userId, "userId");
        long startTime = startQuery();

        if (userExists(userId)) {
            throw new CatalogDBException("User {id:\"" + userId + "\"} already exists");
        }
        return null;

    }

    @Override
    public QueryResult<User> insertUser(User user, QueryOptions options) throws CatalogDBException {
        checkParameter(user, "user");
        long startTime = startQuery();

        if (userExists(user.getId())) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        List<Project> projects = user.getProjects();
        user.setProjects(Collections.<Project>emptyList());
        user.setLastActivity(TimeUtils.getTimeMillis());
//        DBObject userDBObject = getDbObject(user, "User " + user.getId());
        Document userDBObject = getMongoDBDocument(user, "User " + user.getId());
        userDBObject.append(PRIVATE_ID, user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDBObject, null);
        } catch (DuplicateKeyException e) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        // TODO review this code. A project 'converter' could create Document objects easily
        String errorMsg = insert.getErrorMsg() != null ? insert.getErrorMsg() : "";
        for (Project p : projects) {
            String projectErrorMsg = dbAdaptorFactory.getCatalogProjectDbAdaptor().createProject(user.getId(), p, options).getErrorMsg();
            if (projectErrorMsg != null && !projectErrorMsg.isEmpty()) {
                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
            }
        }

        //Get the inserted user.
        user.setProjects(projects);
        List<User> result = getUser(user.getId(), options, "").getResult();

        return endQuery("insertUser", startTime, result, errorMsg, null);
    }

    /*
     * TODO: delete user from:
     * project acl and owner
     * study acl and owner
     * file acl and creator
     * job userid
     * also, delete his:
     * projects
     * studies
     * analysesS
     * jobs
     * files
     */
//    @Override
//    public QueryResult<Long> deleteUser(String userId) throws CatalogDBException {
//        checkParameter(userId, "userId");
//        long startTime = startQuery();
//
////        WriteResult id = nativeUserCollection.remove(new BasicDBObject("id", userId));
//        WriteResult wr = userCollection.remove(new BasicDBObject(PRIVATE_ID, userId), null).getResult().get(0);
//        if (wr.getN() == 0) {
//            throw CatalogDBException.idNotFound("User", userId);
//        } else {
//            return endQuery("Delete user", startTime, Arrays.asList(wr.getN()));
//        }
//    }
    @Override
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException {
        checkParameter(userId, "userId");
        checkParameter(password, "password");
        long startTime = startQuery();
        System.out.println(userId + "::" + password);
//        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder.start("id", userId).append("password", password).get());
//        Bson and = Filters.and(Filters.eq("id", userId), Filters.eq("password", password));
        Query query = new Query(QueryParams.ID.key(), userId).append(QueryParams.PASSWORD.key(), password);
        QueryResult queryResult = nativeGet(query, new QueryOptions());

        QueryResult<Long> count = count(query);
        if (count.getResult().get(0) == 0) {
            throw new CatalogDBException("Bad user or password");
        } else {
            query = new Query(QueryParams.SESSION_ID.key(), session.getId());
//            QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
            QueryResult<Long> countSessions = count(query);
            if (countSessions.getResult().get(0) != 0) {
                throw new CatalogDBException("Already logged");
            } else {
                addSession(userId, session);
                ObjectMap resultObjectMap = new ObjectMap();
                resultObjectMap.put("sessionId", session.getId());
                resultObjectMap.put("userId", userId);
                return endQuery("Login", startTime, Collections.singletonList(resultObjectMap));
            }
        }
    }

    @Override
    public QueryResult<Session> addSession(String userId, Session session) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
        if (countSessions.getResult().get(0) != 0) {
            throw new CatalogDBException("Already logged with this sessionId");
        } else {
            BasicDBObject id = new BasicDBObject("id", userId);
            BasicDBObject updates = new BasicDBObject(
                    "$push", new BasicDBObject(
                    "sessions", getDbObject(session, "Session")
            )
            );
            userCollection.update(id, updates, null);

            return endQuery("Login", startTime, Collections.singletonList(session));
        }
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogDBException {
        long startTime = startQuery();

        String userIdBySessionId = getUserIdBySessionId(sessionId);
        if (userIdBySessionId.isEmpty()) {
            return endQuery("logout", startTime, null, "", "Session not found");
        }
        if (userIdBySessionId.equals(userId)) {
            userCollection.update(
                    new BasicDBObject("sessions.id", sessionId),
                    new BasicDBObject("$set", new BasicDBObject("sessions.$.logout", TimeUtils.getTime())),
                    null);

        } else {
            throw new CatalogDBException("UserId mismatches with the sessionId");
        }

        return endQuery("Logout", startTime);
    }

    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
        if (countSessions.getResult().get(0) != 0) {
            throw new CatalogDBException("Error, sessionID already exists");
        }
        String userId = "anonymous_" + session.getId();
        User user = new User(userId, "Anonymous", "", "", "", User.Role.ANONYMOUS, "");
        user.getSessions().add(session);
//        DBObject anonymous = getDbObject(user, "User");
        Document anonymous = getMongoDBDocument(user, "User");
        anonymous.put(PRIVATE_ID, user.getId());

        userCollection.insert(anonymous, null);
//        try {
//        } catch (MongoException.DuplicateKey e) {
//            throw new CatalogDBException("Anonymous user {id:\"" + user.getId() + "\"} already exists");
//        }

        ObjectMap resultObjectMap = new ObjectMap();
        resultObjectMap.put("sessionId", session.getId());
        resultObjectMap.put("userId", userId);
        return endQuery("Login as anonymous", startTime, Collections.singletonList(resultObjectMap));
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) throws CatalogDBException {
        long startTime = startQuery();
        String userId = "anonymous_" + sessionId;
        logout(userId, sessionId);
        delete(userId);
        return endQuery("Logout anonymous", startTime);
    }

    @Override
    public QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogDBException {
//        long startTime = startQuery();
//        if (!userExists(userId)) {
//            throw CatalogDBException.idNotFound("User", userId);
//        }
//        DBObject query = new BasicDBObject(PRIVATE_ID, userId);
//        query.put("lastActivity", new BasicDBObject("$ne", lastActivity));
//        QueryResult<DBObject> result = userCollection.find(query, options);
//        User user = parseUser(result);
//        if (user == null) {
//            return endQuery("Get user", startTime); // user exists but no different lastActivity was found: return empty result
//        } else {
//            joinFields(user, options);
//            return endQuery("Get user", startTime, Collections.singletonList(user));
//        }
        checkUserExists(userId);
        Query query = new Query(QueryParams.ID.key(), userId);
        query.append(QueryParams.LAST_ACTIVITY.key(), lastActivity);
        return get(query, options);
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject("id", userId);
//        query.put("password", oldPassword);
        Query query = new Query(QueryParams.ID.key(), userId);
        query.append(QueryParams.PASSWORD.key(), oldPassword);
        Bson bson = parseQuery(query);

//        BasicDBObject fields = new BasicDBObject("password", newPassword);
//        BasicDBObject action = new BasicDBObject("$set", fields);
//        Bson set = Updates.set("password", new Document("password", newPassword));
        Bson set = Updates.set("password", newPassword);

//        QueryResult<WriteResult> update = userCollection.update(bson, set, null);
        QueryResult<UpdateResult> update = userCollection.update(bson, set, null);
        if (update.getResult().get(0).getModifiedCount() == 0) {  //0 query matches.
            throw new CatalogDBException("Bad user or password");
        }
        return endQuery("Change Password", startTime, update);
    }

    @Override
    public void updateUserLastActivity(String userId) throws CatalogDBException {
        update(userId, new ObjectMap("lastActivity", TimeUtils.getTimeMillis()));
    }

    @Override
    public QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject("id", userId);
//        query.put("email", email);
        Query query = new Query(QueryParams.ID.key(), userId);
        query.append(QueryParams.EMAIL.key(), email);
        Bson bson = parseQuery(query);

//        BasicDBObject fields = new BasicDBObject("password", newCryptPass);
//        BasicDBObject action = new BasicDBObject("$set", fields);
        Bson set = Updates.set("password", new Document("password", newCryptPass));

//        QueryResult<WriteResult> update = userCollection.update(query, action, null);
        QueryResult<UpdateResult> update = userCollection.update(bson, set, null);
        if (update.getResult().get(0).getModifiedCount() == 0) {  //0 query matches.
            throw new CatalogDBException("Bad user or email");
        }
        return endQuery("Reset Password", startTime, update);
    }

    @Override
    public QueryResult<Session> getSession(String userId, String sessionId) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject("id", userId);
//        query.put("sessions.id", sessionId);
        Query query1 = new Query(QueryParams.ID.key(), userId)
                .append(QueryParams.SESSION_ID.key(), sessionId);
        Bson bson = parseQuery(query1);


//        BasicDBObject projection = new BasicDBObject("sessions",
//                new BasicDBObject("$elemMatch",
//                        new BasicDBObject("id", sessionId)));
        Bson projection = Projections.elemMatch("sessions", Filters.eq("id", sessionId));

//        QueryResult<DBObject> result = userCollection.find(query, projection, null);
        QueryResult<Document> documentQueryResult = userCollection.find(bson, projection, null);
        User user = parseUser(documentQueryResult);

        return endQuery("getSession", startTime, user.getSessions());
    }

    @Override
    public String getUserIdBySessionId(String sessionId) {
        QueryResult id = userCollection.find(
                new BasicDBObject("sessions", new BasicDBObject("$elemMatch", BasicDBObjectBuilder
                        .start("id", sessionId)
                        .append("logout", "").get())),
                new BasicDBObject("id", true),
                null);

        if (id.getNumResults() != 0) {
            return (String) ((Document) id.getResult().get(0)).get("id");
        } else {
            return "";
        }
    }

    @Override
    public QueryResult<Long> count(Query query) {
        Bson bsonDocument = parseQuery(query);
        return userCollection.count(bsonDocument);
    }


    @Override
    public QueryResult distinct(Query query, String field) {
        Bson bsonDocument = parseQuery(query);
        return userCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<User> get(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryResult<User> userQueryResult = userCollection.find(bson, null, userConverter, options);

        for (User user : userQueryResult.getResult()) {
            if (user.getProjects() != null) {
                List<Project> projects = new ArrayList<>(user.getProjects().size());
                for (Project project : user.getProjects()) {
                    Query query1 = new Query(CatalogProjectDBAdaptor.QueryParams.ID.key(), project.getId());
                    QueryResult<Project> projectQueryResult = dbAdaptorFactory.getCatalogProjectDbAdaptor().get(query1, options);
                    projects.add(projectQueryResult.first());
                }
                user.setProjects(projects);
            }
        }
        return userQueryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        QueryResult<Document> queryResult = userCollection.find(bson, options);

        for (Document user : queryResult.getResult()) {
            ArrayList<Document> projects = (ArrayList<Document>) user.get("projects");
            if (projects.size() > 0) {
                List<Document> projectsTmp = new ArrayList<>(projects.size());
                for (Document project : projects) {
                    Query query1 = new Query(CatalogProjectDBAdaptor.QueryParams.ID.key(), project.get(CatalogProjectDBAdaptor
                            .QueryParams.ID.key()));
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
    public QueryResult<Long> update(Query query, ObjectMap parameters) {
        return null;
    }

    @Override
    public QueryResult<User> update(int id, ObjectMap parameters) throws CatalogDBException {
        throw new NotImplementedException("Update user by int id. The id should be a string.");
    }

    public QueryResult<User> update(String userId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> userParameters = new HashMap<>();

        final String[] acceptedParams = {"name", "email", "organization", "lastActivity", "status"};
        filterStringParams(parameters, userParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap("role", User.Role.class);
        filterEnumParams(parameters, userParameters, acceptedEnums);

        final String[] acceptedIntParams = {"diskQuota", "diskUsage"};
        filterIntParams(parameters, userParameters, acceptedIntParams);

        final String[] acceptedMapParams = {"attributes", "configs"};
        filterMapParams(parameters, userParameters, acceptedMapParams);

        if (!userParameters.isEmpty()) {
// QueryResult<WriteResult> update = userCollection.update(new BasicDBObject(PRIVATE_ID, userId),
//                   new BasicDBObject("$set", userParameters), null);
            QueryResult<UpdateResult> update = userCollection.update(Filters.eq(PRIVATE_ID, userId),
                    new Document("$set", userParameters), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("User", userId);
            }
        }

        return endQuery("Modify user", startTime);
    }

    @Override
    public QueryResult<User> delete(int id) throws CatalogDBException {
        throw new NotImplementedException("Delete user by int id. The id should be a string.");
    }

    public QueryResult<User> delete(String id) throws CatalogDBException {
        Query query = new Query(CatalogJobDBAdaptor.QueryParams.ID.key(), id);
        QueryResult<User> userQueryResult = get(query, null);
        if (userQueryResult.getResult().size() == 1) {
            QueryResult<Long> delete = delete(query);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("User id '{}' has not been deleted", id);
            }
        } else {
            throw CatalogDBException.idNotFound("User id '{}' does not exist (or there are too many)", id);
        }
        return userQueryResult;
    }

    /*
     * TODO: delete user from:
     * project acl and owner
     * study acl and owner
     * file acl and creator
     * job userid
     * also, delete his:
     * projects
     * studies
     * analysesS
     * jobs
     * files
     */
    @Override
    public QueryResult<Long> delete(Query query) throws CatalogDBException {
        checkParameter(QueryParams.ID.key(), query.getString(QueryParams.ID.key()));
        long startTime = startQuery();
        Bson bson = parseQuery(query);
        QueryResult<DeleteResult> remove = userCollection.remove(bson, new QueryOptions());
        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("User", query.getString(QueryParams.ID.key()));
        } else {
            return endQuery("Delete user", startTime, Collections.singletonList(remove.first().getDeletedCount()));
        }
    }

    @Override
    public CatalogDBIterator<User> iterator(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = userCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, userConverter);
    }

    @Override
    public Iterator nativeIterator(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        return userCollection.nativeQuery().find(bson, options).iterator();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(userCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(userCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) {
        Objects.requireNonNull(action);
        CatalogDBIterator<User> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) {
        List<Bson> andBsonList = new ArrayList<>();

        // FIXME: Pedro. Check the mongodb names as well as integer createQueries

        addStringOrQuery(PRIVATE_ID, QueryParams.ID.key(), query, andBsonList);
        addStringOrQuery("name", QueryParams.NAME.key(), query, andBsonList);
        addStringOrQuery("email", QueryParams.EMAIL.key(), query, andBsonList);
        addStringOrQuery("password", QueryParams.PASSWORD.key(), query, andBsonList);
        addStringOrQuery("organization", QueryParams.ORGANIZATION.key(), query, andBsonList);
        addStringOrQuery("status", QueryParams.STATUS.key(), query, andBsonList);
        addStringOrQuery("lastActivity", QueryParams.LAST_ACTIVITY.key(), query,
                MongoDBQueryUtils.ComparisonOperator.NOT_EQUAL, andBsonList);

        addIntegerOrQuery("projects.id", QueryParams.PROJECT_ID.key(), query, andBsonList);
        addStringOrQuery("projects.name", QueryParams.PROJECT_NAME.key(), query, andBsonList);
        addStringOrQuery("projects.alias", QueryParams.PROJECT_ALIAS.key(), query, andBsonList);
        addStringOrQuery("projects.organization", QueryParams.PROJECT_ORGANIZATION.key(), query, andBsonList);
        addStringOrQuery("projects.status", QueryParams.PROJECT_STATUS.key(), query, andBsonList);
        addStringOrQuery("projects.lastActivity", QueryParams.PROJECT_LAST_ACTIVITY.key(), query, andBsonList);

        addIntegerOrQuery("tools.id", QueryParams.TOOL_ID.key(), query, andBsonList);
        addStringOrQuery("tools.name", QueryParams.TOOL_NAME.key(), query, andBsonList);
        addStringOrQuery("tools.alias", QueryParams.TOOL_ALIAS.key(), query, andBsonList);

        addStringOrQuery("sessions.id", QueryParams.SESSION_ID.key(), query, andBsonList);
        addStringOrQuery("sessions.ip", QueryParams.SESSION_IP.key(), query, andBsonList);
        addStringOrQuery("sessions.login", QueryParams.SESSION_LOGIN.key(), query, andBsonList);
        addStringOrQuery("sessions.logout", QueryParams.SESSION_LOGOUT.key(), query, andBsonList);

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
