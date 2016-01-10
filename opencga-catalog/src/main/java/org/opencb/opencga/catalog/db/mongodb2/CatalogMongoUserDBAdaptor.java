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

package org.opencb.opencga.catalog.db.mongodb2;

import com.mongodb.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api2.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api2.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb2.CatalogMongoDBAdaptor._ID;
import static org.opencb.opencga.catalog.db.mongodb2.CatalogMongoDBUtils.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoUserDBAdaptor extends AbstractCatalogMongoDBAdaptor implements CatalogUserDBAdaptor {

    private final MongoDBCollection userCollection;
    private final MongoDBCollection metaCollection;
    private final CatalogDBAdaptorFactory dbAdaptorFactory;

    public CatalogMongoUserDBAdaptor(CatalogDBAdaptorFactory dbAdaptorFactory, MongoDBCollection metaCollection, MongoDBCollection
            userCollection) {
        super(LoggerFactory.getLogger(CatalogMongoUserDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaCollection;
        this.userCollection = userCollection;
    }

    /**
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
        QueryResult<Long> count = userCollection.count(new BasicDBObject(_ID, userId));
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
        Document userDBObject = getDbDocument(user, "User " + user.getId());
        userDBObject.put(_ID, user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDBObject, null);
        } catch (DuplicateKeyException e) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        // TODO review this code. A project 'converter' could create Document objects easily
        String errorMsg = insert.getErrorMsg() != null ? insert.getErrorMsg() : "";
//        for (Project p : projects) {
//            String projectErrorMsg = createProject(user.getId(), p, options).getErrorMsg();
//            if (projectErrorMsg != null && !projectErrorMsg.isEmpty()) {
//                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
//            }
//        }

        //Get the inserted user.
        user.setProjects(projects);
        List<User> result = getUser(user.getId(), options, "").getResult();

        return endQuery("insertUser", startTime, result, errorMsg, null);
    }

    /**
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
//        WriteResult wr = userCollection.remove(new BasicDBObject(_ID, userId), null).getResult().get(0);
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

//        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder.start("id", userId).append("password", password).get());
        Bson and = Filters.and(Filters.eq("id", userId), Filters.eq("password", password));
        QueryResult<Long> count = userCollection.count(and);
        if (count.getResult().get(0) == 0) {
            throw new CatalogDBException("Bad user or password");
        } else {
            QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
            if (countSessions.getResult().get(0) != 0) {
                throw new CatalogDBException("Already logged");
            } else {
                addSession(userId, session);
                ObjectMap resultObjectMap = new ObjectMap();
                resultObjectMap.put("sessionId", session.getId());
                resultObjectMap.put("userId", userId);
                return endQuery("Login", startTime, Arrays.asList(resultObjectMap));
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
        Document anonymous = getDbDocument(user, "User");
        anonymous.put(_ID, user.getId());

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
        deleteUser(userId);
        return endQuery("Logout anonymous", startTime);
    }

    @Override
    public QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogDBException {
//        long startTime = startQuery();
//        if (!userExists(userId)) {
//            throw CatalogDBException.idNotFound("User", userId);
//        }
//        DBObject query = new BasicDBObject(_ID, userId);
//        query.put("lastActivity", new BasicDBObject("$ne", lastActivity));
//        QueryResult<DBObject> result = userCollection.find(query, options);
//        User user = parseUser(result);
//        if (user == null) {
//            return endQuery("Get user", startTime); // user exists but no different lastActivity was found: return empty result
//        } else {
//            joinFields(user, options);
//            return endQuery("Get user", startTime, Collections.singletonList(user));
//        }
        Query query = new Query(QueryParams.ID.key(), userId);
        query.append(QueryParams.LAST_ACTIVITY.key(), lastActivity);
        return get(query, options);
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("password", oldPassword);
        BasicDBObject fields = new BasicDBObject("password", newPassword);
        BasicDBObject action = new BasicDBObject("$set", fields);
        QueryResult<WriteResult> update = userCollection.update(query, action, null);
        if (update.getResult().get(0).getN() == 0) {  //0 query matches.
            throw new CatalogDBException("Bad user or password");
        }
        return endQuery("Change Password", startTime, update);
    }

    @Override
    public void updateUserLastActivity(String userId) throws CatalogDBException {
        modifyUser(userId, new ObjectMap("lastActivity", TimeUtils.getTimeMillis()));
    }

    @Override
    public QueryResult<User> modifyUser(String userId, ObjectMap parameters) throws CatalogDBException {
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
            QueryResult<WriteResult> update = userCollection.update(
                    new BasicDBObject(_ID, userId),
                    new BasicDBObject("$set", userParameters), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getN() == 0) {
                throw CatalogDBException.idNotFound("User", userId);
            }
        }

        return endQuery("Modify user", startTime);
    }

    @Override
    public QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("email", email);
        BasicDBObject fields = new BasicDBObject("password", newCryptPass);
        BasicDBObject action = new BasicDBObject("$set", fields);
        QueryResult<WriteResult> update = userCollection.update(query, action, null);
        if (update.getResult().get(0).getN() == 0) {  //0 query matches.
            throw new CatalogDBException("Bad user or email");
        }
        return endQuery("Reset Password", startTime, update);
    }

    @Override
    public QueryResult<Session> getSession(String userId, String sessionId) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("sessions.id", sessionId);
        BasicDBObject projection = new BasicDBObject("sessions",
                new BasicDBObject("$elemMatch",
                        new BasicDBObject("id", sessionId)));
        QueryResult<DBObject> result = userCollection.find(query, projection, null);
        User user = parseUser(result);

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
            return (String) ((DBObject) id.getResult().get(0)).get("id");
        } else {
            return "";
        }
    }


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
            project.setStudies(dbAdaptorFactory.getCatalogStudyDBAdaptor().getAllStudiesInProject(project.getId(), options).getResult());
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
    public QueryResult<User> get(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        return userCollection.find(bson, options);
    }

    @Override
    public QueryResult<User> update(Query query, ObjectMap parameters) {
        return null;
    }

    /**
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
    public Iterator<User> iterator(Query query, QueryOptions options) {
        return null;
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

    }

    private Bson parseQuery(Query query) {
        List<Bson> andBsonList = new ArrayList<>();

        // FIXME: Pedro. Check the mongodb names as well as integer createQueries

        createOrQuery(query, QueryParams.ID.key(), "id", andBsonList);
        createOrQuery(query, QueryParams.NAME.key(), "name", andBsonList);
        createOrQuery(query, QueryParams.EMAIL.key(), "email", andBsonList);
        createOrQuery(query, QueryParams.ORGANIZATION.key(), "organization", andBsonList);
        createOrQuery(query, QueryParams.STATUS.key(), "status", andBsonList);
        createOrQuery(query, QueryParams.LAST_ACTIVITY.key(), "lastActivity", andBsonList);

        createOrQuery(query, QueryParams.PROJECT_ID.key(), "project.id", andBsonList);
        createOrQuery(query, QueryParams.PROJECT_NAME.key(), "project.name", andBsonList);
        createOrQuery(query, QueryParams.PROJECT_ALIAS.key(), "project.alias", andBsonList);
        createOrQuery(query, QueryParams.PROJECT_ORGANIZATION.key(), "project.organization", andBsonList);
        createOrQuery(query, QueryParams.PROJECT_STATUS.key(), "project.status", andBsonList);
        createOrQuery(query, QueryParams.PROJECT_LAST_ACTIVITY.key(), "project.lastActivity", andBsonList);

        createOrQuery(query, QueryParams.TOOL_ID.key(), "tool.id", andBsonList);
        createOrQuery(query, QueryParams.TOOL_NAME.key(), "tool.name", andBsonList);
        createOrQuery(query, QueryParams.TOOL_ALIAS.key(), "tool.alias", andBsonList);

        createOrQuery(query, QueryParams.SESSION_ID.key(), "session.id", andBsonList);
        createOrQuery(query, QueryParams.SESSION_IP.key(), "session.ip", andBsonList);
        createOrQuery(query, QueryParams.SESSION_LOGIN.key(), "session.login", andBsonList);
        createOrQuery(query, QueryParams.SESSION_LOGOUT.key(), "session.logout", andBsonList);

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
