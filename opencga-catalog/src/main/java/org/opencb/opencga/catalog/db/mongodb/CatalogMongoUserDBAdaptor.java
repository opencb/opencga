package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.*;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor._ID;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoUserDBAdaptor extends CatalogDBAdaptor implements CatalogUserDBAdaptor {

    private final MongoDBCollection userCollection;
    private final MongoDBCollection metaCollection;
    private final CatalogDBAdaptorFactory dbAdaptorFactory;

    public CatalogMongoUserDBAdaptor(CatalogDBAdaptorFactory dbAdaptorFactory, MongoDBCollection metaCollection, MongoDBCollection userCollection) {
        super(LoggerFactory.getLogger(CatalogMongoUserDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaCollection;
        this.userCollection = userCollection;
    }

    /** **************************
     * User methods
     * ***************************
     */

    @Override
    public boolean checkUserCredentials(String userId, String sessionId) {
        return false;
    }

    @Override
    public boolean userExists(String userId){
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

        if(userExists(userId)) {
            throw new CatalogDBException("User {id:\"" + userId + "\"} already exists");
        }
        return null;

    }

    @Override
    public QueryResult<User> insertUser(User user, QueryOptions options) throws CatalogDBException {
        checkParameter(user, "user");
        long startTime = startQuery();

        if(userExists(user.getId())) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        List<Project> projects = user.getProjects();
        user.setProjects(Collections.<Project>emptyList());
        user.setLastActivity(TimeUtils.getTimeMillis());
        DBObject userDBObject = getDbObject(user, "User " + user.getId());
        userDBObject.put(_ID, user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDBObject, null);
        } catch (DuplicateKeyException e) {
            throw new CatalogDBException("User {id:\""+user.getId()+"\"} already exists");
        }

        String errorMsg = insert.getErrorMsg() != null ? insert.getErrorMsg() : "";
        for (Project p : projects) {
            String projectErrorMsg = createProject(user.getId(), p, options).getErrorMsg();
            if(projectErrorMsg != null && !projectErrorMsg.isEmpty()){
                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
            }
        }

        //Get the inserted user.
        user.setProjects(projects);
        List<User> result = getUser(user.getId(), options, "").getResult();

        return endQuery("insertUser", startTime, result, errorMsg, null);
    }

    /**
     * TODO: delete user from:
     *      project acl and owner
     *      study acl and owner
     *      file acl and creator
     *      job userid
     * also, delete his:
     *      projects
     *      studies
     *      analysesS
     *      jobs
     *      files
     */
    @Override
    public QueryResult<Integer> deleteUser(String userId) throws CatalogDBException {
        checkParameter(userId, "userId");
        long startTime = startQuery();

//        WriteResult id = nativeUserCollection.remove(new BasicDBObject("id", userId));
        WriteResult wr = userCollection.remove(new BasicDBObject(_ID, userId), null).getResult().get(0);
        if (wr.getN() == 0) {
            throw CatalogDBException.idNotFound("User", userId);
        } else {
            return endQuery("Delete user", startTime, Arrays.asList(wr.getN()));
        }
    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException {
        checkParameter(userId, "userId");
        checkParameter(password, "password");
        long startTime = startQuery();

        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder.start("id", userId).append("password", password).get());
        if(count.getResult().get(0) == 0){
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
        if(userIdBySessionId.isEmpty()){
            return endQuery("logout", startTime, null, "", "Session not found");
        }
        if(userIdBySessionId.equals(userId)){
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
        if(countSessions.getResult().get(0) != 0){
            throw new CatalogDBException("Error, sessionID already exists");
        }
        String userId = "anonymous_" + session.getId();
        User user = new User(userId, "Anonymous", "", "", "", User.Role.ANONYMOUS, "");
        user.getSessions().add(session);
        DBObject anonymous = getDbObject(user, "User");
        anonymous.put(_ID, user.getId());

        try {
            userCollection.insert(anonymous, null);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogDBException("Anonymous user {id:\""+user.getId()+"\"} already exists");
        }

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
        long startTime = startQuery();
        if (!userExists(userId)) {
            throw CatalogDBException.idNotFound("User", userId);
        }
        DBObject query = new BasicDBObject(_ID, userId);
        query.put("lastActivity", new BasicDBObject("$ne", lastActivity));
        QueryResult<DBObject> result = userCollection.find(query, options);
        User user = parseUser(result);
        if(user == null) {
            return endQuery("Get user", startTime); // user exists but no different lastActivity was found: return empty result
        } else {
            joinFields(user, options);
            return endQuery("Get user", startTime, Collections.singletonList(user));
        }
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("password", oldPassword);
        BasicDBObject fields = new BasicDBObject("password", newPassword);
        BasicDBObject action = new BasicDBObject("$set", fields);
        QueryResult<WriteResult> update = userCollection.update(query, action, null);
        if(update.getResult().get(0).getN() == 0){  //0 query matches.
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

        if(!userParameters.isEmpty()) {
            QueryResult<WriteResult> update = userCollection.update(
                    new BasicDBObject(_ID, userId),
                    new BasicDBObject("$set", userParameters), null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
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
        if(update.getResult().get(0).getN() == 0){  //0 query matches.
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
    public String getUserIdBySessionId(String sessionId){
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


    /**
     * Project methods
     * ***************************
     */

    @Override
    public boolean projectExists(int projectId) {
        QueryResult<Long> count = userCollection.count(new BasicDBObject("projects.id", projectId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<Study> studies = project.getStudies();
        if(studies == null) {
            studies = Collections.emptyList();
        }
        project.setStudies(Collections.<Study>emptyList());


        // Check if project.alias already exists.
        DBObject countQuery = BasicDBObjectBuilder
                .start("id", userId)
                .append("projects.alias", project.getAlias())
                .get();
        QueryResult<Long> count = userCollection.count(countQuery);
        if(count.getResult().get(0) != 0){
            throw new CatalogDBException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }
//        if(getProjectId(userId, project.getAlias()) >= 0){
//            throw new CatalogManagerException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
//        }

        //Generate json
        int projectId = CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);
        project.setId(projectId);
        DBObject query = new BasicDBObject("id", userId);
        query.put("projects.alias", new BasicDBObject("$ne", project.getAlias()));
        DBObject projectDBObject = getDbObject(project, "Project");
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("projects", projectDBObject));

        //Update object
        QueryResult<WriteResult> queryResult = userCollection.update(query, update, null);

        if (queryResult.getResult().get(0).getN() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }

        String errorMsg = "";
        for (Study study : studies) {
            String studyErrorMsg = dbAdaptorFactory.getCatalogStudyDBAdaptor().createStudy(project.getId(), study, options).getErrorMsg();
            if(studyErrorMsg != null && !studyErrorMsg.isEmpty()){
                errorMsg += ", " + study.getAlias() + ":" + studyErrorMsg;
            }
        }
        List<Project> result = getProject(project.getId(), null).getResult();
        return endQuery("Create Project", startTime, result, errorMsg, null);
    }

    @Override
    public QueryResult<Project> getProject(int projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = new BasicDBObject(
                "projects",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("id", projectId)
                )
        );
        QueryResult<DBObject> result = userCollection.find(query, projection, options);
        User user = parseUser(result);
        if(user == null || user.getProjects().isEmpty()) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }
        List<Project> projects = user.getProjects();
        joinFields(projects.get(0), options);

        return endQuery("Get project", startTime, projects);
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteProject(int projectId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject pull = new BasicDBObject("$pull",
                new BasicDBObject("projects",
                        new BasicDBObject("id", projectId)));

        QueryResult<WriteResult> update = userCollection.update(query, pull, null);
        List<Integer> deletes = new LinkedList<>();
        if (update.getResult().get(0).getN() == 0) {
            throw CatalogDBException.idNotFound("Project", projectId);
        } else {
            deletes.add(update.getResult().get(0).getN());
            return endQuery("delete project", startTime, deletes);
        }
    }

    @Override
    public QueryResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject("projects", true);
        projection.put("_id", false);
        QueryResult<DBObject> result = userCollection.find(query, projection, options);

        User user = parseUser(result);
        List<Project> projects = user.getProjects();
        for (Project project : projects) {
            joinFields(project, options);
        }
        return endQuery(
                "User projects list", startTime,
                projects);
    }


    /**
     db.user.update(
     {
     "projects.id" : projectId,
     "projects.alias" : {
     $ne : newAlias
     }
     },
     {
     $set:{
     "projects.$.alias":newAlias
     }
     })
     */
    @Override
    public QueryResult renameProjectAlias(int projectId, String newProjectAlias) throws CatalogDBException {
        long startTime = startQuery();
//        String projectOwner = getProjectOwner(projectId);
//
//        int collisionProjectId = getProjectId(projectOwner, newProjectAlias);
//        if (collisionProjectId != -1) {
//            throw new CatalogManagerException("Couldn't rename project alias, alias already used in the same user");
//        }

        QueryResult<Project> projectResult = getProject(projectId, null); // if projectId doesn't exist, an exception is raised
        Project project = projectResult.getResult().get(0);

        //String oldAlias = project.getAlias();
        project.setAlias(newProjectAlias);

        DBObject query = BasicDBObjectBuilder
                .start("projects.id", projectId)
                .append("projects.alias", new BasicDBObject("$ne", newProjectAlias))    // check that any other project in the user has the new name
                .get();
        DBObject update = new BasicDBObject("$set",
                new BasicDBObject("projects.$.alias", newProjectAlias));

        QueryResult<WriteResult> result = userCollection.update(query, update, null);
        if (result.getResult().get(0).getN() == 0) {    //Check if the the study has been inserted
            throw new CatalogDBException("Project {alias:\"" + newProjectAlias+ "\"} already exists");
        }
        return endQuery("rename project alias", startTime, result);
    }

    @Override
    public QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        if (!projectExists(projectId)) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }
        BasicDBObject projectParameters = new BasicDBObject();

        String[] acceptedParams = {"name", "creationDate", "description", "organization", "status", "lastActivity"};
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                projectParameters.put("projects.$."+s, parameters.getString(s));
            }
        }
        String[] acceptedIntParams = {"diskQuota", "diskUsage"};
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if(anInt != Integer.MIN_VALUE) {
                    projectParameters.put(s, anInt);
                }
            }
        }
        Map<String, Object> attributes = parameters.getMap("attributes");
        if(attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                projectParameters.put("projects.$.attributes."+entry.getKey(), entry.getValue());
            }
//            projectParameters.put("projects.$.attributes", attributes);
        }

        if(!projectParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject("projects.id", projectId);
            BasicDBObject updates = new BasicDBObject("$set", projectParameters);
            QueryResult<WriteResult> updateResult = userCollection.update(query, updates, null);
            if(updateResult.getResult().get(0).getN() == 0){
                throw CatalogDBException.idNotFound("Project", projectId);
            }
        }
        return endQuery("Modify project", startTime);
    }

    @Override
    public int getProjectId(String userId, String projectAlias) throws CatalogDBException {
        QueryResult<DBObject> queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", projectAlias)
                        .append("id", userId).get(),
                BasicDBObjectBuilder.start("projects.id", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", projectAlias))).get(),
                null
        );
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            return -1;
        } else {
            return user.getProjects().get(0).getId();
        }
    }

    @Override
    public String getProjectOwnerId(int projectId) throws CatalogDBException {
        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = new BasicDBObject("id", "true");
        QueryResult<DBObject> result = userCollection.find(query, projection, null);

        if(result.getResult().isEmpty()){
            throw CatalogDBException.idNotFound("Project", projectId);
        } else {
            return result.getResult().get(0).get("id").toString();
        }
    }

    public AclEntry getFullProjectAcl(int projectId, String userId) throws CatalogDBException {
        QueryResult<Project> project = getProject(projectId, null);
        if (project.getNumResults() != 0) {
            List<AclEntry> acl = project.getResult().get(0).getAcl();
            for (AclEntry acl1 : acl) {
                if (userId.equals(acl1.getUserId())) {
                    return acl1;
                }
            }
        }
        return null;
    }
    /**
     * db.user.aggregate(
     * {"$match": {"projects.id": 2}},
     * {"$project": {"projects.acl":1, "projects.id":1}},
     * {"$unwind": "$projects"},
     * {"$match": {"projects.id": 2}},
     * {"$unwind": "$projects.acl"},
     * {"$match": {"projects.acl.userId": "jmmut"}}).pretty()
     */
    @Override
    public QueryResult<AclEntry> getProjectAcl(int projectId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject match1 = new BasicDBObject("$match", new BasicDBObject("projects.id", projectId));
        DBObject project = new BasicDBObject("$project", BasicDBObjectBuilder
                .start("_id", false)
                .append("projects.acl", true)
                .append("projects.id", true).get());
        DBObject unwind1 = new BasicDBObject("$unwind", "$projects");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("projects.id", projectId));
        DBObject unwind2 = new BasicDBObject("$unwind", "$projects.acl");
        DBObject match3 = new BasicDBObject("$match", new BasicDBObject("projects.acl.userId", userId));

        List<DBObject> operations = new LinkedList<>();
        operations.add(match1);
        operations.add(project);
        operations.add(unwind1);
        operations.add(match2);
        operations.add(unwind2);
        operations.add(match3);
        QueryResult aggregate = userCollection.aggregate(operations, null);

        List<AclEntry> acls = new LinkedList<>();
        if (aggregate.getNumResults() != 0) {
            DBObject aclObject = (DBObject) ((DBObject) ((DBObject) aggregate.getResult().get(0)).get("projects")).get("acl");
            AclEntry acl = parseObject(aclObject, AclEntry.class);
            acls.add(acl);
        }
        return endQuery("get project ACL", startTime, acls);
    }

    @Override
    public QueryResult setProjectAcl(int projectId, AclEntry newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        DBObject newAclObject = getDbObject(newAcl, "ACL");

        List<AclEntry> projectAcls = getProjectAcl(projectId, userId).getResult();
        DBObject query = new BasicDBObject("projects.id", projectId);
        BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("projects.$.acl", newAclObject));
        if (!projectAcls.isEmpty()) {  // ensure that there is no acl for that user in that project. pull
            DBObject pull = new BasicDBObject("$pull", new BasicDBObject("projects.$.acl", new BasicDBObject("userId", userId)));
            userCollection.update(query, pull, null);
        }
        //Put study
        QueryResult pushResult = userCollection.update(query, push, null);
        return endQuery("Set project acl", startTime, pushResult);
    }


//    public QueryResult<Project> searchProject(QueryOptions query, QueryOptions options) throws CatalogDBException {
//        long startTime = startQuery();
//
//
//        return endQuery("Search Proyect", startTime, projects);
//    }

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
}
