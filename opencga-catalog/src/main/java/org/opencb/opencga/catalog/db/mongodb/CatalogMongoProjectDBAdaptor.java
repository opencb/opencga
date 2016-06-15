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

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang.NotImplementedException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ProjectConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by imedina on 08/01/16.
 */
public class CatalogMongoProjectDBAdaptor extends CatalogMongoDBAdaptor implements CatalogProjectDBAdaptor {

    private final MongoDBCollection userCollection;
    private ProjectConverter projectConverter;

    public CatalogMongoProjectDBAdaptor(MongoDBCollection userCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoProjectDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.userCollection = userCollection;
        this.projectConverter = new ProjectConverter();
    }

    @Override
    public boolean projectExists(long projectId) {
        QueryResult<Long> count = userCollection.count(new Document("projects.id", projectId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<Study> studies = project.getStudies();
        if (studies == null) {
            studies = Collections.emptyList();
        }
        project.setStudies(Collections.<Study>emptyList());


        // Check if project.alias already exists.
//        DBObject countQuery = BasicDBObjectBuilder
//                .start("id", userId)
//                .append("projects.alias", project.getAlias())
//                .get();
        Bson countQuery = Filters.and(Filters.eq("id", userId), Filters.eq("projects.alias", project.getAlias()));
        QueryResult<Long> count = userCollection.count(countQuery);
        if (count.getResult().get(0) != 0) {
            throw new CatalogDBException("Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }
//        if(getProjectId(userId, project.getAlias()) >= 0){
//            throw new CatalogManagerException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
//        }

        //Generate json
//        int projectId = CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);
        long projectId = dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
        project.setId(projectId);
//        DBObject query = new BasicDBObject("id", userId);
//        query.put("projects.alias", new BasicDBObject("$ne", project.getAlias()));
        Bson query = Filters.and(Filters.eq("id", userId), Filters.ne("projects.alias", project.getAlias()));

        Document projectDocument = getMongoDBDocument(project, "Project");
//        DBObject update = new BasicDBObject("$push", new BasicDBObject("projects", projectDBObject));
        Bson update = Updates.push("projects", projectDocument);

        //Update object
//        QueryResult<WriteResult> queryResult = userCollection.update(query, update, null);
        QueryResult<UpdateResult> queryResult = userCollection.update(query, update, null);

        if (queryResult.getResult().get(0).getModifiedCount() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }

        String errorMsg = "";
        for (Study study : studies) {
            String studyErrorMsg = dbAdaptorFactory.getCatalogStudyDBAdaptor().createStudy(project.getId(), study, options).getErrorMsg();
            if (studyErrorMsg != null && !studyErrorMsg.isEmpty()) {
                errorMsg += ", " + study.getAlias() + ":" + studyErrorMsg;
            }
        }
        List<Project> result = getProject(project.getId(), null).getResult();
        return endQuery("Create Project", startTime, result, errorMsg, null);
    }

    @Override
    public QueryResult<Project> getProject(long projectId, QueryOptions options) throws CatalogDBException {
        checkProjectId(projectId);
        return get(new Query(QueryParams.ID.key(), projectId).append(QueryParams.STATUS_STATUS.key(), "!=" + Status.REMOVED), options);
//
//        long startTime = startQuery();
//        Bson query = Filters.eq("projects.id", projectId);
//        Bson projection = Projections.elemMatch("projects", Filters.eq("id", projectId));
//
//        QueryResult<Document> result = userCollection.find(query, projection, options);
//
//        User user = parseUser(result);
//
//        if (user == null || user.getProjects().isEmpty()) {
//            throw CatalogDBException.idNotFound("Project", projectId);
//        }
//        // Fixme: Check the code below
//        List<Project> projects = user.getProjects();
//        joinFields(projects.get(0), options);
//
//        return endQuery("Get project", startTime, projects);
    }

    /**
     * At the moment it does not clean external references to itself.
     */
//    @Override
//    public QueryResult<Integer> deleteProject(int projectId) throws CatalogDBException {
//        long startTime = startQuery();
//        DBObject query = new BasicDBObject("projects.id", projectId);
//        DBObject pull = new BasicDBObject("$pull",
//                new BasicDBObject("projects",
//                        new BasicDBObject("id", projectId)));
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
    public QueryResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.USER_ID.key(), userId);
        return endQuery("User projects list", startTime, get(query, options).getResult());
    }


    /*
     * db.user.update(
     * {
     * "projects.id" : projectId,
     * "projects.alias" : {
     * $ne : newAlias
     * }
     * },
     * {
     * $set:{
     * "projects.$.alias":newAlias
     * }
     * })
     */
    @Override
    public QueryResult renameProjectAlias(long projectId, String newProjectAlias) throws CatalogDBException {
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

        /*
        DBObject query = BasicDBObjectBuilder
                .start("projects.id", projectId)
                .append("projects.alias", new BasicDBObject("$ne", newProjectAlias))    // check that any other project in the user has
                // the new name
                .get();
        DBObject update = new BasicDBObject("$set",
                new BasicDBObject("projects.$.alias", newProjectAlias));
*/
        Bson query = Filters.and(Filters.eq("projects.id", projectId),
                Filters.ne("projects.alias", newProjectAlias));
        Bson update = Updates.set("projects.$.alias", newProjectAlias);

        QueryResult<UpdateResult> result = userCollection.update(query, update, null);
        if (result.getResult().get(0).getModifiedCount() == 0) {    //Check if the the study has been inserted
            throw new CatalogDBException("Project {alias:\"" + newProjectAlias + "\"} already exists");
        }
        return endQuery("rename project alias", startTime, result);
    }

//    @Deprecated
//    @Override
//    public QueryResult<Project> modifyProject(long projectId, ObjectMap parameters) throws CatalogDBException {
//        long startTime = startQuery();
//
//        if (!projectExists(projectId)) {
//            throw CatalogDBException.idNotFound("Project", projectId);
//        }
//        //BasicDBObject projectParameters = new BasicDBObject();
//        Bson projectParameters = new Document();
//
//        String[] acceptedParams = {"name", "creationDate", "description", "organization", "status", "lastActivity"};
//        for (String s : acceptedParams) {
//            if (parameters.containsKey(s)) {
//                ((Document) projectParameters).put("projects.$." + s, parameters.getString(s));
//            }
//        }
//        String[] acceptedIntParams = {"diskQuota", "diskUsage"};
//        for (String s : acceptedIntParams) {
//            if (parameters.containsKey(s)) {
//                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
//                if (anInt != Integer.MIN_VALUE) {
//                    ((Document) projectParameters).put(s, anInt);
//                }
//            }
//        }
//        Map<String, Object> attributes = parameters.getMap("attributes");
//        if (attributes != null) {
//            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
//                ((Document) projectParameters).put("projects.$.attributes." + entry.getKey(), entry.getValue());
//            }
////            projectParameters.put("projects.$.attributes", attributes);
//        }
//
//        if (!((Document) projectParameters).isEmpty()) {
//            Bson query = Filters.eq("projects.id", projectId);
//            Bson updates = new Document("$set", projectParameters);
//            // Fixme: Updates
//                    /*
//            BasicDBObject query = new BasicDBObject("projects.id", projectId);
//            BasicDBObject updates = new BasicDBObject("$set", projectParameters);
//            */
//            QueryResult<UpdateResult> updateResult = userCollection.update(query, updates, null);
//            if (updateResult.getResult().get(0).getModifiedCount() == 0) {
//                throw CatalogDBException.idNotFound("Project", projectId);
//            }
//        }
//        /*
//        if (!projectParameters.isEmpty()) {
//            BasicDBObject query = new BasicDBObject("projects.id", projectId);
//            BasicDBObject updates = new BasicDBObject("$set", projectParameters);
//            QueryResult<WriteResult> updateResult = userCollection.update(query, updates, null);
//            if (updateResult.getResult().get(0).getN() == 0) {
//                throw CatalogDBException.idNotFound("Project", projectId);
//            }
//        }
//        */
//        return endQuery("Modify project", startTime, getProject(projectId, null));
//    }

    @Override
    public long getProjectId(String userId, String projectAlias) throws CatalogDBException {
        QueryResult<Document> queryResult = userCollection.find(
                new BsonDocument("projects.alias", new BsonString(projectAlias))
                        .append("id", new BsonString(userId)),
                Projections.fields(Projections.include("projects.id"),
                        Projections.elemMatch("projects", Filters.eq("alias", projectAlias))),
                null);
/*
        QueryResult<DBObject> queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", projectAlias)
                        .append("id", userId).get(),
                BasicDBObjectBuilder.start("projects.id", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", projectAlias))).get(),
                null
        );*/
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            return -1;
        } else {
            return user.getProjects().get(0).getId();
        }
    }

    @Override
    public String getProjectOwnerId(long projectId) throws CatalogDBException {
//        DBObject query = new BasicDBObject("projects.id", projectId);
        Bson query = Filters.eq("projects.id", projectId);

//        DBObject projection = new BasicDBObject("id", "true");
        Bson projection = Projections.include("id");

//        QueryResult<DBObject> result = userCollection.find(query, projection, null);
        QueryResult<Document> result = userCollection.find(query, projection, null);

        if (result.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("Project", projectId);
        } else {
            return result.getResult().get(0).get("id").toString();
        }
    }

    /*
    @Deprecated
    public AclEntry getFullProjectAcl(int projectId, String userId) throws CatalogDBException {
        QueryResult<Project> project = getProject(projectId, null);
        if (project.getNumResults() != 0) {
            List<AclEntry> acl = project.getResult().get(0).getAcls();
            for (AclEntry acl1 : acl) {
                if (userId.equals(acl1.getUserId())) {
                    return acl1;
                }
            }
        }
        return null;
    }
*/

    /*
     * db.user.aggregate(
     * {"$match": {"projects.id": 2}},
     * {"$project": {"projects.acl":1, "projects.id":1}},
     * {"$unwind": "$projects"},
     * {"$match": {"projects.id": 2}},
     * {"$unwind": "$projects.acl"},
     * {"$match": {"projects.acl.userId": "jmmut"}}).pretty()
     */
    @Override
    public QueryResult<AclEntry> getProjectAcl(long projectId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        /*
        DBObject match1 = new BasicDBObject("$match", new BasicDBObject("projects.id", projectId));
        DBObject project = new BasicDBObject("$project", BasicDBObjectBuilder
                .start("_id", false)
                .append("projects.acl", true)
                .append("projects.id", true).get());
        DBObject unwind1 = new BasicDBObject("$unwind", "$projects");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("projects.id", projectId));
        DBObject unwind2 = new BasicDBObject("$unwind", "$projects.acl");
        DBObject match3 = new BasicDBObject("$match", new BasicDBObject("projects.acl.userId", userId));

*/
        Bson match1 = Aggregates.match(Filters.eq("projects.id", projectId));
//        Bson project = Projections.fields(Projections.excludeId(), Projections.include("projects.acl", "projects.id"));
        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("projects.acl", "projects.id")));
        Bson unwind1 = Aggregates.unwind("$projects");
        Bson match2 = Aggregates.match(Filters.eq("projects.id", projectId));
        Bson unwind2 = Aggregates.unwind("$projects.acl");
        Bson match3 = Aggregates.match(Filters.eq("projects.acl.userId", userId));

        List<Bson> operations = new LinkedList<>();
        operations.add(match1);
        operations.add(project);
        operations.add(unwind1);
        operations.add(match2);
        operations.add(unwind2);
        operations.add(match3);

        QueryResult aggregate = userCollection.aggregate(operations, null);
        List<AclEntry> acls = new LinkedList<>();
        if (aggregate.getNumResults() != 0) {
//            DBObject aclObject = (DBObject) ((DBObject) ((DBObject) aggregate.getResult().get(0)).get("projects")).get("acl");
            Document aclObject = (Document) ((Document) ((Document) aggregate.getResult().get(0)).get("projects")).get("acl");
            AclEntry acl = parseObject(aclObject, AclEntry.class);
            acls.add(acl);
        }
        return endQuery("get project ACL", startTime, acls);
    }

    @Override
    public QueryResult setProjectAcl(long projectId, AclEntry newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();
        if (!dbAdaptorFactory.getCatalogUserDBAdaptor().userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        Document newAclObject = getMongoDBDocument(newAcl, "ACL");
        //DBObject newAclObject = getDbObject(newAcl, "ACL");

        List<AclEntry> projectAcls = getProjectAcl(projectId, userId).getResult();
        Bson query = new Document("projects.id", projectId);
        Bson push = Updates.push("projects.$.acl", newAclObject);
        if (!projectAcls.isEmpty()) { // ensure that there is no acl for that user in that project. pull
            Bson pull = Updates.pull("projects.$.acl", Filters.eq("userId", userId));
            userCollection.update(query, pull, null);
        }
        /*
        DBObject query = new BasicDBObject("projects.id", projectId);
        BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("projects.$.acl", newAclObject));
        if (!projectAcls.isEmpty()) {  // ensure that there is no acl for that user in that project. pull
            DBObject pull = new BasicDBObject("$pull", new BasicDBObject("projects.$.acl", new BasicDBObject("userId", userId)));
            userCollection.update(query, pull, null);
        }
        */
        //Put study
        QueryResult pushResult = userCollection.update(query, push, null);
        return endQuery("Set project acl", startTime, pushResult);
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
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return userCollection.count(bson);
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
    public QueryResult<Project> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
        List<Bson> aggregates = new ArrayList<>();

        aggregates.add(Aggregates.unwind("$projects"));
        aggregates.add(Aggregates.match(parseQuery(query)));

        // Check include
        if (options != null && options.containsKey(QueryOptions.INCLUDE)) {
            List<String> includeList = new ArrayList<>();
            List<String> optionsAsStringList = options.getAsStringList(QueryOptions.INCLUDE);
            includeList.addAll(optionsAsStringList.stream().collect(Collectors.toList()));

            if (includeList.size() > 0) {
                aggregates.add(Aggregates.project(Projections.include(includeList)));
            }
        }

        QueryResult<Project> projectQueryResult = userCollection.aggregate(aggregates, projectConverter, options);

        if (options == null || !options.containsKey(QueryOptions.EXCLUDE)
                || !options.getAsStringList(QueryOptions.EXCLUDE).contains("study")) {
            for (Project project : projectQueryResult.getResult()) {
                Query studyQuery = new Query(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), project.getId());
                try {
                    QueryResult<Study> studyQueryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(studyQuery, options);
                    project.setStudies(studyQueryResult.getResult());
                } catch (CatalogDBException e) {
                    e.printStackTrace();
                }
            }
        }

        return endQuery("Get project", startTime, projectQueryResult.getResult());
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
        List<Bson> aggregates = new ArrayList<>();
        aggregates.add(Aggregates.match(parseQuery(query)));
        aggregates.add(Aggregates.unwind("$projects"));

        // Check include & excludes
        if (options != null && options.containsKey(QueryOptions.INCLUDE)) {
            List<String> includeList = new ArrayList<>();
            List<String> optionsAsStringList = options.getAsStringList(QueryOptions.INCLUDE);
            includeList.addAll(optionsAsStringList.stream().collect(Collectors.toList()));

            if (includeList.size() > 0) {
                aggregates.add(Aggregates.project(Projections.include(includeList)));
            }
        }

        QueryResult<Document> projectQueryResult = userCollection.aggregate(aggregates, options);
        ArrayList<Document> returnedProjectList = new ArrayList<>();

        for (Document user : projectQueryResult.getResult()) {
            Document project = (Document) user.get("projects");
            Query studyQuery = new Query(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), project.get("id"));
            QueryResult studyQueryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, options);
            project.remove("studies");
            project.append("studies", studyQueryResult.getResult());
            returnedProjectList.add(project);
        }

        projectQueryResult.setResult(returnedProjectList);

        return projectQueryResult;

    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        Bson projectParameters = new Document();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.CREATION_DATE.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.ORGANIZATION.key(), QueryParams.LAST_ACTIVITY.key(), };
        for (String s : acceptedParams) {
            if (parameters.containsKey(s)) {
                ((Document) projectParameters).put("projects.$." + s, parameters.getString(s));
            }
        }
        String[] acceptedIntParams = {QueryParams.DISK_USAGE.key()};
        for (String s : acceptedIntParams) {
            if (parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if (anInt != Integer.MIN_VALUE) {
                    ((Document) projectParameters).put(s, anInt);
                }
            }
        }
        Map<String, Object> attributes = parameters.getMap(QueryParams.ATTRIBUTES.key());
        if (attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                ((Document) projectParameters).put("projects.$.attributes." + entry.getKey(), entry.getValue());
            }
//            projectParameters.put("projects.$.attributes", attributes);
        }

        if (parameters.containsKey(QueryParams.STATUS_STATUS.key())) {
            ((Document) projectParameters).put("projects.$." + QueryParams.STATUS_STATUS.key(),
                    parameters.get(QueryParams.STATUS_STATUS.key()));
            ((Document) projectParameters).put("projects.$." + QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        QueryResult<UpdateResult> updateResult = new QueryResult<>();
        if (!((Document) projectParameters).isEmpty()) {
            Bson bsonQuery = parseQuery(query);
            Bson updates = new Document("$set", projectParameters);
            // Fixme: Updates
                    /*
            BasicDBObject query = new BasicDBObject("projects.id", projectId);
            BasicDBObject updates = new BasicDBObject("$set", projectParameters);
            */
            updateResult = userCollection.update(bsonQuery, updates, null);
        }
        /*
        if (!projectParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject("projects.id", projectId);
            BasicDBObject updates = new BasicDBObject("$set", projectParameters);
            QueryResult<WriteResult> updateResult = userCollection.update(query, updates, null);
            if (updateResult.getResult().get(0).getN() == 0) {
                throw CatalogDBException.idNotFound("Project", projectId);
            }
        }
        */
        return endQuery("Update project", startTime, Collections.singletonList(updateResult.first().getModifiedCount()));
    }

    @Override
    public QueryResult<Project> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        checkProjectId(id);
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters);
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update project with id " + id);
        }
        return endQuery("Update project", startTime, getProject(id, null));
    }

    @Override
    public QueryResult<Project> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkProjectId(id);
        // Check the project is active
        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), Status.READY);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED + "," + Status.REMOVED);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_STATUS.key());
            Project project = get(query, options).first();
            throw new CatalogDBException("The project {" + id + "} was already " + project.getStatus().getStatus());
        }

        // If we don't find the force parameter, we check first if the user does not have an active project.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            // Delete the active studies (if any)
            query = new Query(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), id);
            dbAdaptorFactory.getCatalogStudyDBAdaptor().delete(query, queryOptions);
        }

        // Change the status of the project to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), Status.DELETED);

        return endQuery("Delete project", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_STATUS.key(), Status.READY);
        QueryResult<Project> projectQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Project project : projectQueryResult.getResult()) {
            delete(project.getId(), queryOptions);
        }
        return endQuery("Delete project", startTime, Collections.singletonList(projectQueryResult.getNumTotalResults()));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    private QueryResult<Project>  setStatus(long projectId, String status) throws CatalogDBException {
        return update(projectId, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
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
        query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        return endQuery("Restore projects", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Project> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkProjectId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The project {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore project", startTime, get(query, null));
    }

    @Override
    public CatalogDBIterator<Project> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = userCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, projectConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = userCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
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
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<Project> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery("projects." + queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case USER_ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    default:
                        addAutoOrQuery("projects." + queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
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
        checkProjectId(projectId);
        Query query = new Query(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId)
                .append(CatalogStudyDBAdaptor.QueryParams.STATUS_STATUS.key(), Status.READY);
        Long count = dbAdaptorFactory.getCatalogStudyDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The project {" + projectId + "} cannot be deleted. The project has " + count
                    + " studies in use.");
        }
    }
}
