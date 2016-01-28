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

import com.mongodb.WriteResult;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.StudyConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created on 07/09/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoStudyDBAdaptor extends CatalogMongoDBAdaptor implements CatalogStudyDBAdaptor {

    private final MongoDBCollection studyCollection;
    private StudyConverter studyConverter;

    public CatalogMongoStudyDBAdaptor(MongoDBCollection studyCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoStudyDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.studyCollection = studyCollection;
        this.studyConverter = new StudyConverter();
    }

    /*
     * Study methods
     * ***************************
     */

//    @Override
//    public boolean studyExists(int studyId) {
//        QueryResult<Long> count = studyCollection.count(new BasicDBObject(PRIVATE_ID, studyId));
//        return count.getResult().get(0) != 0;
//    }
//
//    @Override
//    public void checkStudyId(int studyId) throws CatalogDBException {
//        if (!studyExists(studyId)) {
//            throw CatalogDBException.idNotFound("Study", studyId);
//        }
//    }
    private boolean studyAliasExists(int projectId, String studyAlias) throws CatalogDBException {
        if (projectId < 0) {
            throw CatalogDBException.newInstance("Project id '{}' is not valid: ", projectId);
        }

        Query query = new Query(PRIVATE_PROJECT_ID, projectId).append("alias", studyAlias);
        QueryResult<Long> count = count(query);
        return count.first() != 0;
    }

    @Override
    public QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (projectId < 0) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }

        // Check if study.alias already exists.
        if (studyAliasExists(projectId, study.getAlias())) {
            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }

        //Set new ID
//        int newId = getNewAutoIncrementId(metaCollection);
        int newId = getNewId();
        study.setId(newId);

        //Empty nested fields
        List<File> files = study.getFiles();
        study.setFiles(Collections.<File>emptyList());

        List<Job> jobs = study.getJobs();
        study.setJobs(Collections.<Job>emptyList());

        //Create DBObject
        Document studyObject = getMongoDBDocument(study, "Study");
        studyObject.put(PRIVATE_ID, newId);

        //Set ProjectId
        studyObject.put(PRIVATE_PROJECT_ID, projectId);

        //Insert
        QueryResult<WriteResult> updateResult = studyCollection.insert(studyObject, null);

        //Check if the the study has been inserted
//        if (updateResult.getResult().get(0).getN() == 0) {
//            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
//        }

        // Insert nested fields
        String errorMsg = updateResult.getErrorMsg() != null ? updateResult.getErrorMsg() : "";

        for (File file : files) {
            String fileErrorMsg = dbAdaptorFactory.getCatalogFileDBAdaptor().createFile(study.getId(), file, options).getErrorMsg();
            if (fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg += file.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        for (Job job : jobs) {
//            String jobErrorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
            String jobErrorMsg = dbAdaptorFactory.getCatalogJobDBAdaptor().createJob(study.getId(), job, options).getErrorMsg();
            if (jobErrorMsg != null && !jobErrorMsg.isEmpty()) {
                errorMsg += job.getName() + ":" + jobErrorMsg + ", ";
            }
        }

        QueryResult<Study> result = getStudy(study.getId(), options);
        List<Study> studyList = result.getResult();
        return endQuery("Create Study", startTime, studyList, errorMsg, null);

    }


    @Override
    public QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException {
        checkStudyId(studyId);
        return get(new Query(QueryParams.ID.key(), studyId), options);
    }

    @Override
    public QueryResult<Study> getAllStudiesInProject(int projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogProjectDbAdaptor().checkProjectId(projectId);
        Query query = new Query(PRIVATE_PROJECT_ID, projectId);
        return endQuery("getAllSudiesInProject", startTime, get(query, options));
    }

    @Deprecated
    public QueryResult<Study> getAllStudies(QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        List<Document> mongoQueryList = new LinkedList<>();

        for (Map.Entry<String, Object> entry : queryOptions.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                StudyFilterOptions option = StudyFilterOptions.valueOf(key);
                switch (option) {
                    case id:
                        addCompQueryFilter(option, option.name(), PRIVATE_ID, queryOptions, mongoQueryList);
                        break;
                    case projectId:
                        addCompQueryFilter(option, option.name(), PRIVATE_PROJECT_ID, queryOptions, mongoQueryList);
                        break;
                    default:
                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), queryKey, queryOptions, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        Document mongoQuery = new Document();

        if (!mongoQueryList.isEmpty()) {
            mongoQuery.put("$and", mongoQueryList);
        }

        QueryResult<Document> queryResult = studyCollection.find(mongoQuery, filterOptions(queryOptions, FILTER_ROUTE_STUDIES));
        List<Study> studies = parseStudies(queryResult);
        for (Study study : studies) {
            joinFields(study, queryOptions);
        }

        return endQuery("getAllStudies", startTime, studies);
    }

    @Override
    public void updateStudyLastActivity(int studyId) throws CatalogDBException {
        update(studyId, new ObjectMap("lastActivity", TimeUtils.getTime()));
    }

    @Deprecated
    @Override
    public QueryResult<Study> modifyStudy(int studyId, ObjectMap parameters) throws CatalogDBException {
        /*long startTime = startQuery();

        checkStudyId(studyId);
//        BasicDBObject studyParameters = new BasicDBObject();
        Document studyParameters = new Document();

        String[] acceptedParams = {"name", "creationDate", "creationId", "description", "status", "lastActivity", "cipher"};
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, parameters, acceptedLongParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("type"), Study.Type.class);
        filterEnumParams(parameters, studyParameters, acceptedEnums);

        if (parameters.containsKey("uri")) {
            URI uri = parameters.get("uri", URI.class);
            studyParameters.put("uri", uri.toString());
        }

        if (!studyParameters.isEmpty()) {
//            BasicDBObject query = new BasicDBObject(PRIVATE_ID, studyId);
            Bson eq = Filters.eq(PRIVATE_ID, studyId);
            BasicDBObject updates = new BasicDBObject("$set", studyParameters);

//            QueryResult<WriteResult> updateResult = studyCollection.update(query, updates, null);
            QueryResult<UpdateResult> updateResult = studyCollection.update(eq, updates, null);
            if (updateResult.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Study", studyId);
            }
        }
        return endQuery("Modify study", startTime, getStudy(studyId, null));
        */
        return null;
    }

    /**
     * At the moment it does not clean external references to itself.
     */
//    @Override
//    public QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException {
//        long startTime = startQuery();
//        DBObject query = new BasicDBObject(PRIVATE_ID, studyId);
//        QueryResult<WriteResult> remove = studyCollection.remove(query, null);
//
//        List<Integer> deletes = new LinkedList<>();
//
//        if (remove.getResult().get(0).getN() == 0) {
//            throw CatalogDBException.idNotFound("Study", studyId);
//        } else {
//            deletes.add(remove.getResult().get(0).getN());
//            return endQuery("delete study", startTime, deletes);
//        }
//    }
    @Override
    public int getStudyId(int projectId, String studyAlias) throws CatalogDBException {
        Query query1 = new Query(PRIVATE_PROJECT_ID, projectId).append("alias", studyAlias);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key());
        QueryResult<Study> studyQueryResult = get(query1, queryOptions);
        List<Study> studies = studyQueryResult.getResult();
        return studies == null || studies.isEmpty() ? -1 : studies.get(0).getId();
    }

    @Override
    public int getProjectIdByStudyId(int studyId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), studyId);
        QueryOptions queryOptions = new QueryOptions("include", FILTER_ROUTE_STUDIES + PRIVATE_PROJECT_ID);
        QueryResult result = nativeGet(query, queryOptions);

        if (!result.getResult().isEmpty()) {
            Document study = (Document) result.getResult().get(0);
            Object id = study.get(PRIVATE_PROJECT_ID);
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
    }

    @Override
    public String getStudyOwnerId(int studyId) throws CatalogDBException {
        int projectId = getProjectIdByStudyId(studyId);
        return dbAdaptorFactory.getCatalogProjectDbAdaptor().getProjectOwnerId(projectId);
    }


    private long getDiskUsageByStudy(int studyId) {
        /*
        List<DBObject> operations = Arrays.<DBObject>asList(
                new BasicDBObject(
                        "$match",
                        new BasicDBObject(
                                PRIVATE_STUDY_ID,
                                studyId
                                //new BasicDBObject("$in",studyIds)
                        )
                ),
                new BasicDBObject(
                        "$group",
                        BasicDBObjectBuilder
                                .start("_id", "$" + PRIVATE_STUDY_ID)
                                .append("diskUsage",
                                        new BasicDBObject(
                                                "$sum",
                                                "$diskUsage"
                                        )).get()
                )
        );*/
        List<Bson> operations = new ArrayList<>();
        operations.add(Aggregates.match(Filters.eq(PRIVATE_STUDY_ID, studyId)));
        operations.add(Aggregates.group("$" + PRIVATE_STUDY_ID, Accumulators.sum("diskUsage", "$diskUsage")));

//        Bson match = Aggregates.match(Filters.eq(PRIVATE_STUDY_ID, studyId));
//        Aggregates.group()

        QueryResult<Document> aggregate = dbAdaptorFactory.getCatalogFileDBAdaptor().getFileCollection()
                .aggregate(operations, null);
        if (aggregate.getNumResults() == 1) {
            Object diskUsage = aggregate.getResult().get(0).get("diskUsage");
            if (diskUsage instanceof Integer) {
                return ((Integer) diskUsage).longValue();
            } else if (diskUsage instanceof Long) {
                return ((Long) diskUsage);
            } else {
                return Long.parseLong(diskUsage.toString());
            }
        } else {
            return 0;
        }
    }


    @Override
    public QueryResult<Group> getGroup(int studyId, String userId, String groupId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Bson query = new Document(PRIVATE_ID, studyId);
        Document groupQuery = new Document();
        if (userId != null) {
            groupQuery.put("userIds", userId);
        }
        if (groupId != null) {
            groupQuery.put("id", groupId);
        }
        Bson projection = new Document("groups", new Document("$elemMatch", groupQuery));

        QueryResult<Document> queryResult = studyCollection.find(query, projection,
                filterOptions(options, FILTER_ROUTE_STUDIES + "groups."));
        List<Study> studies = CatalogMongoDBUtils.parseStudies(queryResult);
        List<Group> groups = new ArrayList<>(1);
        studies.stream().filter(study -> study.getGroups() != null).forEach(study -> {
            groups.addAll(study.getGroups());
        });
        return endQuery("getGroup", startTime, groups);
    }

    boolean groupExists(int studyId, String groupId) throws CatalogDBException {
        Query query = new Query(PRIVATE_ID, studyId).append(QueryParams.GROUP_ID.key(), groupId);
        return count(query).first() == 1;
    }

    @Override
    public QueryResult<Group> addMemberToGroup(int studyId, String groupId, String userId) throws CatalogDBException {
        long startTime = startQuery();

        if (!groupExists(studyId, groupId)) {
            throw new CatalogDBException("Group \"" + groupId + "\" does not exists in study " + studyId);
        }

        Bson and = Filters.and(Filters.eq(PRIVATE_ID, studyId), Filters.eq("groups.id", groupId));
        Bson addToSet = Updates.addToSet("groups.$.userIds", userId);
        QueryResult<UpdateResult> queryResult = studyCollection.update(and, addToSet, null);
        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to add member to group " + groupId);
        }

        return endQuery("addMemberToGroup", startTime, getGroup(studyId, null, groupId, null));
    }

    @Override
    public QueryResult<Group> removeMemberFromGroup(int studyId, String groupId, String userId) throws CatalogDBException {
        long startTime = startQuery();

        if (!groupExists(studyId, groupId)) {
            throw new CatalogDBException("Group \"" + groupId + "\" does not exists in study " + studyId);
        }
        Bson and = Filters.and(Filters.eq(PRIVATE_ID, studyId), Filters.eq("groups.id", groupId));
        Bson pull = Updates.pull("groups.$.userIds", userId);
        QueryResult<UpdateResult> update = studyCollection.update(and, pull, null);
        if (update.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to remove member to group " + groupId);
        }

        return endQuery("removeMemberFromGroup", startTime, getGroup(studyId, null, groupId, null));
    }


    /*
     * Variables Methods
     * ***************************
     */

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query(QueryParams.VARIABLE_SET_NAME.key(), variableSet.getName()).append(QueryParams.ID.key(), studyId);
        QueryResult<Long> count = count(query);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("VariableSet { name: '" + variableSet.getName() + "'} already exists.");
        }

        int variableSetId = getNewId();
        variableSet.setId(variableSetId);
        Document object = getMongoDBDocument(variableSet, "VariableSet");

        Bson bsonQuery = Filters.eq(PRIVATE_ID, studyId);
        Bson update = Updates.push("variableSets", object);
        QueryResult<UpdateResult> queryResult = studyCollection.update(bsonQuery, update, null);

        if (queryResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("createVariableSet: Could not create a new variable set in study " + studyId);
        }

        return endQuery("createVariableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query(QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        Bson projection = Projections.elemMatch("variableSets", Filters.eq("id", variableSetId));
        if (options == null) {
            options = new QueryOptions();
        }
        QueryOptions qOptions = new QueryOptions(options);
        qOptions.put(MongoDBCollection.ELEM_MATCH, projection);
        QueryResult<Study> studyQueryResult = get(query, qOptions);

        if (studyQueryResult.getResult().isEmpty() || studyQueryResult.first().getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist");
        }

/*
        Bson query = Filters.eq("variableSets.id", variableSetId);
        Bson projection = Projections.elemMatch("variableSets", Filters.eq("id", variableSetId));
        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_STUDIES);

        QueryResult<Document> queryResult = studyCollection.find(query, projection, filteredOptions);
        List<Study> studies = parseStudies(queryResult);
        if (studies.isEmpty() || studies.get(0).getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist");
        }
*/
        return endQuery("", startTime, studyQueryResult.first().getVariableSets());
    }

    //FIXME: getAllVariableSets method should receive Query and QueryOptions
    @Override
    public QueryResult<VariableSet> getAllVariableSets(int studyId, QueryOptions options) throws CatalogDBException {

        long startTime = startQuery();

        List<Document> mongoQueryList = new LinkedList<>();

        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                CatalogSampleDBAdaptor.VariableSetFilterOption option = CatalogSampleDBAdaptor.VariableSetFilterOption.valueOf(key);
                switch (option) {
                    case studyId:
                        addCompQueryFilter(option, option.name(), PRIVATE_ID, options, mongoQueryList);
                        break;
                    default:
                        String optionsKey = "variableSets." + entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), optionsKey, options, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        /*
        QueryResult<DBObject> queryResult = studyCollection.aggregate(Arrays.<DBObject>asList(
                new BasicDBObject("$match", new BasicDBObject(PRIVATE_ID, studyId)),
                new BasicDBObject("$project", new BasicDBObject("variableSets", 1)),
                new BasicDBObject("$unwind", "$variableSets"),
                new BasicDBObject("$match", new BasicDBObject("$and", mongoQueryList))
        ), filterOptions(options, FILTER_ROUTE_STUDIES));
*/

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, studyId)));
        aggregation.add(Projections.include("variableSets"));
        aggregation.add(Aggregates.unwind("$variableSets"));
        aggregation.add(Aggregates.match(new Document("$and", mongoQueryList)));

        QueryResult<Document> queryResult = studyCollection.aggregate(aggregation,
                filterOptions(options, FILTER_ROUTE_STUDIES));

        List<VariableSet> variableSets = parseObjects(queryResult, Study.class).stream().map(study -> study.getVariableSets().get(0))
                .collect(Collectors.toList());

        return endQuery("", startTime, variableSets);
    }

    @Override
    public QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkVariableSetInUse(variableSetId);
        int studyId = getStudyIdByVariableSetId(variableSetId);
        QueryResult<VariableSet> variableSet = getVariableSet(variableSetId, queryOptions);
        Bson query = Filters.eq(PRIVATE_ID, studyId);
        Bson operation = Updates.pull("variableSets", Filters.eq("id", variableSetId));
        QueryResult<UpdateResult> update = studyCollection.update(query, operation, null);

        if (update.first().getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
        return endQuery("Delete VariableSet", startTime, variableSet);
    }


    public void checkVariableSetInUse(int variableSetId) throws CatalogDBException {
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().getAllSamples(new QueryOptions(CatalogSampleDBAdaptor
                .SampleFilterOption.variableSetId.toString(), variableSetId));
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of samples : [";
            for (Sample sample : samples.getResult()) {
                msg += " { id: " + sample.getId() + ", name: \"" + sample.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Individual> individuals = dbAdaptorFactory.getCatalogIndividualDBAdaptor().getAllIndividuals(
                new QueryOptions(CatalogIndividualDBAdaptor.IndividualFilterOption.variableSetId.toString(), variableSetId));
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete VariableSetId, still in use as \"variableSetId\" of individuals : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
    }


    @Override
    public int getStudyIdByVariableSetId(int variableSetId) throws CatalogDBException {
//        DBObject query = new BasicDBObject("variableSets.id", variableSetId);
        Bson query = Filters.eq("variableSets.id", variableSetId);
        Bson projection = Projections.include("id");

//        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", true), null);
        QueryResult<Document> queryResult = studyCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
    }



    /*
    * Helper methods
    ********************/

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
            project.setStudies(getAllStudiesInProject(project.getId(), options).getResult());
        }
    }

    private void joinFields(Study study, QueryOptions options) throws CatalogDBException {
        int studyId = study.getId();
        if (studyId <= 0 || options == null) {
            return;
        }

        if (options.getBoolean("includeFiles")) {
            study.setFiles(dbAdaptorFactory.getCatalogFileDBAdaptor().getAllFilesInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeJobs")) {
            study.setJobs(dbAdaptorFactory.getCatalogJobDBAdaptor().getAllJobsInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeSamples")) {
            study.setSamples(dbAdaptorFactory.getCatalogSampleDBAdaptor().getAllSamplesInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeIndividuals")) {
            study.setIndividuals(dbAdaptorFactory.getCatalogIndividualDBAdaptor().getAllIndividualsInStudy(studyId, options).getResult());
        }
    }


    @Override
    public QueryResult<Long> count(Query query) {
        Bson bson = parseQuery(query);
        return studyCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        Bson bson = parseQuery(query);
        return studyCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Study> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        qOptions = filterOptions(qOptions, FILTER_ROUTE_STUDIES);
        QueryResult<Study> result = studyCollection.find(bson, studyConverter, qOptions);
        for (Study study : result.getResult()) {
            joinFields(study, options);
        }
        return endQuery("Get study", startTime, result.getResult());
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_STUDIES);
        // Fixme: If necessary, include in the results also the files, jobs, individuals...
        return studyCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        //FIXME: Check the commented code from modifyStudy
        /*
        long startTime = startQuery();

        checkStudyId(studyId);
//        BasicDBObject studyParameters = new BasicDBObject();
        Document studyParameters = new Document();

        String[] acceptedParams = {"name", "creationDate", "creationId", "description", "status", "lastActivity", "cipher"};
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, parameters, acceptedLongParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("type"), Study.Type.class);
        filterEnumParams(parameters, studyParameters, acceptedEnums);

        if (parameters.containsKey("uri")) {
            URI uri = parameters.get("uri", URI.class);
            studyParameters.put("uri", uri.toString());
        }

        if (!studyParameters.isEmpty()) {
//            BasicDBObject query = new BasicDBObject(PRIVATE_ID, studyId);
            Bson eq = Filters.eq(PRIVATE_ID, studyId);
            BasicDBObject updates = new BasicDBObject("$set", studyParameters);

//            QueryResult<WriteResult> updateResult = studyCollection.update(query, updates, null);
            QueryResult<UpdateResult> updateResult = studyCollection.update(eq, updates, null);
            if (updateResult.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Study", studyId);
            }
        }
        return endQuery("Modify study", startTime, getStudy(studyId, null));
        * */

        long startTime = startQuery();
        Document studyParameters = new Document();

        String[] acceptedParams = {"name", "creationDate", "creationId", "description", "status", "lastActivity", "cipher"};
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, studyParameters, acceptedLongParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

        //Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("type"), Study.Type.class);
        //filterEnumParams(parameters, studyParameters, acceptedEnums);

        if (parameters.containsKey("uri")) {
            URI uri = parameters.get("uri", URI.class);
            studyParameters.put("uri", uri.toString());
        }

        if (!studyParameters.isEmpty()) {
            Document updates = new Document("$set", studyParameters);
            Long nModified = studyCollection.update(parseQuery(query), updates, null).getNumTotalResults();
            return endQuery("Study update", startTime, Collections.singletonList(nModified));
        }

        return endQuery("Study update", startTime, Collections.singletonList(0L));
    }

    @Override
    public QueryResult<Study> update(int id, ObjectMap parameters) throws CatalogDBException {

        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters);
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update study with id " + id);
        }
        return endQuery("Update study", startTime, getStudy(id, null));

    }

    @Override
    public QueryResult<Study> delete(int id) throws CatalogDBException {
        //FIXME: Check the following commented code from deleteStudy
        /*
        * Query query = new Query(CatalogStudyDBAdaptor.QueryParams.ID.key(), studyId);
        QueryResult<Study> sampleQueryResult = get(query, new QueryOptions());
        if (sampleQueryResult.getResult().size() == 1) {
            QueryResult<Long> delete = delete(query);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Study id '{}' has not been deleted", studyId);
            }
        } else {
            throw CatalogDBException.newInstance("Study id '{}' does not exist", studyId);
        }
        return sampleQueryResult;
        * */
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Study> studyQueryResult = get(query, null);
        if (studyQueryResult.getResult().size() == 1) {
            QueryResult<Long> delete = delete(query);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Study id '{}' has not been deleted", id);
            }
        } else {
            throw CatalogDBException.idNotFound("Study id '{}' does not exist (or there are too many)", id);
        }
        return studyQueryResult;
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Long> delete(Query query) throws CatalogDBException {
        long startTime = startQuery();
        Bson bson = parseQuery(query);
        QueryResult<DeleteResult> remove = studyCollection.remove(bson, null);
        return endQuery("Delete study", startTime, Arrays.asList(remove.getNumTotalResults()));
    }


    @Override
    public CatalogDBIterator<Study> iterator(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = studyCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, studyConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = studyCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        Bson bsonQuery = parseQuery(query);
        return rank(studyCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(studyCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(studyCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) {
        Objects.requireNonNull(action);
        CatalogDBIterator<Study> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) {
        List<Bson> andBsonList = new ArrayList<>();

        // FIXME: Pedro. Check the mongodb names as well as integer createQueries

        addIntegerOrQuery(PRIVATE_ID, QueryParams.ID.key(), query, andBsonList);
        addIntegerOrQuery(PRIVATE_ID, PRIVATE_ID, query, andBsonList);
        addStringOrQuery(QueryParams.NAME.key(), QueryParams.NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.ALIAS.key(), QueryParams.ALIAS.key(), query, andBsonList);
        addStringOrQuery(QueryParams.CREATOR_ID.key(), QueryParams.CREATOR_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.STATUS.key(), QueryParams.STATUS.key(), query, andBsonList);
        addStringOrQuery(QueryParams.LAST_ACTIVITY.key(), QueryParams.LAST_ACTIVITY.key(), query,
                MongoDBQueryUtils.ComparisonOperator.NOT_EQUAL, andBsonList);
        addIntegerOrQuery(QueryParams.DISK_USAGE.key(), QueryParams.DISK_USAGE.key(), query, andBsonList);
        addIntegerOrQuery(PRIVATE_PROJECT_ID, QueryParams.PROJECT_ID.key(), query, andBsonList);
        addIntegerOrQuery(PRIVATE_PROJECT_ID, PRIVATE_PROJECT_ID, query, andBsonList);

        addStringOrQuery(QueryParams.GROUP_ID.key(), QueryParams.GROUP_ID.key(), query, andBsonList);

        addStringOrQuery(QueryParams.EXPERIMENT_ID.key(), QueryParams.EXPERIMENT_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_NAME.key(), QueryParams.EXPERIMENT_NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_TYPE.key(), QueryParams.EXPERIMENT_TYPE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_PLATFORM.key(), QueryParams.EXPERIMENT_PLATFORM.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_MANUFACTURER.key(), QueryParams.EXPERIMENT_MANUFACTURER.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_DATE.key(), QueryParams.EXPERIMENT_DATE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_LAB.key(), QueryParams.EXPERIMENT_LAB.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_CENTER.key(), QueryParams.EXPERIMENT_CENTER.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_RESPONSIBLE.key(), QueryParams.EXPERIMENT_RESPONSIBLE.key(), query, andBsonList);

        addIntegerOrQuery(QueryParams.FILE_ID.key(), QueryParams.FILE_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.FILE_NAME.key(), QueryParams.FILE_NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.FILE_TYPE.key(), QueryParams.FILE_TYPE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.FILE_FORMAT.key(), QueryParams.FILE_FORMAT.key(), query, andBsonList);
        addStringOrQuery(QueryParams.FILE_BIOFORMAT.key(), QueryParams.FILE_BIOFORMAT.key(), query, andBsonList);
        addIntegerOrQuery(QueryParams.FILE_DISK_USAGE.key(), QueryParams.FILE_DISK_USAGE.key(), query, andBsonList);

        addIntegerOrQuery(QueryParams.JOB_ID.key(), QueryParams.JOB_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.JOB_NAME.key(), QueryParams.JOB_NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.JOB_USER_ID.key(), QueryParams.JOB_USER_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.JOB_TOOL_NAME.key(), QueryParams.JOB_TOOL_NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.JOB_DATE.key(), QueryParams.JOB_DATE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.JOB_STATUS.key(), QueryParams.JOB_STATUS.key(), query, andBsonList);
        addStringOrQuery(QueryParams.JOB_DISK_USAGE.key(), QueryParams.JOB_DISK_USAGE.key(), query, andBsonList);

        addStringOrQuery(QueryParams.INDIVIDUAL_ID.key(), QueryParams.INDIVIDUAL_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.INDIVIDUAL_NAME.key(), QueryParams.INDIVIDUAL_NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.INDIVIDUAL_FATHER_ID.key(), QueryParams.INDIVIDUAL_FATHER_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.INDIVIDUAL_MOTHER_ID.key(), QueryParams.INDIVIDUAL_MOTHER_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.INDIVIDUAL_FAMILY.key(), QueryParams.INDIVIDUAL_FAMILY.key(), query, andBsonList);
        addStringOrQuery(QueryParams.INDIVIDUAL_RACE.key(), QueryParams.INDIVIDUAL_RACE.key(), query, andBsonList);

        addStringOrQuery(QueryParams.SAMPLE_ID.key(), QueryParams.SAMPLE_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.SAMPLE_NAME.key(), QueryParams.SAMPLE_NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.SAMPLE_SOURCE.key(), QueryParams.SAMPLE_SOURCE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.SAMPLE_INDIVIDUAL_ID.key(), QueryParams.SAMPLE_INDIVIDUAL_ID.key(), query, andBsonList);

        addStringOrQuery(QueryParams.DATASET_ID.key(), QueryParams.DATASET_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.DATASET_NAME.key(), QueryParams.DATASET_NAME.key(), query, andBsonList);

        addStringOrQuery(QueryParams.COHORT_ID.key(), QueryParams.COHORT_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.COHORT_NAME.key(), QueryParams.COHORT_NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.COHORT_TYPE.key(), QueryParams.COHORT_TYPE.key(), query, andBsonList);

        addIntegerOrQuery(QueryParams.VARIABLE_SET_ID.key(), QueryParams.VARIABLE_SET_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.VARIABLE_SET_DESCRIPTION.key(), QueryParams.VARIABLE_SET_DESCRIPTION.key(), query, andBsonList);
        addStringOrQuery(QueryParams.VARIABLE_SET_NAME.key(), QueryParams.VARIABLE_SET_NAME.key(), query, andBsonList);

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    public MongoDBCollection getStudyCollection() {
        return studyCollection;
    }

    /***
     * This method is called every time a file has been inserted, modified or deleted to keep track of the current study diskUsage.
     *
     * @param studyId   Study Identifier
     * @param diskUsage disk usage of a new created, updated or deleted file belonging to studyId. This argument
     *                  will be > 0 to increment the diskUsage field in the study collection or < 0 to decrement it.
     * @throws CatalogDBException An exception is launched when the update crashes.
     */
    public void updateDiskUsage(int studyId, long diskUsage) throws CatalogDBException {
        Bson query = new Document(QueryParams.ID.key(), studyId);
        Bson update = Updates.inc(QueryParams.DISK_USAGE.key(), diskUsage);
        if (studyCollection.update(query, update, null).getNumTotalResults() == 0) {
            throw new CatalogDBException("CatalogMongoStudyDBAdaptor updateDiskUsage: Couldn't update the diskUsage field of"
                    + " the study " + studyId);
        }
    }
}
