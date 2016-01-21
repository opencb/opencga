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
import com.mongodb.DBObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by hpccoll1 on 14/08/15.
 */
public class CatalogMongoSampleDBAdaptor extends CatalogMongoDBAdaptor implements CatalogSampleDBAdaptor {

    private final MongoDBCollection sampleCollection;
    private SampleConverter sampleConverter;

    public CatalogMongoSampleDBAdaptor(MongoDBCollection sampleCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoSampleDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.sampleCollection = sampleCollection;
        this.sampleConverter = new SampleConverter();
    }

    /**
     * Samples methods
     * ***************************
     */

    @Override
    public QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        /*
        QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("name", sample.getName()).append(_STUDY_ID, studyId));
                */
        Bson bson = Filters.and(Filters.eq("name", sample.getName()), Filters.eq(_STUDY_ID, studyId));
        QueryResult<Long> count = sampleCollection.count(bson);
//                new BsonDocument("name", new BsonString(sample.getName())).append(_STUDY_ID, new BsonInt32(studyId)));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getName() + "'} already exists.");
        }

        int sampleId = getNewId();
        sample.setId(sampleId);
        sample.setAnnotationSets(Collections.<AnnotationSet>emptyList());
        //TODO: Add annotationSets
        Document sampleObject = getMongoDBDocument(sample, "sample");
        sampleObject.put(_STUDY_ID, studyId);
        sampleObject.put(_ID, sampleId);
        sampleCollection.insert(sampleObject, null);

        return endQuery("createSample", startTime, getSample(sampleId, options));
    }


    @Override
    public QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);

//        DBObject query = new BasicDBObject(_ID, sampleId);
        Query query1 = new Query(QueryParams.ID.key(), sampleId);
        QueryResult<Sample> sampleQueryResult = get(query1, filteredOptions);

//        QueryResult<Document> queryResult = sampleCollection.find(bson, filteredOptions);
//        List<Sample> samples = parseSamples(queryResult);

        if (sampleQueryResult.getResult().size() == 0) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }

        return endQuery("getSample", startTime, sampleQueryResult);
    }

    @Override
    public QueryResult<Sample> getAllSamples(QueryOptions options) throws CatalogDBException {
        int variableSetId = options.getInt(SampleFilterOption.variableSetId.toString());
        Map<String, Variable> variableMap = null;
        if (variableSetId > 0) {
            variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                    .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
        }
        return getAllSamples(variableMap, options);
    }

    @Override
    public QueryResult<Sample> getAllSamples(Map<String, Variable> variableMap, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        String warning = "";

        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);

        List<DBObject> mongoQueryList = new LinkedList<>();
        List<DBObject> annotationSetFilter = new LinkedList<>();

        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                SampleFilterOption option = SampleFilterOption.valueOf(key);
                switch (option) {
                    case id:
                        addCompQueryFilter(option, option.name(), options, _ID, mongoQueryList);
                        break;
                    case studyId:
                        addCompQueryFilter(option, option.name(), options, _STUDY_ID, mongoQueryList);
                        break;
                    case annotationSetId:
                        addCompQueryFilter(option, option.name(), options, "id", annotationSetFilter);
                        break;
                    case variableSetId:
                        addCompQueryFilter(option, option.name(), options, option.getKey(), annotationSetFilter);
                        break;
                    case annotation:
                        addAnnotationQueryFilter(option.name(), options, annotationSetFilter, variableMap);
                        break;
                    default:
                        String optionsKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), options, optionsKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        Document query = new Document();
        if (!annotationSetFilter.isEmpty()) {
            query.put("annotationSets", new BasicDBObject("$elemMatch", new BasicDBObject("$and", annotationSetFilter)));//            query
        }
        if (!mongoQueryList.isEmpty()) {
            query.put("$and", mongoQueryList);
        }
        logger.debug("GetAllSamples query: {}", query);
//        QueryResult<DBObject> queryResult = sampleCollection.find(query, filteredOptions);


        QueryResult<Document> queryResult = sampleCollection.find(query, filteredOptions);
        List<Sample> samples = parseSamples(queryResult);

        QueryResult<Sample> result = endQuery("getAllSamples", startTime, samples, null, warning.isEmpty() ? null : warning);
        result.setNumTotalResults(queryResult.getNumTotalResults());
        return result;
    }

    @Override
    public QueryResult<Sample> getAllSamplesInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(_STUDY_ID, studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> sampleParams = new HashMap<>();
        //List<Bson> sampleParams = new ArrayList<>();

        String[] acceptedParams = {"source", "description"};
        filterStringParams(parameters, sampleParams, acceptedParams);

        String[] acceptedIntParams = {"individualId"};
        filterIntParams(parameters, sampleParams, acceptedIntParams);

        String[] acceptedMapParams = {"attributes"};
        filterMapParams(parameters, sampleParams, acceptedMapParams);

        if (sampleParams.containsKey("individualId")) {
            if (!dbAdaptorFactory.getCatalogIndividualDBAdaptor().individualExists(parameters.getInt("individualId"))) {
                throw CatalogDBException.idNotFound("Individual", parameters.getInt("individualId"));
            }
        }

        if (!sampleParams.isEmpty()) {
            /*QueryResult<WriteResult> update = sampleCollection.update(new BasicDBObject(_ID, sampleId),
                    new BasicDBObject("$set", sampleParams), null);
                    */
            Bson query = Filters.eq(_ID, sampleId);
            Bson operation = new Document("$set", sampleParams);
            QueryResult<UpdateResult> update = sampleCollection.update(query, operation, null);

            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount()== 0) {
                throw CatalogDBException.idNotFound("Sample", sampleId);
            }
        }

        return endQuery("Modify cohort", startTime, getSample(sampleId, parameters));
    }

    public QueryResult<AclEntry> getSampleAcl(int sampleId, String userId) throws CatalogDBException {
        long startTime = startQuery();

        int studyId = getStudyIdBySampleId(sampleId);
        checkAclUserId(dbAdaptorFactory, userId, studyId);

//        DBObject query = new BasicDBObject(_ID, sampleId);
        Bson eq = Filters.eq(_ID, sampleId);

//        DBObject projection = new BasicDBObject("acl", new BasicDBObject("$elemMatch", new BasicDBObject("userId", userId)));
        Bson projection = Projections.elemMatch("acl", Filters.eq("userId", userId));

        QueryResult<Document> queryResult = sampleCollection.find(eq, projection, null);
        Sample sample = parseObject(queryResult, Sample.class);

        if (queryResult.getNumResults() == 0 || sample == null) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }

        return endQuery("get file acl", startTime, sample.getAcl());
    }

    @Override
    public QueryResult<Map<String, AclEntry>> getSampleAcl(int sampleId, List<String> userIds) throws CatalogDBException {

        long startTime = startQuery();
        /*DBObject match = new BasicDBObject("$match", new BasicDBObject(_ID, sampleId));
        DBObject unwind = new BasicDBObject("$unwind", "$acl");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("acl.userId", new BasicDBObject("$in", userIds)));
        DBObject project = new BasicDBObject("$project", new BasicDBObject("id", 1).append("acl", 1));
*/
        Bson match = Aggregates.match(Filters.eq("_ID", sampleId));
        Bson unwind = Aggregates.unwind("$acl");
        Bson match2 = Aggregates.match(Filters.in("acl.userId", userIds));
//        Bson project = Projections.include("id", "acl");
        Bson project = Aggregates.project(Projections.include("id", "acl"));

        QueryResult<Document> aggregate = sampleCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        List<Sample> sampleList = parseSamples(aggregate);

        Map<String, AclEntry> userAclMap = sampleList.stream().map(s -> s.getAcl().get(0))
                .collect(Collectors.toMap(AclEntry::getUserId, s -> s));

        return endQuery("getSampleAcl", startTime, Collections.singletonList(userAclMap));
    }

    @Override
    public QueryResult<AclEntry> setSampleAcl(int sampleId, AclEntry acl) throws CatalogDBException {
        long startTime = startQuery();

        String userId = acl.getUserId();
        /*DBObject query;
        DBObject newAclObject = getDbObject(acl, "ACL");
        DBObject update;
*/
        Bson query;
        Document newAclObject = getMongoDBDocument(acl, "ACL");
        Bson update;

        /*
        List<AclEntry> aclList = getSampleAcl(sampleId, userId).getResult();
        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
            query = new BasicDBObject(_ID, sampleId);
            update = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            query = BasicDBObjectBuilder
                    .start(_ID, sampleId)
                    .append("acl.userId", userId).get();
            update = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
        }
        */
        List<AclEntry> aclList = getSampleAcl(sampleId, userId).getResult();
        if (aclList.isEmpty()) { // there is no acl for that user in that file. Push
            query = new BsonDocument(_ID, new BsonInt32(sampleId));
            update = Updates.push("acl", newAclObject);
        } else {
            query = new BsonDocument(_ID, new BsonInt32(sampleId)).append("acl.userId", new BsonString(userId));
            update = Updates.set("acl.$", newAclObject);
        }

        QueryResult<UpdateResult> queryResult = sampleCollection.update(query, update, null);
        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }

        return endQuery("setSampleAcl", startTime, getSampleAcl(sampleId, userId));
    }

    @Override
    public QueryResult<AclEntry> unsetSampleAcl(int sampleId, String userId) throws CatalogDBException {

        long startTime = startQuery();
/*
        QueryResult<AclEntry> sampleAcl = getSampleAcl(sampleId, userId);
        DBObject query = new BasicDBObject(_ID, sampleId);
        ;
        DBObject update = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", userId)));

        QueryResult queryResult = sampleCollection.update(query, update, null);
*/
        QueryResult<AclEntry> sampleAcl = getSampleAcl(sampleId, userId);

        if (!sampleAcl.getResult().isEmpty()) {
            Bson query = new BsonDocument(_ID, new BsonInt32(sampleId));
            Bson update = Updates.pull("acl", new BsonDocument("userId", new BsonString(userId)));
            sampleCollection.update(query, update, null);
        }

        return endQuery("unsetSampleAcl", startTime, sampleAcl);

    }

//    @Override
//    public QueryResult<Sample> deleteSample(int sampleId) throws CatalogDBException {
//        Query query = new Query(CatalogStudyDBAdaptor.QueryParams.ID.key(), sampleId);
//        QueryResult<Sample> sampleQueryResult = get(query, new QueryOptions());
//        if (sampleQueryResult.getResult().size() == 1) {
//            QueryResult<Long> delete = delete(query);
//            if (delete.getResult().size() == 0) {
//                throw CatalogDBException.newInstance("Sample id '{}' has not been deleted", sampleId);
//            }
//        } else {
//            throw CatalogDBException.newInstance("Sample id '{}' does not exist", sampleId);
//        }
//        return sampleQueryResult;
//    }


//    @Override
//    public QueryResult<Sample> deleteSample(int sampleId) throws CatalogDBException {
//        long startTime = startQuery();
//
//        QueryResult<Sample> sampleQueryResult = getSample(sampleId, null);
//
//        checkInUse(sampleId);
//        DeleteResult id = sampleCollection.remove(new BasicDBObject(_ID, sampleId), null).getResult().get(0);
//        if (id.getDeletedCount() == 0) {
//            throw CatalogDBException.idNotFound("Sample", sampleId);
//        } else {
//            return endQuery("delete sample", startTime, sampleQueryResult);
//        }
//    }


//    public void checkInUse(int sampleId) throws CatalogDBException {
//        int studyId = getStudyIdBySampleId(sampleId);
//
//        QueryOptions query = new QueryOptions(FileFilterOption.sampleIds.toString(), sampleId);
//        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.path"));
//        QueryResult<File> fileQueryResult = dbAdaptorFactory.getCatalogFileDBAdaptor().getAllFiles(query, queryOptions);
//        if (fileQueryResult.getNumResults() != 0) {
//            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : " +
//                    fileQueryResult.getResult().stream()
//                            .map(file -> "{ id: " + file.getId() + ", path: \"" + file.getPath() + "\" }")
//                            .collect(Collectors.joining(", ", "[", "]"));
//            throw new CatalogDBException(msg);
//        }
//
//
//        queryOptions = new QueryOptions(CohortFilterOption.samples.toString(), sampleId)
//                .append("include", Arrays.asList("projects.studies.cohorts.id", "projects.studies.cohorts.name"));
//        QueryResult<Cohort> cohortQueryResult = getAllCohorts(studyId, queryOptions);
//        if (cohortQueryResult.getNumResults() != 0) {
//            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : " +
//                    cohortQueryResult.getResult().stream()
//                            .map(cohort -> "{ id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" }")
//                            .collect(Collectors.joining(", ", "[", "]"));
//            throw new CatalogDBException(msg);
//        }
//
//    }


    public int getStudyIdBySampleId(int sampleId) throws CatalogDBException {
        /*DBObject query = new BasicDBObject(_ID, sampleId);
        BasicDBObject projection = new BasicDBObject(_STUDY_ID, true);
        QueryResult<DBObject> queryResult = sampleCollection.find(query, projection, null);*/
        Bson query = new BsonDocument(_ID, new BsonInt32(sampleId));
        Bson projection = Projections.include(_STUDY_ID);
        QueryResult<Document> queryResult = sampleCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).intValue() : (int) Double.parseDouble(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }
    }

    /*
     * Cohorts methods
     * ***************************
     */

    @Override
    public QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        checkCohortNameExists(studyId, cohort.getName());

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

//        int newId = getNewAutoIncrementId(metaCollection);
        int newId = dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();

        cohort.setId(newId);

        Document cohortObject = getMongoDBDocument(cohort, "Cohort");
        QueryResult<UpdateResult> update = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection().update(
                new BasicDBObject(_ID, studyId).append("cohorts.name", new BasicDBObject("$ne", cohort.getName())),
                new BasicDBObject("$push", new BasicDBObject("cohorts", cohortObject)), null);

        if (update.getResult().get(0).getModifiedCount() == 0) {
            throw CatalogDBException.alreadyExists("Cohort", "name", cohort.getName());
        }

        return endQuery("createCohort", startTime, getCohort(newId));
    }

    private void checkCohortNameExists(int studyId, String cohortName) throws CatalogDBException {
//        QueryResult<Long> count = studyCollection.count(BasicDBObjectBuilder
//                .start(_ID, studyId)
//                .append("cohorts.name", cohortName)
//                .get());
        QueryResult<Long> count = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                .count(Filters.and(Filters.eq(_ID, studyId), Filters.eq("cohorts.name", cohortName)));

        if (count.getResult().get(0) > 0) {
            throw CatalogDBException.alreadyExists("Cohort", "name", cohortName);
        }
    }

    @Override
    public QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject("cohorts.id", cohortId);
//        BasicDBObject projection = new BasicDBObject("cohorts", new BasicDBObject("$elemMatch", new BasicDBObject("id", cohortId)));
//        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, null);

        Bson query = Filters.eq("cohorts.id", cohortId);
        Bson projection = Projections.elemMatch("cohorts", Filters.eq("id", cohortId));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                .find(query, projection, new QueryOptions());

        List<Study> studies = parseStudies(queryResult);
        if (studies == null || studies.get(0).getCohorts().isEmpty()) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            return endQuery("getCohort", startTime, studies.get(0).getCohorts());
        }
    }

    @Override
    public QueryResult<Cohort> getAllCohorts(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();
        options.put(CohortFilterOption.studyId.toString(), studyId);
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                CohortFilterOption option = CohortFilterOption.valueOf(key);
                switch (option) {
                    case studyId:
                        addCompQueryFilter(option, option.name(), options, _ID, mongoQueryList);
                        break;
                    default:
                        String optionsKey = "cohorts." + entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), options, optionsKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        // TODO change to MongoDB 3.x
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                .aggregate(Arrays.<Bson>asList(
                        new BasicDBObject("$match", new BasicDBObject(_ID, studyId)),
                        new BasicDBObject("$project", new BasicDBObject("cohorts", 1)),
                        new BasicDBObject("$unwind", "$cohorts"),
                        new BasicDBObject("$match", new BasicDBObject("$and", mongoQueryList))
                ), filterOptions(options, FILTER_ROUTE_STUDIES));

        List<Cohort> cohorts = parseObjects(queryResult, Study.class).stream().map((study) -> study.getCohorts().get(0)).collect
                (Collectors.toList());

        return endQuery("getAllCohorts", startTime, cohorts);
    }

    @Override
    public QueryResult<Cohort> modifyCohort(int cohortId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> cohortParams = new HashMap<>();

        String[] acceptedParams = {"description", "name", "creationDate"};
        filterStringParams(parameters, cohortParams, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap("type", Cohort.Type.class);
        filterEnumParams(parameters, cohortParams, acceptedEnums);

        String[] acceptedIntegerListParams = {"samples"};
        filterIntegerListParams(parameters, cohortParams, acceptedIntegerListParams);
        if (parameters.containsKey("samples")) {
            for (Integer sampleId : parameters.getAsIntegerList("samples")) {
                if (!sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, cohortParams, acceptedMapParams);

        Map<String, Class<? extends Enum>> acceptedEnumParams = Collections.singletonMap("status", Cohort.Status.class);
        filterEnumParams(parameters, cohortParams, acceptedEnumParams);

        if (!cohortParams.isEmpty()) {
            HashMap<Object, Object> studyRelativeCohortParameters = new HashMap<>();
            for (Map.Entry<String, Object> entry : cohortParams.entrySet()) {
                studyRelativeCohortParameters.put("cohorts.$." + entry.getKey(), entry.getValue());
            }
            QueryResult<UpdateResult> update = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                    .update(new BasicDBObject("cohorts.id", cohortId), new BasicDBObject("$set", studyRelativeCohortParameters), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Cohort", cohortId);
            }
        }

        return endQuery("Modify cohort", startTime, getCohort(cohortId));
    }

    @Override
    public QueryResult<Cohort> deleteCohort(int cohortId, ObjectMap queryOptions) throws CatalogDBException {
        long startTime = startQuery();

//        checkCohortInUse(cohortId);
        int studyId = getStudyIdByCohortId(cohortId);
        QueryResult<Cohort> cohort = getCohort(cohortId);

//        QueryResult<WriteResult> update = studyCollection.update(new BasicDBObject(_ID, studyId), new BasicDBObject("$pull", new
//                BasicDBObject("cohorts", new BasicDBObject("id", cohortId))), null);
        QueryResult<UpdateResult> update = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                .update(Filters.eq(_ID, studyId), Updates.pull("cohorts", new Document("id", cohortId)), null);
        if (update.first().getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        }

        return endQuery("Delete Cohort", startTime, cohort);
    }

    @Override
    public int getStudyIdByCohortId(int cohortId) throws CatalogDBException {
//        BasicDBObject query = new BasicDBObject("cohorts.id", cohortId);
//        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", true), null);

        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor()
                .nativeGet(new Query("cohorts.id", cohortId), new QueryOptions("include", "id"));

        if (queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsKey("id")) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        }
    }


    /*
     * Annotations Methods
     * ***************************
     */

    @Override
    public QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet, boolean overwrite) throws
            CatalogDBException {
        long startTime = startQuery();

        /*QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("annotationSets.id", annotationSet.getId()).append(_ID, sampleId));*/
        QueryResult<Long> count = sampleCollection.count(new BsonDocument("annotationSets.id", new BsonString(annotationSet.getId()))
                        .append(_ID, new BsonInt32(sampleId)));
        if (overwrite) {
            if (count.getResult().get(0) == 0) {
                throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getId());
            }
        } else {
            if (count.getResult().get(0) > 0) {
                throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
            }
        }

        /*DBObject object = getDbObject(annotationSet, "AnnotationSet");

        DBObject query = new BasicDBObject(_ID, sampleId);*/
        Document object = getMongoDBDocument(annotationSet, "AnnotationSet");

        Bson query = new Document(_ID, sampleId);
        if (overwrite) {
            ((Document) query).put("annotationSets.id", annotationSet.getId());
        } else {
            ((Document) query).put("annotationSets.id", new BasicDBObject("$ne", annotationSet.getId()));
        }

        /*
        DBObject update;
        if (overwrite) {
            update = new BasicDBObject("$set", new BasicDBObject("annotationSets.$", object));
        } else {
            update = new BasicDBObject("$push", new BasicDBObject("annotationSets", object));
        }
*/

        Bson update;
        if (overwrite) {
            update = Updates.set("annotationSets.$", object);
        } else {
            update = Updates.push("annotationSets", object);
        }

        QueryResult<UpdateResult> queryResult = sampleCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
        }

        return endQuery("", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(int sampleId, String annotationId) throws CatalogDBException {

        long startTime = startQuery();

        Sample sample = getSample(sampleId, new QueryOptions("include", "projects.studies.samples.annotationSets")).first();
        AnnotationSet annotationSet = null;
        for (AnnotationSet as : sample.getAnnotationSets()) {
            if (as.getId().equals(annotationId)) {
                annotationSet = as;
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        /*
        DBObject query = new BasicDBObject(_ID, sampleId);
        DBObject update = new BasicDBObject("$pull", new BasicDBObject("annotationSets", new BasicDBObject("id", annotationId)));
        */
        Bson query = new BsonDocument(_ID, new BsonInt32(sampleId));
        Bson update = Updates.pull("annotationSets", new BsonDocument("id", new BsonString(annotationId)));
        QueryResult<UpdateResult> resultQueryResult = sampleCollection.update(query, update, null);
        if (resultQueryResult.first().getModifiedCount() < 1) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        return endQuery("Delete annotation", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<Long> count(Query query) {
        Bson bson = parseQuery(query);
        return sampleCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        Bson bson = parseQuery(query);
        return sampleCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Sample> get(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        options = filterOptions(options, FILTER_ROUTE_SAMPLES);
        return sampleCollection.find(bson, Projections.exclude(_ID, _STUDY_ID), sampleConverter, options);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        return sampleCollection.find(bson, options);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) { return null; }

    @Override
    public QueryResult<Sample> update(int id, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Sample> delete(int id) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> delete(Query query) throws CatalogDBException {
        long startTime = startQuery();

//        QueryResult<Sample> sampleQueryResult = getSample(sampleId, null);
//        checkInUse(sampleId);
        Bson bson = parseQuery(query);
        DeleteResult deleteResult = sampleCollection.remove(bson, null).getResult().get(0);
        if (deleteResult.getDeletedCount() == 0) {
            throw CatalogDBException.newInstance("Sample id '{}' not found", query.get(CatalogSampleDBAdaptor.QueryParams.ID.key()));
//            throw CatalogDBException.idNotFound("Sample", query.get(CatalogUserDBAdaptor.QueryParams.ID.key()));
        } else {
            return endQuery("delete sample", startTime, Collections.singletonList(deleteResult.getDeletedCount()));
        }
    }

    @Override
    public Iterator<Sample> iterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public Iterator nativeIterator(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        return sampleCollection.nativeQuery().find(bson, options).iterator();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) {

    }

    private Bson parseQuery(Query query) {
        List<Bson> andBsonList = new ArrayList<>();

        addIntegerOrQuery(_ID, QueryParams.ID.key(), query, andBsonList);
        addStringOrQuery("name", QueryParams.NAME.key(), query, andBsonList);
        addStringOrQuery("source", QueryParams.SOURCE.key(), query, andBsonList);
        addIntegerOrQuery("individualId", QueryParams.INDIVIDUAL_ID.key(), query, andBsonList);

        addIntegerOrQuery("studyId", QueryParams.STUDY_ID.key(), query, andBsonList);

        addStringOrQuery("acl.userId", QueryParams.ACL_USER_ID.key(), query, andBsonList);
        // FIXME: Add boolean queries. ACL_READ, ACL_WRITE...

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
