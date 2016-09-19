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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.GenericConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;
import static org.opencb.opencga.catalog.utils.CatalogMemberValidator.checkMembers;

/**
 * Created by hpccoll1 on 14/08/15.
 */
public class SampleMongoDBAdaptor extends AnnotationMongoDBAdaptor implements SampleDBAdaptor {

    private final MongoDBCollection sampleCollection;
    private SampleConverter sampleConverter;
    private AclMongoDBAdaptor<SampleAclEntry> aclDBAdaptor;

    public SampleMongoDBAdaptor(MongoDBCollection sampleCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(SampleMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.sampleCollection = sampleCollection;
        this.sampleConverter = new SampleConverter();
        this.aclDBAdaptor = new AclMongoDBAdaptor<>(sampleCollection, sampleConverter, logger);
    }

    @Override
    protected GenericConverter<? extends Annotable, Document> getConverter() {
        return sampleConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return sampleCollection;
    }

    /*
     * Samples methods
     * ***************************
     */

    @Override
    public QueryResult<Sample> insert(Sample sample, long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        /*
        QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("name", sample.getName()).append(PRIVATE_STUDY_ID, studyId));
                */
        Bson bson = Filters.and(Filters.eq("name", sample.getName()), Filters.eq(PRIVATE_STUDY_ID, studyId));
        QueryResult<Long> count = sampleCollection.count(bson);
//                new BsonDocument("name", new BsonString(sample.getName())).append(PRIVATE_STUDY_ID, new BsonInt32(studyId)));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getName() + "'} already exists.");
        }

        long sampleId = getNewId();
        sample.setId(sampleId);
        sample.setAnnotationSets(Collections.<AnnotationSet>emptyList());
        //TODO: Add annotationSets
        Document sampleObject = sampleConverter.convertToStorageType(sample);
        sampleObject.put(PRIVATE_STUDY_ID, studyId);
        sampleObject.put(PRIVATE_ID, sampleId);
        sampleCollection.insert(sampleObject, null);

        return endQuery("createSample", startTime, get(sampleId, options));
    }


    @Override
    public QueryResult<Sample> get(long sampleId, QueryOptions options) throws CatalogDBException {
        checkId(sampleId);
        return get(new Query(QueryParams.ID.key(), sampleId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED), options);
//        long startTime = startQuery();
//        //QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);
//
////        DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);
//        Query query1 = new Query(QueryParams.ID.key(), sampleId);
//        QueryResult<Sample> sampleQueryResult = get(query1, options);
//
////        QueryResult<Document> queryResult = sampleCollection.find(bson, filteredOptions);
////        List<Sample> samples = parseSamples(queryResult);
//
//        if (sampleQueryResult.getResult().size() == 0) {
//            throw CatalogDBException.idNotFound("Sample", sampleId);
//        }
//
//        return endQuery("getSample", startTime, sampleQueryResult);
    }

//    @Override
//    public QueryResult<Sample> getAllSamples(QueryOptions options) throws CatalogDBException {
//        throw new UnsupportedOperationException("Deprecated method. Use get instead");
//        /*
//        int variableSetId = options.getInt(SampleFilterOption.variableSetId.toString());
//        Map<String, Variable> variableMap = null;
//        if (variableSetId > 0) {
//            variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
//                    .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
//        }
//        return getAllSamples(variableMap, options);*/
//    }

    @Override
    public QueryResult<Sample> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<Sample> update(long sampleId, QueryOptions parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> sampleParams = new HashMap<>();
        //List<Bson> sampleParams = new ArrayList<>();

        String[] acceptedParams = {"source", "description", "name"};
        filterStringParams(parameters, sampleParams, acceptedParams);

        if (sampleParams.containsKey("name")) {
            // Check that the new sample name is still unique
            long studyId = getStudyId(sampleId);

            QueryResult<Long> count = sampleCollection.count(
                    new Document("name", sampleParams.get("name")).append(PRIVATE_STUDY_ID, studyId));
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Sample { name: '" + sampleParams.get("name") + "'} already exists.");
            }
        }

        String[] acceptedIntParams = {"individualId"};
        filterIntParams(parameters, sampleParams, acceptedIntParams);

        String[] acceptedMapParams = {"attributes"};
        filterMapParams(parameters, sampleParams, acceptedMapParams);

        if (sampleParams.containsKey("individualId")) {
            int individualId = parameters.getInt("individualId");
            if (individualId > 0 && !dbAdaptorFactory.getCatalogIndividualDBAdaptor().exists(individualId)) {
                throw CatalogDBException.idNotFound("Individual", individualId);
            }
        }

        if (!sampleParams.isEmpty()) {
            /*QueryResult<WriteResult> update = sampleCollection.update(new BasicDBObject(PRIVATE_ID, sampleId),
                    new BasicDBObject("$set", sampleParams), null);
                    */
            Bson query = Filters.eq(PRIVATE_ID, sampleId);
            Bson operation = new Document("$set", sampleParams);
            QueryResult<UpdateResult> update = sampleCollection.update(query, operation, null);

            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Sample", sampleId);
            }
        }

        return endQuery("Modify sample", startTime, get(sampleId, parameters));
    }

    public void unsetSampleAcl(long sampleId, List<String> members, List<String> permissions) throws CatalogDBException {
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, getStudyId(sampleId), members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_ID, sampleId).append(QueryParams.ACL_MEMBER.key(), member);
            Bson update;
            if (permissions.size() == 0) {
                update = new Document("$pull", new Document("acl", new Document("member", member)));
            } else {
                update = new Document("$pull", new Document("acl.$.permissions", new Document("$in", permissions)));
            }
            QueryResult<UpdateResult> updateResult = sampleCollection.update(query, update, null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("unsetSampleAcl: An error occurred when trying to stop sharing sample " + sampleId
                        + " with other " + member + ".");
            }
        }

        // Remove possible SampleAcls that might have permissions defined but no users
//        Bson queryBson = new Document(QueryParams.ID.key(), sampleId)
//                .append(QueryParams.ACL_MEMBER.key(),
//                        new Document("$exists", true).append("$eq", Collections.emptyList()));
//        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
//        sampleCollection.update(queryBson, update, null);
    }

    @Override
    public long getStudyId(long sampleId) throws CatalogDBException {
        /*DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);
        BasicDBObject projection = new BasicDBObject(PRIVATE_STUDY_ID, true);
        QueryResult<DBObject> queryResult = sampleCollection.find(query, projection, null);*/
        Bson query = new Document(PRIVATE_ID, sampleId);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = sampleCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }
    }

    @Override
    public List<Long> getStudyIdsBySampleIds(String sampleIds) throws CatalogDBException {
        Bson query = parseQuery(new Query(QueryParams.ID.key(), sampleIds));
        return sampleCollection.distinct(PRIVATE_STUDY_ID, query, Long.class).getResult();
    }

    /*
     * Annotations Methods
     * ***************************
     */

    @Override
    @Deprecated
    public QueryResult<AnnotationSet> annotate(long sampleId, AnnotationSet annotationSet, boolean overwrite) throws
            CatalogDBException {
        long startTime = startQuery();

        /*QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("annotationSets.id", annotationSet.getId()).append(PRIVATE_ID, sampleId));*/
        QueryResult<Long> count = sampleCollection.count(new Document("annotationSets.name", annotationSet.getName())
                .append(PRIVATE_ID, sampleId));
        if (overwrite) {
            if (count.getResult().get(0) == 0) {
                throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getName());
            }
        } else {
            if (count.getResult().get(0) > 0) {
                throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
            }
        }

        /*DBObject object = getDbObject(annotationSet, "AnnotationSet");

        DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);*/
        Document object = getMongoDBDocument(annotationSet, "AnnotationSet");

        Bson query = new Document(PRIVATE_ID, sampleId);
        if (overwrite) {
            ((Document) query).put("annotationSets.name", annotationSet.getName());
        } else {
            ((Document) query).put("annotationSets.name", new Document("$ne", annotationSet.getName()));
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
            throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
        }

        return endQuery("", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    @Deprecated
    public QueryResult<AnnotationSet> deleteAnnotation(long sampleId, String annotationId) throws CatalogDBException {

        long startTime = startQuery();

        Sample sample = get(sampleId, new QueryOptions("include", "projects.studies.samples.annotationSets")).first();
        AnnotationSet annotationSet = null;
        for (AnnotationSet as : sample.getAnnotationSets()) {
            if (as.getName().equals(annotationId)) {
                annotationSet = as;
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        /*
        DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);
        DBObject update = new BasicDBObject("$pull", new BasicDBObject("annotationSets", new BasicDBObject("id", annotationId)));
        */
        Bson query = new Document(PRIVATE_ID, sampleId);
        Bson update = Updates.pull("annotationSets", new Document("name", annotationId));
        QueryResult<UpdateResult> resultQueryResult = sampleCollection.update(query, update, null);
        if (resultQueryResult.first().getModifiedCount() < 1) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        return endQuery("Delete annotation", startTime, Collections.singletonList(annotationSet));
    }

    @Deprecated
    public void checkInUse(long sampleId) throws CatalogDBException {
        long studyId = getStudyId(sampleId);

        Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_FILES + FileDBAdaptor
                .QueryParams.ID.key(), FILTER_ROUTE_FILES + FileDBAdaptor.QueryParams.PATH.key()));
        QueryResult<File> fileQueryResult = dbAdaptorFactory.getCatalogFileDBAdaptor().get(query, queryOptions);
        if (fileQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : "
                    + fileQueryResult.getResult().stream()
                            .map(file -> "{ id: " + file.getId() + ", path: \"" + file.getPath() + "\" }")
                            .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }


        queryOptions = new QueryOptions(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId)
                .append(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.ID.key(),
                        FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.NAME.key()));
        QueryResult<Cohort> cohortQueryResult = dbAdaptorFactory.getCatalogCohortDBAdaptor().getAllInStudy(studyId, queryOptions);
        if (cohortQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : "
                    + cohortQueryResult.getResult().stream()
                            .map(cohort -> "{ id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" }")
                            .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }
    }

    /**
     * To be able to delete a sample, the sample does not have to be part of any cohort.
     *
     * @param sampleId sample id.
     * @throws CatalogDBException if the sampleId is used on any cohort.
     */
    private void checkCanDelete(long sampleId) throws CatalogDBException {
        Query query = new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
        if (dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).first() > 0) {
            List<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor()
                    .get(query, new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key())).getResult();
            throw new CatalogDBException("The sample {" + sampleId + "} cannot be deleted/removed. It is being used in "
                    + cohorts.size() + " cohorts: [" + cohorts.stream().map(Cohort::getId).collect(Collectors.toList()).toString() + "]");
        }
    }


    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return sampleCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return sampleCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Sample> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);
        QueryResult<Sample> sampleQueryResult = sampleCollection.find(bson, sampleConverter, qOptions);
        logger.debug("Sample get: query : {}, dbTime: {}", bson, qOptions == null ? "" : qOptions.toJson(),
                sampleQueryResult.getDbTime());
        return endQuery("Get sample", startTime, sampleQueryResult);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        //qOptions.append(MongoDBCollection.EXCLUDE, Arrays.asList(PRIVATE_ID, PRIVATE_STUDY_ID));
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);
        return sampleCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> sampleParameters = new HashMap<>();

        final String[] acceptedParams = {QueryParams.SOURCE.key(), QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, sampleParameters, acceptedParams);

        final String[] acceptedIntParams = {QueryParams.ID.key(), QueryParams.INDIVIDUAL_ID.key()};
        filterIntParams(parameters, sampleParameters, acceptedIntParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, sampleParameters, acceptedMapParams);

        if (parameters.containsKey(QueryParams.NAME.key())) {
            // That can only be done to one sample...
            QueryResult<Sample> sampleQueryResult = get(query, new QueryOptions());
            if (sampleQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update sample: No sample found to be updated");
            }
            if (sampleQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update sample: Cannot update name parameter. More than one sample found to be updated.");
            }

            // Check that the new sample name is still unique
            long studyId = getStudyId(sampleQueryResult.first().getId());

            Query tmpQuery = new Query()
                    .append(QueryParams.NAME.key(), parameters.get(QueryParams.NAME.key()))
                    .append(QueryParams.STUDY_ID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Sample { name: '" + parameters.get(QueryParams.NAME.key()) + "'} already exists.");
            }

            sampleParameters.put(QueryParams.NAME.key(), parameters.get(QueryParams.NAME.key()));
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            sampleParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            sampleParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!sampleParameters.isEmpty()) {
            QueryResult<UpdateResult> update = sampleCollection.update(parseQuery(query),
                    new Document("$set", sampleParameters), null);
            return endQuery("Update sample", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update sample", startTime, new QueryResult<>());
    }

    @Override
    public QueryResult<Sample> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters);
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update sample with id " + id);
        }
        Query query = new Query()
                .append(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), "!=EMPTY");
        return endQuery("Update sample", startTime, get(query, new QueryOptions()));
    }

    // TODO: Check clean
    public QueryResult<Sample> clean(long id) throws CatalogDBException {
        throw new UnsupportedOperationException("Clean is not yet implemented.");
    }

    @Override
    public QueryResult<Sample> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check the sample is active
        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.READY);
        QueryOptions tmpOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.NAME.key());
        QueryResult<Sample> sampleQueryResult = get(query, tmpOptions);
        if (sampleQueryResult.getNumResults() == 0) {
            query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_NAME.key());
            Sample sample = get(query, options).first();
            throw new CatalogDBException("The sample " + id + " was already " + sample.getStatus().getName());
        }

        // If we don't find the force parameter, we check first if the sample could be deleted.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            deleteReferencesToSample(id);
        }

        // Update the sample
        String suffixName = ".DELETED_" + TimeUtils.getTime();
        ObjectMap objectMap = new ObjectMap()
                .append(QueryParams.NAME.key(), sampleQueryResult.first().getName() + suffixName)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        update(id, objectMap);

        query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return endQuery("Delete sample", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_NAME.key(), Status.READY);
        QueryResult<Sample> sampleQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Sample sample : sampleQueryResult.getResult()) {
            delete(sample.getId(), queryOptions);
        }
        return endQuery("Delete sample", startTime, Collections.singletonList(sampleQueryResult.getNumTotalResults()));
    }

    @Override
    public QueryResult<Sample> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore samples", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Sample> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The sample {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore sample", startTime, get(query, null));
    }

    /***
     * Checks whether the sample id corresponds to any Individual and if it is parent of any other individual.
     * @param id Sample id that will be checked.
     * @throws CatalogDBException when the sample is parent of other individual.
     */
    @Deprecated
    public void checkSampleIsParentOfFamily(int id) throws CatalogDBException {
        Sample sample = get(id, new QueryOptions()).first();
        if (sample.getIndividualId() > 0) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), sample.getIndividualId())
                    .append(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), sample.getIndividualId());
            Long count = dbAdaptorFactory.getCatalogIndividualDBAdaptor().count(query).first();
            if (count > 0) {
                throw CatalogDBException.sampleIdIsParentOfOtherIndividual(id);
            }
        }
    }

    @Override
    public DBIterator<Sample> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = sampleCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, sampleConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = sampleCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(sampleCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        DBIterator<Sample> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        List<Bson> annotationList = new ArrayList<>();
        // We declare variableMap here just in case we have different annotation queries
        Map<String, Variable> variableMap = null;

        if (query.containsKey(QueryParams.ANNOTATION.key())) {
            fixAnnotationQuery(query);
        }

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam =  QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                throw CatalogDBException.queryParamNotFound(key, "Samples");
            }
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case VARIABLE_SET_ID:
                        addOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    case ANNOTATION:
                        if (variableMap == null) {
                            long variableSetId = query.getLong(QueryParams.VARIABLE_SET_ID.key());
                            if (variableSetId > 0) {
                                variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                                        .getVariables().stream().collect(Collectors.toMap(Variable::getName, Function.identity()));
                            }
                        }
                        addAnnotationQueryFilter(entry.getKey(), query, variableMap, annotationList);
                        break;
                    case ANNOTATION_SET_NAME:
                        addOrQuery("name", queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + query.toJson(), e);
                }
            }
        }

        if (annotationList.size() > 0) {
            Bson projection = Projections.elemMatch(QueryParams.ANNOTATION_SETS.key(), Filters.and(annotationList));
            andBsonList.add(projection);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    public MongoDBCollection getSampleCollection() {
        return sampleCollection;
    }

    QueryResult<Sample> setStatus(long sampleId, String status) throws CatalogDBException {
        return update(sampleId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    private void deleteReferencesToSample(long sampleId) throws CatalogDBException {
        // Remove references from files
        Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        QueryResult<Long> result = dbAdaptorFactory.getCatalogFileDBAdaptor()
                .extractSampleFromFiles(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} files", sampleId, result.first());

        // Remove references from cohorts
        query = new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
        result = dbAdaptorFactory.getCatalogCohortDBAdaptor().extractSamplesFromCohorts(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} cohorts", sampleId, result.first());
    }

    @Override
    public QueryResult<SampleAclEntry> createAcl(long id, SampleAclEntry acl) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.createAcl(id, acl, sampleCollection, "SampleAcl");
        return endQuery("create sample Acl", startTime, Arrays.asList(aclDBAdaptor.createAcl(id, acl)));
    }

    @Override
    public QueryResult<SampleAclEntry> getAcl(long id, List<String> members) throws CatalogDBException {
        long startTime = startQuery();
//
//        List<SampleAclEntry> acl = null;
//        QueryResult<Document> aggregate = CatalogMongoDBUtils.getAcl(id, members, sampleCollection, logger);
//        Sample sample = sampleConverter.convertToDataModelType(aggregate.first());
//
//        if (sample != null) {
//            acl = sample.getAcl();
//        }

        return endQuery("get sample Acl", startTime, aclDBAdaptor.getAcl(id, members));
    }

    @Override
    public void removeAcl(long id, String member) throws CatalogDBException {
//        CatalogMongoDBUtils.removeAcl(id, member, sampleCollection);
        aclDBAdaptor.removeAcl(id, member);
    }

    @Override
    public QueryResult<SampleAclEntry> setAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.setAclsToMember(id, member, permissions, sampleCollection);
        return endQuery("Set Acls to member", startTime, Arrays.asList(aclDBAdaptor.setAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<SampleAclEntry> addAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.addAclsToMember(id, member, permissions, sampleCollection);
        return endQuery("Add Acls to member", startTime, Arrays.asList(aclDBAdaptor.addAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<SampleAclEntry> removeAclsFromMember(long id, String member, List<String> permissions) throws CatalogDBException {
//        CatalogMongoDBUtils.removeAclsFromMember(id, member, permissions, sampleCollection);
        long startTime = startQuery();
        return endQuery("Remove Acls from member", startTime, Arrays.asList(aclDBAdaptor.removeAclsFromMember(id, member, permissions)));
    }

    public void removeAclsFromStudy(long studyId, String member) throws CatalogDBException {
        aclDBAdaptor.removeAclsFromStudy(studyId, member);
    }
}
