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
import com.mongodb.WriteResult;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.IndividualConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Variable;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class CatalogMongoIndividualDBAdaptor extends CatalogMongoDBAdaptor implements CatalogIndividualDBAdaptor {

    private final MongoDBCollection individualCollection;
    private IndividualConverter individualConverter;

    public CatalogMongoIndividualDBAdaptor(MongoDBCollection individualCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoIndividualDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.individualCollection = individualCollection;
        this.individualConverter = new IndividualConverter();
    }

    @Override
    public boolean individualExists(int individualId) {
        return individualCollection.count(new BasicDBObject(PRIVATE_ID, individualId)).first() != 0;
    }

    @Override
    public QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        if (!getAllIndividuals(new QueryOptions(IndividualFilterOption.name.toString(), individual.getName())
                .append(IndividualFilterOption.studyId.toString(), studyId)).getResult().isEmpty()) {
            throw CatalogDBException.alreadyExists("Individual", "name", individual.getName());
        }
        if (individual.getFatherId() > 0 && !individualExists(individual.getFatherId())) {
            throw CatalogDBException.idNotFound("Individual", individual.getFatherId());
        }
        if (individual.getMotherId() > 0 && !individualExists(individual.getMotherId())) {
            throw CatalogDBException.idNotFound("Individual", individual.getMotherId());
        }

        int individualId = getNewId();

        individual.setId(individualId);

        Document individualDbObject = getMongoDBDocument(individual, "Individual");
        individualDbObject.put(PRIVATE_ID, individualId);
        individualDbObject.put(PRIVATE_STUDY_ID, studyId);
        QueryResult<WriteResult> insert = individualCollection.insert(individualDbObject, null);

        return endQuery("createIndividual", startQuery, Collections.singletonList(individual));
    }

    @Override
    public QueryResult<Individual> getIndividual(int individualId, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();

        QueryResult<Document> result = individualCollection.find(new BasicDBObject(PRIVATE_ID, individualId), filterOptions(options,
                FILTER_ROUTE_INDIVIDUALS));
        Individual individual = parseObject(result, Individual.class);
        if (individual == null) {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }

        return endQuery("getIndividual", startQuery, Collections.singletonList(individual));
    }

    @Deprecated
    @Override
    public QueryResult<Individual> getAllIndividuals(QueryOptions options) throws CatalogDBException {
        int variableSetId = options.getInt(CatalogSampleDBAdaptor.SampleFilterOption.variableSetId.toString());
        Map<String, Variable> variableMap = null;
        if (variableSetId > 0) {
            variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                    .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
        }
        return getAllIndividuals(options, variableMap);
    }


    public QueryResult<Individual> getAllIndividuals(QueryOptions options, Map<String, Variable> variableMap) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();
        List<DBObject> annotationSetFilter = new LinkedList<>();
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                IndividualFilterOption option = IndividualFilterOption.valueOf(key);
                switch (option) {
                    case id:
                        addCompQueryFilter(option, option.name(), options, PRIVATE_ID, mongoQueryList);
                        break;
                    case studyId:
                        addCompQueryFilter(option, option.name(), options, PRIVATE_STUDY_ID, mongoQueryList);
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
                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), options, queryKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        BasicDBObject mongoQuery = new BasicDBObject();
        if (!mongoQueryList.isEmpty()) {
            mongoQuery.put("$and", mongoQueryList);
        }
        if (!annotationSetFilter.isEmpty()) {
            mongoQuery.put("annotationSets", new BasicDBObject("$elemMatch", new BasicDBObject("$and", annotationSetFilter)));
        }
        QueryResult<Document> result = individualCollection.find(mongoQuery, filterOptions(options, FILTER_ROUTE_INDIVIDUALS));
        List<Individual> individuals = parseObjects(result, Individual.class);
        return endQuery("getAllIndividuals", startTime, individuals);
    }

    @Override
    public QueryResult<Individual> getAllIndividualsInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(PRIVATE_STUDY_ID, studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<Individual> modifyIndividual(int individualId, QueryOptions parameters) throws CatalogDBException {

        long startTime = startQuery();
        Map<String, Object> individualParameters = new HashMap<>();

        String[] acceptedParams = {"name", "family", "race", "gender",
                "species.taxonomyCode", "species.scientificName", "species.commonName",
                "population.name", "population.subpopulation", "population.description", };
        filterStringParams(parameters, individualParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("gender"), Individual.Gender.class);
        filterEnumParams(parameters, individualParameters, acceptedEnums);

        String[] acceptedIntParams = {"fatherId", "motherId"};
        filterIntParams(parameters, individualParameters, acceptedIntParams);

        String[] acceptedMapParams = {"attributes"};
        filterMapParams(parameters, individualParameters, acceptedMapParams);


        //Check existing name
        if (individualParameters.containsKey("name")) {
            String name = individualParameters.get("name").toString();
            QueryOptions query = new QueryOptions(IndividualFilterOption.name.toString(), name)
                    .append(IndividualFilterOption.studyId.toString(), getStudyIdByIndividualId(individualId));
            if (!getAllIndividuals(query).getResult().isEmpty()) {
                throw CatalogDBException.alreadyExists("Individual", "name", name);
            }
        }
        //Check individualIds exists
        String[] individualIdParams = {"fatherId", "motherId"};
        for (String individualIdParam : individualIdParams) {
            if (individualParameters.containsKey(individualIdParam)) {
                Integer individualId1 = (Integer) individualParameters.get(individualIdParam);
                if (individualId1 > 0 && !individualExists(individualId1)) {
                    throw CatalogDBException.idNotFound("Individual " + individualIdParam, individualId1);
                }
            }
        }


        if (!individualParameters.isEmpty()) {
            QueryResult<UpdateResult> update = individualCollection.update(
                    new BasicDBObject(PRIVATE_ID, individualId),
                    new BasicDBObject("$set", individualParameters), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Individual", individualId);
            }
        }

        return endQuery("Modify individual", startTime, getIndividual(individualId, parameters));
    }

    @Override
    public QueryResult<AnnotationSet> annotateIndividual(int individualId, AnnotationSet annotationSet, boolean overwrite)
            throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = individualCollection.count(
                new BasicDBObject("annotationSets.id", annotationSet.getId()).append(PRIVATE_ID, individualId));

        if (overwrite) {
            if (count.first() == 0) {
                throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getId());
            }
        } else {
            if (count.first() > 0) {
                throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
            }
        }

        DBObject object = getDbObject(annotationSet, "AnnotationSet");

        Bson query;
        Bson individualQuery = Filters.eq(PRIVATE_ID, individualId);
        if (overwrite) {
//            query.put("annotationSets.id", annotationSet.getId());
            query = Filters.and(individualQuery, Filters.eq("annotationSets.id", annotationSet.getId()));
        } else {
//            query.put("annotationSets.id", new BasicDBObject("$ne", annotationSet.getId()));
            query = Filters.and(individualQuery, Filters.eq("annotationSets.id", new BasicDBObject("$ne", annotationSet.getId())));
        }

        Bson update;
        if (overwrite) {
            update = new BasicDBObject("$set", new BasicDBObject("annotationSets.$", object));
        } else {
            update = new BasicDBObject("$push", new BasicDBObject("annotationSets", object));
        }

        QueryResult<UpdateResult> queryResult = individualCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
        }

        return endQuery("", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(int individualId, String annotationId) throws CatalogDBException {

        long startTime = startQuery();

        Individual individual =
                getIndividual(individualId, new QueryOptions("include", "projects.studies.individuals.annotationSets")).first();
        AnnotationSet annotationSet = null;
        for (AnnotationSet as : individual.getAnnotationSets()) {
            if (as.getId().equals(annotationId)) {
                annotationSet = as;
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

//        DBObject query = new BasicDBObject(PRIVATE_ID, individualId);
//        DBObject update = new BasicDBObject("$pull", new BasicDBObject("annotationSets", new BasicDBObject("id", annotationId)));
//        QueryResult<WriteResult> resultQueryResult = individualCollection.update(query, update, null);
//        if (resultQueryResult.first().getN() < 1) {
//            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
//        }

        Bson eq = Filters.eq(PRIVATE_ID, individualId);
        Bson pull = Updates.pull("annotationSets", new Document("id", annotationId));
        QueryResult<UpdateResult> update = individualCollection.update(eq, pull, null);
        if (update.first().getModifiedCount() < 1) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        return endQuery("Delete annotation", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<Individual> deleteIndividual(int individualId, QueryOptions options) throws CatalogDBException {

        long startTime = startQuery();

        QueryResult<Individual> individual = getIndividual(individualId, options);

        checkInUse(individualId);

        QueryResult<DeleteResult> remove = individualCollection.remove(new BasicDBObject(PRIVATE_ID, individualId), options);
        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }

        return endQuery("Delete individual", startTime, individual);
    }

    public void checkInUse(int individualId) throws CatalogDBException {
        int studyId = getStudyIdByIndividualId(individualId);
        QueryResult<Individual> individuals = getAllIndividuals(new QueryOptions(IndividualFilterOption.fatherId.toString(),
                individualId).append(IndividualFilterOption.studyId.toString(), studyId));
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"fatherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        individuals = getAllIndividuals(new QueryOptions(IndividualFilterOption.motherId.toString(), individualId)
                .append(IndividualFilterOption.studyId.toString(), studyId));
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"motherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().getAllSamples(new QueryOptions(CatalogSampleDBAdaptor
                .SampleFilterOption.individualId.toString(), individualId));
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"individualId\" of sample : [";
            for (Sample sample : samples.getResult()) {
                msg += " { id: " + sample.getId() + ", name: \"" + sample.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }

    }

    @Override
    public int getStudyIdByIndividualId(int individualId) throws CatalogDBException {
        QueryResult<Document> result =
                individualCollection.find(new BasicDBObject(PRIVATE_ID, individualId), new BasicDBObject(PRIVATE_STUDY_ID, 1), null);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) {
        Bson bson = parseQuery(query);
        return individualCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        Bson bson = parseQuery(query);
        return individualCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_INDIVIDUALS);
        QueryResult<Individual> individualQueryResult = individualCollection.find(bson, individualConverter, qOptions);
        return endQuery("Get Individual", startTime, individualQueryResult.getResult());
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
        qOptions = filterOptions(qOptions, FILTER_ROUTE_INDIVIDUALS);
        return individualCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Individual> update(int id, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) {
        return null;
    }

    @Override
    public QueryResult<Long> delete(Query query) {
        return null;
    }

    @Override
    public QueryResult<Individual> delete(int id) throws CatalogDBException {
        return null;
    }

    @Override
    public CatalogDBIterator<Individual> iterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = individualCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) {

    }

    private Bson parseQuery(Query query) {
        List<Bson> andBsonList = new ArrayList<>();

        // FIXME: Pedro. Check the mongodb names as well as integer createQueries

        addIntegerOrQuery(PRIVATE_ID, QueryParams.ID.key(), query, andBsonList);
        addStringOrQuery("name", QueryParams.NAME.key(), query, andBsonList);
        addStringOrQuery("fatherId", QueryParams.FATHER_ID.key(), query, andBsonList);
        addStringOrQuery("motherId", QueryParams.MOTHER_ID.key(), query, andBsonList);
        addStringOrQuery("family", QueryParams.FAMILY.key(), query, andBsonList);
        addStringOrQuery("gender", QueryParams.GENDER.key(), query, andBsonList);
        addStringOrQuery("race", QueryParams.RACE.key(), query, andBsonList);
        addStringOrQuery("populationName", QueryParams.POPULATION_NAME.key(), query, andBsonList);
        addStringOrQuery("populationSubpopulation", QueryParams.POPULATION_SUBPOPULATION.key(), query, andBsonList);
        addIntegerOrQuery(PRIVATE_STUDY_ID, QueryParams.STUDY_ID.key(), query, andBsonList);
        addIntegerOrQuery(PRIVATE_STUDY_ID, PRIVATE_STUDY_ID, query, andBsonList);

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
