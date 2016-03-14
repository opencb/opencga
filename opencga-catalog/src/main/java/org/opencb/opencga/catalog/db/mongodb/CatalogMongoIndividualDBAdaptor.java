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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
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
        return individualCollection.count(new Document(PRIVATE_ID, individualId)).first() != 0;
    }

    @Override
    public QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        if (!get(new Query(QueryParams.NAME.key(), individual.getName())
                .append(QueryParams.STUDY_ID.key(), studyId), new QueryOptions()).getResult().isEmpty()) {
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

        Document individualDocument = getMongoDBDocument(individual, "Individual");
        individualDocument.put(PRIVATE_ID, individualId);
        individualDocument.put(PRIVATE_STUDY_ID, studyId);
        QueryResult<WriteResult> insert = individualCollection.insert(individualDocument, null);

        return endQuery("createIndividual", startQuery, Collections.singletonList(individual));
    }

    @Override
    public QueryResult<Individual> getIndividual(int individualId, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();

        QueryResult<Document> result = individualCollection.find(new Document(PRIVATE_ID, individualId), filterOptions(options,
                FILTER_ROUTE_INDIVIDUALS));
        Individual individual = parseObject(result, Individual.class);
        if (individual == null) {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }

        return endQuery("getIndividual", startQuery, Collections.singletonList(individual));
    }

    @Deprecated
    @Override
    public QueryResult<Individual> getAllIndividuals(Query query, QueryOptions options) throws CatalogDBException {
        int variableSetId = query.getInt(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key());
        Map<String, Variable> variableMap = null;
        if (variableSetId > 0) {
            variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                    .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
        }
        return getAllIndividuals(query, options, variableMap);
    }

    @Deprecated
    public QueryResult<Individual> getAllIndividuals(Query query, QueryOptions options, Map<String, Variable> variableMap)
            throws CatalogDBException {
        long startTime = startQuery();

        List<Bson> mongoQueryList = new LinkedList<>();
        List<Bson> annotationSetFilter = new LinkedList<>();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                CatalogIndividualDBAdaptor.QueryParams option = CatalogIndividualDBAdaptor.QueryParams.getParam(key);
                switch (option) {
                    case ID:
                        addCompQueryFilter(option, option.name(), PRIVATE_ID, query, mongoQueryList);
                        break;
                    case STUDY_ID:
                        addCompQueryFilter(option, option.name(), PRIVATE_STUDY_ID, query, mongoQueryList);
                        break;
                    case ANNOTATION_SET_ID:
                        addCompQueryFilter(option, option.name(), "id", query, annotationSetFilter);
                        break;
                    case VARIABLE_SET_ID:
                        addCompQueryFilter(option, option.name(), option.key(), query, annotationSetFilter);
                        break;
                    case ANNOTATION:
                        addAnnotationQueryFilter(option.name(), query, variableMap, annotationSetFilter);
                        break;
                    default:
                        String queryKey = entry.getKey().replaceFirst(option.name(), option.key());
                        addCompQueryFilter(option, entry.getKey(), queryKey, query, mongoQueryList);
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
        if (!annotationSetFilter.isEmpty()) {
            mongoQuery.put("annotationSets", new Document("$elemMatch", new Document("$and", annotationSetFilter)) {
            });
        }
        QueryResult<Document> result = individualCollection.find(mongoQuery, filterOptions(options, FILTER_ROUTE_INDIVIDUALS));
        List<Individual> individuals = parseObjects(result, Individual.class);
        return endQuery("getAllIndividuals", startTime, individuals);
    }

    @Deprecated
    @Override
    public QueryResult<Individual> getAllIndividualsInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Deprecated
    @Override
    public QueryResult<Individual> modifyIndividual(int individualId, QueryOptions parameters) throws CatalogDBException {
        throw new UnsupportedOperationException("Deprecated method. Use update instead");
/*
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
            Query query = new Query(QueryParams.NAME.key(), name)
                    .append(QueryParams.STUDY_ID.key(), getStudyIdByIndividualId(individualId));
            if (!getAllIndividuals(query, new QueryOptions()).getResult().isEmpty()) {
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
        */
    }

    @Override
    public QueryResult<AnnotationSet> annotateIndividual(int individualId, AnnotationSet annotationSet, boolean overwrite)
            throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = individualCollection.count(
                new Document("annotationSets.id", annotationSet.getId()).append(PRIVATE_ID, individualId));

        if (overwrite) {
            if (count.first() == 0) {
                throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getId());
            }
        } else {
            if (count.first() > 0) {
                throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
            }
        }

        Document document = getMongoDBDocument(annotationSet, "AnnotationSet");

        Bson query;
        Bson individualQuery = Filters.eq(PRIVATE_ID, individualId);
        if (overwrite) {
//            query.put("annotationSets.id", annotationSet.getId());
            query = Filters.and(individualQuery, Filters.eq("annotationSets.id", annotationSet.getId()));
        } else {
//            query.put("annotationSets.id", new BasicDBObject("$ne", annotationSet.getId()));
            query = Filters.and(individualQuery, Filters.eq("annotationSets.id", new Document("$ne", annotationSet.getId())));
        }

        Bson update;
        if (overwrite) {
            update = new Document("$set", new Document("annotationSets.$", document));
        } else {
            update = new Document("$push", new Document("annotationSets", document));
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

        QueryResult<DeleteResult> remove = individualCollection.remove(new Document(PRIVATE_ID, individualId), options);
        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }

        return endQuery("Delete individual", startTime, individual);
    }

    public void checkInUse(int individualId) throws CatalogDBException {
        int studyId = getStudyIdByIndividualId(individualId);
        QueryResult<Individual> individuals = get(new Query(QueryParams.FATHER_ID.key(), individualId)
                .append(QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"fatherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        individuals = get(new Query(QueryParams.MOTHER_ID.key(), individualId)
                .append(QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"motherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(
                new Query(CatalogSampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId), new QueryOptions());
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
                individualCollection.find(new Document(PRIVATE_ID, individualId), new Document(PRIVATE_STUDY_ID, 1), null);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return individualCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
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
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
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
        long startTime = startQuery();
        checkIndividualId(id);
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Long> update = update(query, parameters);
        if (update.getResult().isEmpty() || update.first() != 1) {
            throw new CatalogDBException("Could not update individual " + id);
        }
        return endQuery("Update individual", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> individualParameters = new HashMap<>();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.FAMILY.key(), QueryParams.RACE.key(), QueryParams.GENDER.key(),
                QueryParams.SPECIES_TAXONOMY_CODE.key(), QueryParams.SPECIES_SCIENTIFIC_NAME.key(), QueryParams.SPECIES_COMMON_NAME.key(),
                QueryParams.POPULATION_NAME.key(), QueryParams.POPULATION_SUBPOPULATION.key(), QueryParams.POPULATION_DESCRIPTION.key(), };
        filterStringParams(parameters, individualParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap((QueryParams.GENDER.key()), Individual.Gender.class);
        filterEnumParams(parameters, individualParameters, acceptedEnums);

        String[] acceptedIntParams = {QueryParams.FATHER_ID.key(), QueryParams.MOTHER_ID.key()};
        filterIntParams(parameters, individualParameters, acceptedIntParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, individualParameters, acceptedMapParams);

        // Obtain all the possible individual Ids that satisfies the query
        QueryResult<Individual> myResults= get(query, new QueryOptions("include", "id"));

        for (Individual individual : myResults.getResult()) {
            //Check existing name
            if (individualParameters.containsKey("name")) {
                String name = individualParameters.get("name").toString();
                Query subquery = new Query(QueryParams.NAME.key(), name)
                        .append(QueryParams.STUDY_ID.key(), getStudyIdByIndividualId(individual.getId()));
                if (!get(subquery, new QueryOptions()).getResult().isEmpty()) {
                    throw CatalogDBException.alreadyExists("Individual", "name", name);
                }
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
            QueryResult<UpdateResult> update = individualCollection.update(parseQuery(query), new Document("$set", individualParameters),
                    null);
            return endQuery("Update individual", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update individual", startTime, new QueryResult<Long>());
    }

    @Override
    public QueryResult<Long> delete(Query query) throws CatalogDBException {
        long startTime = startQuery();
        Bson bson = parseQuery(query);
        QueryResult<DeleteResult> remove = individualCollection.remove(bson, null);
        return endQuery("Delete individual", startTime, Arrays.asList(remove.getNumTotalResults()));
    }

    @Override
    public QueryResult<Individual> delete(int id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Individual> individualQueryResult = get(query, null);
        if (individualQueryResult.getResult().size() == 1) {
            QueryResult<Long> delete = delete(query);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Individual id '{}' has not been deleted", id);
            }
        } else {
            throw CatalogDBException.idNotFound("Individual id '{}' does not exist (or there are too many)", id);
        }
        return individualQueryResult;
    }

    @Override
    public CatalogDBIterator<Individual> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = individualCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, individualConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = individualCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(individualCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<Individual> catalogDBIterator = iterator(query, options);
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

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
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
                            int variableSetId = query.getInt(QueryParams.VARIABLE_SET_ID.key());
                            variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                                    .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
                        }
                        addAnnotationQueryFilter(entry.getKey(), query, variableMap, annotationList);
                        break;
                    case ANNOTATION_SET_ID:
                        addOrQuery("id", queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (annotationList.size() > 0) {
            Bson projection = Projections.elemMatch("annotationSets", Filters.and(annotationList));
            andBsonList.add(projection);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
