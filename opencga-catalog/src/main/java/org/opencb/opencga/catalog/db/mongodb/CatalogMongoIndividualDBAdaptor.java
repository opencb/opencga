package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Variable;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor.*;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class CatalogMongoIndividualDBAdaptor extends CatalogDBAdaptor implements CatalogIndividualDBAdaptor {

    private CatalogDBAdaptorFactory dbAdaptorFactory;
    private final MongoDBCollection metaCollection;
    private final MongoDBCollection individualCollection;

    public CatalogMongoIndividualDBAdaptor(CatalogDBAdaptorFactory dbAdaptorFactory, MongoDBCollection metaCollection, MongoDBCollection individualCollection) {
        super(LoggerFactory.getLogger(CatalogMongoIndividualDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaCollection;
        this.individualCollection = individualCollection;
    }

    @Override
    public boolean individualExists(int individualId) {
        return individualCollection.count(new BasicDBObject(_ID, individualId)).first() != 0;
    }

    @Override
    public QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();


        if (!dbAdaptorFactory.getCatalogStudyDBAdaptor().studyExists(studyId)) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
        if (!getAllIndividuals(new QueryOptions(IndividualFilterOption.name.toString(), individual.getName()).append(IndividualFilterOption.studyId.toString(), studyId)).getResult().isEmpty()) {
            throw CatalogDBException.alreadyExists("Individual", "name", individual.getName());
        }
        if (individual.getFatherId() > 0 && !individualExists(individual.getFatherId())) {
            throw CatalogDBException.idNotFound("Individual", individual.getFatherId());
        }
        if (individual.getMotherId() > 0 && !individualExists(individual.getMotherId())) {
            throw CatalogDBException.idNotFound("Individual", individual.getMotherId());
        }

        int individualId = getNewAutoIncrementId(metaCollection);

        individual.setId(individualId);
        DBObject individualDbObject = getDbObject(individual, "Individual");

        individualDbObject.put(_ID, individualId);
        individualDbObject.put(_STUDY_ID, studyId);
        QueryResult<WriteResult> insert = individualCollection.insert(individualDbObject, null);

        return endQuery("createIndividual", startQuery, Collections.singletonList(individual));
    }

    @Override
    public QueryResult<Individual> getIndividual(int individualId, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();

        QueryResult<DBObject> result = individualCollection.find(new BasicDBObject(_ID, individualId), filterOptions(options, FILTER_ROUTE_INDIVIDUALS));
        Individual individual = parseObject(result, Individual.class);
        if (individual == null) {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }

        return endQuery("getIndividual", startQuery, Collections.singletonList(individual));
    }

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
        QueryResult<DBObject> result = individualCollection.find(mongoQuery, filterOptions(options, FILTER_ROUTE_INDIVIDUALS));
        List<Individual> individuals = parseObjects(result, Individual.class);
        return endQuery("getAllIndividuals", startTime, individuals);
    }

    @Override
    public QueryResult<Individual> modifyIndividual(int individualId, QueryOptions parameters) throws CatalogDBException {

        long startTime = startQuery();
        Map<String, Object> individualParameters = new HashMap<>();

        String[] acceptedParams = {"name", "family", "race", "gender",
                "species.taxonomyCode", "species.scientificName", "species.commonName",
                "population.name", "population.subpopulation", "population.description"};
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


        if(!individualParameters.isEmpty()) {
            QueryResult<WriteResult> update = individualCollection.update(
                    new BasicDBObject(_ID, individualId),
                    new BasicDBObject("$set", individualParameters), null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
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
                new BasicDBObject("annotationSets.id", annotationSet.getId()).append(_ID, individualId));

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

        DBObject query = new BasicDBObject(_ID, individualId);
        if (overwrite) {
            query.put("annotationSets.id", annotationSet.getId());
        } else {
            query.put("annotationSets.id", new BasicDBObject("$ne", annotationSet.getId()));
        }

        DBObject update;
        if (overwrite) {
            update = new BasicDBObject("$set", new BasicDBObject("annotationSets.$", object));
        } else {
            update = new BasicDBObject("$push", new BasicDBObject("annotationSets", object));
        }

        QueryResult<WriteResult> queryResult = individualCollection.update(query, update, null);

        if (queryResult.first().getN() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
        }

        return endQuery("", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(int individualId, String annotationId) throws CatalogDBException {

        long startTime = startQuery();

        Individual individual = getIndividual(individualId, new QueryOptions("include", "projects.studies.individuals.annotationSets")).first();
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

        DBObject query = new BasicDBObject(_ID, individualId);
        DBObject update = new BasicDBObject("$pull", new BasicDBObject("annotationSets", new BasicDBObject("id", annotationId)));
        QueryResult<WriteResult> resultQueryResult = individualCollection.update(query, update, null);
        if (resultQueryResult.first().getN() < 1) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        return endQuery("Delete annotation", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<Individual> deleteIndividual(int individualId, QueryOptions options) throws CatalogDBException {

        long startTime = startQuery();

        QueryResult<Individual> individual = getIndividual(individualId, options);

        checkInUse(individualId);

        QueryResult<WriteResult> remove = individualCollection.remove(new BasicDBObject(_ID, individualId), options);
        if (remove.first().getN() == 0) {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }

        return endQuery("Delete individual", startTime, individual);
    }

    public void checkInUse(int individualId) throws CatalogDBException {
        int studyId = getStudyIdByIndividualId(individualId);
        QueryResult<Individual> individuals = getAllIndividuals(new QueryOptions(IndividualFilterOption.fatherId.toString(), individualId).append(IndividualFilterOption.studyId.toString(), studyId));
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"fatherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        individuals = getAllIndividuals(new QueryOptions(IndividualFilterOption.motherId.toString(), individualId).append(IndividualFilterOption.studyId.toString(), studyId));
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"motherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().getAllSamples(new QueryOptions(CatalogSampleDBAdaptor.SampleFilterOption.individualId.toString(), individualId));
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
        QueryResult<DBObject> result = individualCollection.find(new BasicDBObject(_ID, individualId), new BasicDBObject(_STUDY_ID, 1), null);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get(_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }
    }
}
