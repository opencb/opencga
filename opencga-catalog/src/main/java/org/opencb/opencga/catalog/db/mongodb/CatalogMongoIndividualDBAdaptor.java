package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Individual;

import java.util.*;

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
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.metaCollection = metaCollection;
        this.individualCollection = individualCollection;
    }

    boolean individualExists(int individualId) {
        return individualCollection.count(new BasicDBObject(_ID, individualId)).first() != 0;
    }

    @Override
    public QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();


        if (!dbAdaptorFactory.getCatalogStudyDBAdaptor().studyExists(studyId)) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
        if (!getAllIndividuals(studyId, new QueryOptions(IndividualFilterOption.name.toString(), individual.getName())).getResult().isEmpty()) {
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
    public QueryResult<Individual> getAllIndividuals(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();
        options.put(IndividualFilterOption.studyId.toString(), studyId);
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                IndividualFilterOption option = IndividualFilterOption.valueOf(key);
                switch (option) {
                    case id:
                        addCompQueryFilter(option.getType(), option.name(), options, _ID, mongoQueryList);
                        break;
                    case studyId:
                        addCompQueryFilter(option.getType(), option.name(), options, _STUDY_ID, mongoQueryList);
                        break;
                    default:
                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option.getType(), entry.getKey(), options, queryKey, mongoQueryList);
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }
        QueryResult<DBObject> result = individualCollection.find(new BasicDBObject("$and", mongoQueryList), filterOptions(options, FILTER_ROUTE_INDIVIDUALS));
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
            if (!getAllIndividuals(getStudyIdByIndividualId(individualId), new QueryOptions(IndividualFilterOption.name.toString(), name)).getResult().isEmpty()) {
                throw CatalogDBException.alreadyExists("Individual", "name", name);
            }
        }
        //Check individualIds exists
        String[] individualIdParams = {"fatherId", "motherId"};
        for (String individualIdParam : individualIdParams) {
            if (individualParameters.containsKey(individualIdParam)) {
                Integer individualId1 = (Integer) individualParameters.get(individualIdParam);
                if (!individualExists(individualId1)) {
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
    public QueryResult<Integer> deleteIndividual(int individualId) throws CatalogDBException {
        throw new CatalogDBException("Unimplemented method deleteIndividual");
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
