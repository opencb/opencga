package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Individual;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor.*;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class CatalogMongoIndividualDBAdaptor extends CatalogDBAdaptor implements CatalogIndividualDBAdaptor {

    private final MongoDBCollection metaCollection;
    private final MongoDBCollection individualCollection;

    public CatalogMongoIndividualDBAdaptor(MongoDBCollection metaCollection, MongoDBCollection individualCollection) {
        this.metaCollection = metaCollection;
        this.individualCollection = individualCollection;
    }

    @Override
    public QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();

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

        QueryResult<DBObject> result = individualCollection.find(new BasicDBObject(_ID, individualId), options);
        Individual individual = parseObject(result, Individual.class);

        return endQuery("getIndividual", startQuery, Collections.singletonList(individual));
    }

    @Override
    public QueryResult<Individual> getAllIndividuals(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();

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
        QueryResult<DBObject> result = individualCollection.find(new BasicDBObject("$and", mongoQueryList), options);
        List<Individual> individuals = parseObjects(result, Individual.class);
        return endQuery("getAllIndividuals", startTime, individuals);
    }

    @Override
    public QueryResult<Individual> modifyIndividual(int individualId, QueryOptions parameters) throws CatalogDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult<Integer> deleteIndividual(int individualId) throws CatalogDBException {
        throw new UnsupportedOperationException();
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
