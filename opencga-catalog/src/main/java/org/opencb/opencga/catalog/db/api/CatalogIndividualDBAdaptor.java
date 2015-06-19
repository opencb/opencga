package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Individual;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface CatalogIndividualDBAdaptor {

    enum IndividualFilterOption implements CatalogDBAdaptor.FilterOption {
        studyId(Type.NUMERICAL, ""),
        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        father(Type.NUMERICAL, ""),
        mother(Type.NUMERICAL, ""),
        family(Type.TEXT, ""),
        gender(Type.TEXT, ""),
        race(Type.TEXT, ""),
        species(Type.TEXT, ""),
        population(Type.TEXT, ""),
        attributes("attributes", Type.TEXT, ""),
        nattributes("attributes", Type.NUMERICAL, ""),
        battributes("attributes", Type.BOOLEAN, ""),
        ;

        IndividualFilterOption(Type type, String description) {
            this._key = name();
            this._description = description;
            this._type = type;
        }

        IndividualFilterOption(String key, Type type, String description) {
            this._key = key;
            this._description = description;
            this._type = type;
        }

        final private String _key;
        final private String _description;
        final private Type _type;

        @Override
        public String getDescription() {
            return _description;
        }

        @Override
        public Type getType() {
            return _type;
        }

        @Override
        public String getKey() {
            return _key;
        }
    }

    /**
     * Individual methods
     * ***************************
     */
    
    QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getIndividual(int individualId, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getAllIndividuals(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> modifyIndividual(int individualId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<Integer> deleteIndividual(int individualId) throws CatalogDBException;

    int getStudyIdByIndividualId(int individualId) throws CatalogDBException;



}
