package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface CatalogIndividualDBAdaptor {

    enum IndividualFilterOption implements CatalogDBAdaptor.FilterOption {
        studyId(Type.NUMERICAL, ""),
        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        fatherId(Type.NUMERICAL, ""),
        motherId(Type.NUMERICAL, ""),
        family(Type.TEXT, ""),
        gender(Type.TEXT, ""),
        race(Type.TEXT, ""),
        species(Type.TEXT, ""),
        population(Type.TEXT, ""),

        variableSetId(Type.NUMERICAL, ""),
        annotationSetId(Type.NUMERICAL, ""),
        annotation(Type.TEXT, ""),

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
    boolean individualExists(int individualId);

    QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getIndividual(int individualId, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getAllIndividuals(QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> modifyIndividual(int individualId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateIndividual(int individualId, AnnotationSet annotationSet, boolean overwrite) throws CatalogDBException;

    QueryResult<AnnotationSet> deleteAnnotation(int individualId, String annotationId) throws CatalogDBException;

    QueryResult<Individual> deleteIndividual(int individualId, QueryOptions options) throws CatalogDBException;

    int getStudyIdByIndividualId(int individualId) throws CatalogDBException;



}
