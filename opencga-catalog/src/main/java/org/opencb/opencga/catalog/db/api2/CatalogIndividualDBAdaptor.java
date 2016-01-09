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

package org.opencb.opencga.catalog.db.api2;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Study;

import static org.opencb.commons.datastore.core.QueryParam.Type.DECIMAL;
import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER_ARRAY;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface CatalogIndividualDBAdaptor extends CatalogDBAdaptor<Individual> {

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        FATHER_ID("fatherId", INTEGER_ARRAY, ""),
        MOTHER_ID("motherId", INTEGER_ARRAY, ""),
        FAMILY("family", TEXT_ARRAY, ""),
        GENDER("gender", TEXT_ARRAY, ""),
        RACE("race", TEXT_ARRAY, ""),
        POPULATION_NAME("populationName", TEXT_ARRAY, ""),
        POPULATION_SUBPOPULATION("populationSubpopulation", TEXT_ARRAY, "");

        // TOCHECK: Pedro. Should we be considering annotations?

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }
    }


    default boolean individualExists(int sampleId) {
        return count(new Query(QueryParams.ID.key(), sampleId)).first() > 0;
    }

    default void checkIndividualId(int individualId) throws CatalogDBException {
        if (individualId < 0) {
            throw CatalogDBException.newInstance("Individual id '{}' is not valid: ", individualId);
        }

        if (!individualExists(individualId)) {
            throw CatalogDBException.newInstance("Indivivual id '{}' does not exist", individualId);
        }
    }

    QueryResult<Individual> createIndividual(int studyId, Individual individual, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getIndividual(int individualId, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getAllIndividuals(QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> modifyIndividual(int individualId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateIndividual(int individualId, AnnotationSet annotationSet, boolean overwrite) throws
            CatalogDBException;

    QueryResult<AnnotationSet> deleteAnnotation(int individualId, String annotationId) throws CatalogDBException;

    QueryResult<Individual> deleteIndividual(int individualId, QueryOptions options) throws CatalogDBException;

    int getStudyIdByIndividualId(int individualId) throws CatalogDBException;

    enum IndividualFilterOption implements AbstractCatalogDBAdaptor.FilterOption {
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
        battributes("attributes", Type.BOOLEAN, ""),;

        final private String _key;
        final private String _description;
        final private Type _type;
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


}
