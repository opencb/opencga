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

package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections.map.LinkedMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.AbstractCatalogDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.acls.IndividualAcl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface CatalogIndividualDBAdaptor extends CatalogDBAdaptor<Individual> {

    default boolean individualExists(long sampleId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), sampleId)).first() > 0;
    }

    default void checkIndividualId(long individualId) throws CatalogDBException {
        if (individualId < 0) {
            throw CatalogDBException.newInstance("Individual id '{}' is not valid: ", individualId);
        }

        if (!individualExists(individualId)) {
            throw CatalogDBException.newInstance("Indivivual id '{}' does not exist", individualId);
        }
    }

    QueryResult<Individual> createIndividual(long studyId, Individual individual, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getIndividual(long individualId, QueryOptions options) throws CatalogDBException;

//    @Deprecated
//    QueryResult<Individual> getAllIndividuals(Query query, QueryOptions options) throws CatalogDBException;

//    QueryResult<Individual> getAllIndividualsInStudy(long studyId, QueryOptions options) throws CatalogDBException;

//    @Deprecated
//    QueryResult<Individual> modifyIndividual(long individualId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateIndividual(long individualId, AnnotationSet annotationSet, boolean overwrite) throws
            CatalogDBException;

    QueryResult<AnnotationSet> deleteAnnotation(long individualId, String annotationId) throws CatalogDBException;

    QueryResult<Individual> deleteIndividual(long individualId, QueryOptions options) throws CatalogDBException;

    default QueryResult<IndividualAcl> getIndividualAcl(long individualId, String member) throws CatalogDBException {
        return getIndividualAcl(individualId, Arrays.asList(member));
    }

    QueryResult<IndividualAcl> getIndividualAcl(long individualId, List<String> members) throws CatalogDBException;

    QueryResult<IndividualAcl> setIndividualAcl(long individualId, IndividualAcl acl, boolean override) throws CatalogDBException;

    void unsetIndividualAcl(long individualId, List<String> members, List<String> permissions) throws CatalogDBException;

    void unsetIndividualAclsInStudy(long studyId, List<String> members) throws CatalogDBException;

    long getStudyIdByIndividualId(long individualId) throws CatalogDBException;

    enum QueryParams implements QueryParam {
        ID("id", DECIMAL, ""),
        NAME("name", TEXT, ""),
        FATHER_ID("fatherId", DECIMAL, ""),
        MOTHER_ID("motherId", DECIMAL, ""),
        FAMILY("family", TEXT, ""),
        GENDER("gender", TEXT, ""),
        RACE("race", TEXT, ""),
        STATUS_STATUS("status.status", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        SPECIES("species", TEXT, ""),
        SPECIES_TAXONOMY_CODE("species.taxonomyCode", TEXT, ""),
        SPECIES_SCIENTIFIC_NAME("species.scientificName", TEXT, ""),
        SPECIES_COMMON_NAME("species.commonName", TEXT, ""),
        POPULATION_NAME("population.name", TEXT, ""),
        POPULATION_SUBPOPULATION("population.subpopulation", TEXT, ""),
        POPULATION_DESCRIPTION("population.description", TEXT, ""),
        ACLS("acls", TEXT_ARRAY, ""),
        ACLS_USERS("acls.users", TEXT_ARRAY, ""),
        ACLS_PERMISSIONS("acls.permissions", TEXT_ARRAY, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        STUDY_ID("studyId", DECIMAL, ""),
        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        VARIABLE_SET_ID("variableSetId", DECIMAL, ""),
        ANNOTATION_SET_ID("annotationSetId", TEXT, ""),
        ANNOTATION("annotation", TEXT, "");

        private static Map<String, QueryParams> map;
        static {
            map = new LinkedMap();
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

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

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    @Deprecated
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
        battributes("attributes", Type.BOOLEAN, "");

        private final String _key;
        private final String _description;
        private final Type _type;

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
