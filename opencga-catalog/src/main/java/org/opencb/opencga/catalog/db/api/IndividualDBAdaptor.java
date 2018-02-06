/*
 * Copyright 2015-2017 OpenCB
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
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.VariableSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface IndividualDBAdaptor extends AnnotationSetDBAdaptor<Individual> {

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT, ""),
        FATHER("father", TEXT, ""),
        MOTHER("mother", TEXT, ""),
        FATHER_ID("father.id", DECIMAL, ""),
        MOTHER_ID("mother.id", DECIMAL, ""),
        MULTIPLES("multiples", TEXT, ""),
        FAMILY("family", TEXT, ""),
        SEX("sex", TEXT, ""),
        SAMPLES("samples", TEXT_ARRAY, ""),
        SAMPLES_ID("samples.id", INTEGER_ARRAY, ""),
        ETHNICITY("ethnicity", TEXT, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        POPULATION_NAME("population.name", TEXT, ""),
        POPULATION_SUBPOPULATION("population.subpopulation", TEXT, ""),
        POPULATION_DESCRIPTION("population.description", TEXT, ""),
        DATE_OF_BIRTH("dateOfBirth", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        RELEASE("release", INTEGER, ""), //  Release where the individual was created
        SNAPSHOT("snapshot", INTEGER, ""), // Last version of individual at release = snapshot
        VERSION("version", INTEGER, ""), // Version of the individual

        PHENOTYPES("phenotypes", TEXT_ARRAY, ""),
        PHENOTYPES_ID("phenotypes.id", TEXT, ""),
        PHENOTYPES_NAME("phenotypes.name", TEXT, ""),
        PHENOTYPES_SOURCE("phenotypes.source", TEXT, ""),

        KARYOTYPIC_SEX("karyotypicSex", TEXT, ""),
        LIFE_STATUS("lifeStatus", TEXT, ""),
        AFFECTATION_STATUS("affectationStatus", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        STUDY_ID("studyId", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""), // Alias to studyId in the database. Only for the webservices.
        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        VARIABLE_SET_ID("variableSetId", DECIMAL, ""),
        ANNOTATION_SET_NAME("annotationSetName", TEXT, ""),
        ANNOTATION("annotation", TEXT, ""),

        PRIVATE_FIELDS(SampleDBAdaptor.QueryParams.PRIVATE_FIELDS.key(), TEXT_ARRAY, ""); // Map of other fields

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

    default boolean exists(long sampleId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), sampleId)).first() > 0;
    }

    default void checkId(long individualId) throws CatalogDBException {
        if (individualId < 0) {
            throw CatalogDBException.newInstance("Individual id '{}' is not valid: ", individualId);
        }

        if (!exists(individualId)) {
            throw CatalogDBException.newInstance("Indivivual id '{}' does not exist", individualId);
        }
    }

    void nativeInsert(Map<String, Object> individual, String userId) throws CatalogDBException;

    default QueryResult<Individual> insert(long studyId, Individual individual, QueryOptions options) throws CatalogDBException {
        individual.setAnnotationSets(Collections.emptyList());
        return insert(studyId, individual, Collections.emptyList(), options);
    }

    QueryResult<Individual> insert(long studyId, Individual individual, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException;

    QueryResult<Individual> get(long individualId, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> get(long individualId, QueryOptions options, String userId)
            throws CatalogDBException, CatalogAuthorizationException;

//    @Deprecated
//    QueryResult<Individual> getAllIndividuals(Query query, QueryOptions options) throws CatalogDBException;

//    QueryResult<Individual> getAllIndividualsInStudy(long studyId, QueryOptions options) throws CatalogDBException;

//    @Deprecated
//    QueryResult<Individual> modifyIndividual(long individualId, QueryOptions parameters) throws CatalogDBException;

//    QueryResult<AnnotationSet> annotate(long individualId, AnnotationSet annotationSet, boolean overwrite) throws
//            CatalogDBException;

//    QueryResult<AnnotationSet> deleteAnnotation(long individualId, String annotationId) throws CatalogDBException;

    long getStudyId(long individualId) throws CatalogDBException;

    void updateProjectRelease(long studyId, int release) throws CatalogDBException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @throws CatalogException if there is any database error.
     */
    void unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

}
