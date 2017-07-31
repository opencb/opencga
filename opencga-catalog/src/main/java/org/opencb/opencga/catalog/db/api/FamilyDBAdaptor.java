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
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Family;

import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 03/05/17.
 */
public interface FamilyDBAdaptor extends AnnotationSetDBAdaptor<Family> {

    enum QueryParams implements QueryParam {
        ID("id", INTEGER, ""),
        NAME("name", TEXT, ""),
        PARENTAL_CONSANGUINITY("parentalConsanguinity", BOOLEAN, ""),
        FATHER("father", TEXT, ""),
        MOTHER("mother", TEXT, ""),
        CHILDREN("children", TEXT_ARRAY, ""),
        FATHER_ID("fatherId", INTEGER, ""),
        MOTHER_ID("motherId", INTEGER, ""),
        CHILDREN_IDS("childrenIds", INTEGER_ARRAY, ""),
        CREATION_DATE("creationDate", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        RELEASE("release", INTEGER, ""),

        STUDY_ID("studyId", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""), // Alias to studyId in the database. Only for the webservices.

        ONTOLOGIES("ontologies", TEXT_ARRAY, ""), // Alias in the webservice to ONTOLOGY_TERMS
        ONTOLOGY_TERMS("ontologyTerms", TEXT_ARRAY, ""),
        ONTOLOGY_TERMS_ID("ontologyTerms.id", TEXT, ""),
        ONTOLOGY_TERMS_NAME("ontologyTerms.name", TEXT, ""),
        ONTOLOGY_TERMS_SOURCE("ontologyTerms.source", TEXT, ""),
        ONTOLOGY_TERMS_AGE_OF_ONSET("ontologyTerms.ageOfOnset", TEXT, ""),
        ONTOLOGY_TERMS_MODIFIERS("ontologyTerms.modifiers", TEXT_ARRAY, ""),

        VARIABLE_SET_ID("variableSetId", INTEGER, ""),
        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        ANNOTATION_SET_NAME("annotationSetName", TEXT_ARRAY, ""),
        ANNOTATION("annotation", TEXT_ARRAY, "");

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

    default boolean exists(long familyId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), familyId)).first() > 0;
    }

    default void checkId(long familyId) throws CatalogDBException {
        if (familyId < 0) {
            throw CatalogDBException.newInstance("Family id '{}' is not valid: ", familyId);
        }

        if (!exists(familyId)) {
            throw CatalogDBException.newInstance("Family id '{}' does not exist", familyId);
        }
    }

    QueryResult<Family> insert(Family family, long studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Family> get(long familyId, QueryOptions options) throws CatalogDBException;

    long getStudyId(long familyId) throws CatalogDBException;

}
