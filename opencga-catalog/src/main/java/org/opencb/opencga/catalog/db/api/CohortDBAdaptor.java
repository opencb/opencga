/*
 * Copyright 2015-2020 OpenCB
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

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 3/22/16.
 */
public interface CohortDBAdaptor extends AnnotationSetDBAdaptor<Cohort> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", DECIMAL, ""),
        UUID("uuid", TEXT, ""),
        TYPE("type", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        STATUS_DESCRIPTION("status.description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DESCRIPTION("internal.status.description", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        RELEASE("release", INTEGER, ""),

        SAMPLES("samples", TEXT_ARRAY, ""),
        SAMPLE_UIDS("samples.uid", INTEGER, ""),

        DELETED("deleted", BOOLEAN, ""),

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
//        VARIABLE_NAME("variableName", TEXT, ""),

        ANNOTATION("annotation", TEXT_ARRAY, ""),

        ATTRIBUTES("attributes", TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        NATTRIBUTES("nattributes", DECIMAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        BATTRIBUTES("battributes", BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""); // Alias to studyId in the database. Only for the webservices.

        private static Map<String, QueryParams> map;
        static {
            map = new HashMap<>();
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

    default boolean exists(long cohortId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), cohortId)).getNumMatches() > 0;
    }

    default void checkId(long cohortId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (cohortId < 0) {
            throw CatalogDBException.newInstance("Cohort id '{}' is not valid: ", cohortId);
        }

        if (!exists(cohortId)) {
            throw CatalogDBException.newInstance("Cohort id '{}' does not exist", cohortId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> cohort, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, Cohort cohort, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Cohort> get(long cohortId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Cohort> getAllInStudy(long studyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyId(long cohortId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

}
