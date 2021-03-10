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

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface SampleDBAdaptor extends AnnotationSetDBAdaptor<Sample> {

    // TODO: Replace with QueryParam
    String STATS_ID = "stats.id";
    String STATS_VARIANT_COUNT = "stats.variantCount";

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", LONG, ""),
        UUID("uuid", TEXT, ""),
        PROCESSING("processing", TEXT_ARRAY, ""),
        COLLECTION("collection", TEXT_ARRAY, ""),
        INDIVIDUAL("individual", TEXT, ""),
        INDIVIDUAL_UID("individual.uid", INTEGER_ARRAY, ""),
        INDIVIDUAL_ID("individualId", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        FILE_IDS("fileIds", TEXT_ARRAY, ""),
        SOMATIC("somatic", BOOLEAN, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        STATUS_DESCRIPTION("status.description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        INTERNAL_RGA("internal.rga", OBJECT, ""),
        RELEASE("release", INTEGER, ""), //  Release where the sample was created
        SNAPSHOT("snapshot", INTEGER, ""), // Last version of sample at release = snapshot
        VERSION("version", INTEGER, ""), // Version of the sample
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""), // Alias to studyId in the database. Only for the webservices.

        QUALITY_CONTORL("qualityControl", TEXT_ARRAY, ""),
        PHENOTYPES("phenotypes", TEXT_ARRAY, ""),
        PHENOTYPES_ID("phenotypes.id", TEXT, ""),
        PHENOTYPES_NAME("phenotypes.name", TEXT, ""),
        PHENOTYPES_SOURCE("phenotypes.source", TEXT, ""),

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        ANNOTATION_SET_NAME("annotationSetName", TEXT_ARRAY, ""),
        ANNOTATION(Constants.ANNOTATION, TEXT_ARRAY, "");

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

    default boolean exists(long sampleId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), sampleId)).getNumMatches() > 0;
    }

    default void checkId(long sampleId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (sampleId < 0) {
            throw CatalogDBException.newInstance("Sample id '{}' is not valid: ", sampleId);
        }

        if (!exists(sampleId)) {
            throw CatalogDBException.newInstance("Sample id '{}' does not exist", sampleId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> sample, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, Sample sample, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Sample> get(long sampleId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Sample> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException;

    long getStudyId(long sampleId) throws CatalogDBException;

    OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return a OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

}
