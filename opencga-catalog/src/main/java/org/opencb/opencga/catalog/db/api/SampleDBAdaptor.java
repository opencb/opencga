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
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface SampleDBAdaptor extends AnnotationSetDBAdaptor<Sample, SampleAclEntry> {

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        SOURCE("source", TEXT_ARRAY, ""),
        INDIVIDUAL_ID("individualId", INTEGER_ARRAY, ""),
        DESCRIPTION("description", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),

        STUDY_ID("studyId", INTEGER_ARRAY, ""),

        ACL("acl", TEXT_ARRAY, ""),
        ACL_MEMBER("acl.member", TEXT_ARRAY, ""),
        ACL_PERMISSIONS("acl.permissions", TEXT_ARRAY, ""),

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        VARIABLE_SET_ID("variableSetId", INTEGER, ""),
        ANNOTATION_SET_NAME("annotationSetName", TEXT_ARRAY, ""),
        ANNOTATION("annotation", TEXT_ARRAY, "");

        /*
        ANNOTATIONS_SET_VARIABLE_SET_ID("annotationSets.variableSetId", DOUBLE, ""),
        ANNOTATION_SET_NAME("annotationSets.id", TEXT, ""),
        ANNOTATION_SET("annotationSets", TEXT_ARRAY, "");
*/
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

    default void checkId(long sampleId) throws CatalogDBException {
        if (sampleId < 0) {
            throw CatalogDBException.newInstance("Sample id '{}' is not valid: ", sampleId);
        }

        if (!exists(sampleId)) {
            throw CatalogDBException.newInstance("Sample id '{}' does not exist", sampleId);
        }
    }

    QueryResult<Sample> insert(Sample sample, long studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> get(long sampleId, QueryOptions options) throws CatalogDBException;

//    @Deprecated
//    QueryResult<Sample> getAllSamples(QueryOptions options) throws CatalogDBException;
//
//    @Deprecated
//    QueryResult<Sample> getAllSamples(Map<String, Variable> variableMap, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> update(long sampleId, QueryOptions parameters) throws CatalogDBException;

    long getStudyId(long sampleId) throws CatalogDBException;

    List<Long> getStudyIdsBySampleIds(String sampleIds) throws CatalogDBException;

    @Deprecated
    QueryResult<AnnotationSet> annotate(long sampleId, AnnotationSet annotationSet, boolean overwrite) throws CatalogDBException;

    @Deprecated
    QueryResult<AnnotationSet> deleteAnnotation(long sampleId, String annotationId) throws CatalogDBException;

    /**
     * Remove all the Acls defined for the member in the resource.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member member from whom the Acls will be removed.
     * @throws CatalogDBException if any problem occurs during the removal.
     */
    void removeAclsFromStudy(long studyId, String member) throws CatalogDBException;

}
