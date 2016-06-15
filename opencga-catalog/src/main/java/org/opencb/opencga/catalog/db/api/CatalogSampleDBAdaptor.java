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
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.acls.SampleAcl;

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogSampleDBAdaptor extends CatalogDBAdaptor<Sample> {

    default boolean sampleExists(long sampleId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), sampleId)).first() > 0;
    }

    default void checkSampleId(long sampleId) throws CatalogDBException {
        if (sampleId < 0) {
            throw CatalogDBException.newInstance("Sample id '{}' is not valid: ", sampleId);
        }

        if (!sampleExists(sampleId)) {
            throw CatalogDBException.newInstance("Sample id '{}' does not exist", sampleId);
        }
    }

    QueryResult<Sample> createSample(long studyId, Sample sample, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getSample(long sampleId, QueryOptions options) throws CatalogDBException;

//    @Deprecated
//    QueryResult<Sample> getAllSamples(QueryOptions options) throws CatalogDBException;
//
//    @Deprecated
//    QueryResult<Sample> getAllSamples(Map<String, Variable> variableMap, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getAllSamplesInStudy(long studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> modifySample(long sampleId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<SampleAcl> getSampleAcl(long sampleId, String userId) throws CatalogDBException;

    QueryResult<SampleAcl> getSampleAcl(long sampleId, List<String> members) throws CatalogDBException;

    @Deprecated
    QueryResult<AclEntry> setSampleAcl(long sampleId, AclEntry acl) throws CatalogDBException;

    QueryResult<SampleAcl> setSampleAcl(long sampleId, SampleAcl acl, boolean override) throws CatalogDBException;

    @Deprecated
    QueryResult<AclEntry> unsetSampleAcl(long sampleId, String userId) throws CatalogDBException;

    void unsetSampleAcl(long sampleId, List<String> members, List<String> permissions) throws CatalogDBException;

    void unsetSampleAclsInStudy(long studyId, List<String> members) throws CatalogDBException;

    @Deprecated
    default QueryResult<Sample> deleteSample(long sampleId) throws CatalogDBException {
        return delete(sampleId, new QueryOptions());
        /*
        // TODO check that the sample is not in use!

        Query query = new Query(CatalogStudyDBAdaptor.QueryParams.ID.key(), sampleId);
        QueryResult<Sample> sampleQueryResult = get(query, new QueryOptions());
        if (sampleQueryResult.getResult().size() == 1) {
            QueryResult<Long> delete = delete(query);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Sample id '{}' has not been deleted", sampleId);
            }
        } else {
            throw CatalogDBException.newInstance("Sample id '{}' does not exist", sampleId);
        }
        return sampleQueryResult;*/
    }

    long getStudyIdBySampleId(long sampleId) throws CatalogDBException;

    List<Long> getStudyIdsBySampleIds(String sampleIds) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateSample(long sampleId, AnnotationSet annotationSet, boolean overwrite) throws CatalogDBException;

    QueryResult<Long> addVariableToAnnotations(long variableSetId, Variable variable) throws CatalogDBException;

    /**
     * This method will rename the id of all the annotations corresponding to the variableSetId changing oldName per newName.
     * This method cannot be called by any of the managers and will be only called when the user wants to rename the field of a variable
     * from a variableSet.
     * @param variableSetId Id of the variable to be renamed.
     * @param oldName Name of the field to be renamed.
     * @param newName New name that will be set.
     * @return a QueryResult containing the number of annotations that have been changed.
     * @throws CatalogDBException when there is an error with database transactions.
     */
    QueryResult<Long> renameAnnotationField(long variableSetId, String oldName, String newName) throws CatalogDBException;

    QueryResult<Long> removeAnnotationField(long variableSetId, String fieldId) throws CatalogDBException;

    QueryResult<AnnotationSet> deleteAnnotation(long sampleId, String annotationId) throws CatalogDBException;

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        SOURCE("source", TEXT_ARRAY, ""),
        INDIVIDUAL_ID("individualId", INTEGER_ARRAY, ""),
        DESCRIPTION("description", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATUS_STATUS("status.status", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),

        STUDY_ID("studyId", INTEGER_ARRAY, ""),

        ACLS("acls", TEXT_ARRAY, ""),
        ACLS_USERS("acls.users", TEXT_ARRAY, ""),
        ACLS_PERMISSIONS("acls.permissions", TEXT_ARRAY, ""),
        @Deprecated
        ACL_USER_ID("acls.userId", TEXT_ARRAY, ""),
        @Deprecated
        ACL_READ("acls.read", BOOLEAN, ""),
        @Deprecated
        ACL_WRITE("acls.write", BOOLEAN, ""),
        @Deprecated
        ACL_EXECUTE("acls.execute", BOOLEAN, ""),
        @Deprecated
        ACL_DELETE("acls.delete", BOOLEAN, ""),

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        VARIABLE_SET_ID("variableSetId", INTEGER, ""),
        ANNOTATION_SET_ID("annotationSetId", TEXT_ARRAY, ""),
        ANNOTATION("annotation", TEXT_ARRAY, "");

        /*
        ANNOTATIONS_SET_VARIABLE_SET_ID("annotationSets.variableSetId", DOUBLE, ""),
        ANNOTATION_SET_ID("annotationSets.id", TEXT, ""),
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

    enum AnnotationSetParams implements QueryParam {
        ID("id", TEXT, ""),
        VARIABLE_SET_ID("variableSetId", DOUBLE, ""),
        ANNOTATIONS("annotations", TEXT_ARRAY, ""),
        ANNOTATIONS_ID("annotations.id", TEXT, ""),
        ANNOTATIONS_VALUE("annotations.value", TEXT, ""), // We don't really know the type. It is defined  in VariableSet.
        DATE("date", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        NATTRIBUTES("nattributes", DECIMAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        BATTRIBUTES("battributes", BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]");

        private static Map<String, AnnotationSetParams> map;
        static {
            map = new LinkedMap();
            for (AnnotationSetParams params : AnnotationSetParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        AnnotationSetParams(String key, Type type, String description) {
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

        public static Map<String, AnnotationSetParams> getMap() {
            return map;
        }

        public static AnnotationSetParams getParam(String key) {
            return map.get(key);
        }
    }

    //FIXME: This should be ported to CatalogStudyDBAdaptor
    @Deprecated
    enum VariableSetParams implements QueryParam {
        ID("id", DECIMAL, ""),
        NAME("name", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        NATTRIBUTES("nattributes", DECIMAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        BATTRIBUTES("battributes", BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),
        STUDY_ID("studyId", DECIMAL, "");

        private static Map<String, VariableSetParams> map;
        static {
            map = new LinkedMap();
            for (VariableSetParams params : VariableSetParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        VariableSetParams(String key, Type type, String description) {
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

        public static Map<String, VariableSetParams> getMap() {
            return map;
        }

        public static VariableSetParams getParam(String key) {
            return map.get(key);
        }
    }

    @Deprecated
    enum SampleFilterOption implements AbstractCatalogDBAdaptor.FilterOption {
        studyId(Type.NUMERICAL, ""),
        annotationSetId(Type.TEXT, ""),
        variableSetId(Type.NUMERICAL, ""),

        annotation(Type.TEXT, "Format: [<VariableId>:[<operator><value>,]+;]+  -> ID:3,4,5;AGE:>30;NAME:Luke,Leia,Vader"),

        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        source(Type.TEXT, ""),
        individualId(Type.NUMERICAL, ""),

        acl(Type.TEXT, ""),
        bacl("acl", Type.BOOLEAN, ""),

        attributes(Type.TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        nattributes("attributes", Type.NUMERICAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        battributes("attributes", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]");

        private final String _key;
        private final String _description;
        private final Type _type;

        SampleFilterOption(Type type, String description) {
            this._key = name();
            this._description = description;
            this._type = type;
        }

        SampleFilterOption(String key, Type type, String description) {
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

    @Deprecated
    enum CohortFilterOption implements AbstractCatalogDBAdaptor.FilterOption {
        studyId(Type.NUMERICAL, ""),

        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        type(Type.TEXT, ""),
        status(Type.TEXT, ""),
        creationDate(Type.TEXT, ""),
        description(Type.TEXT, ""),

        samples(Type.NUMERICAL, ""),

        attributes(Type.TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        nattributes("attributes", Type.NUMERICAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        battributes("attributes", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),

        stats(Type.TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        nstats("stats", Type.NUMERICAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        bstats("stats", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]");


        private final String _key;
        private final String _description;
        private final Type _type;

        CohortFilterOption(Type type, String description) {
            this._key = name();
            this._description = description;
            this._type = type;
        }

        CohortFilterOption(String key, Type type, String description) {
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

    @Deprecated
    enum VariableSetFilterOption implements AbstractCatalogDBAdaptor.FilterOption {
        studyId(Type.NUMERICAL, ""),

        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        description(Type.TEXT, ""),
        attributes(Type.TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        nattributes("attributes", Type.NUMERICAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        battributes("attributes", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]");

        private final String _key;
        private final String _description;
        private final Type _type;

        VariableSetFilterOption(Type type, String description) {
            this._key = name();
            this._description = description;
            this._type = type;
        }

        VariableSetFilterOption(String key, Type type, String description) {
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
