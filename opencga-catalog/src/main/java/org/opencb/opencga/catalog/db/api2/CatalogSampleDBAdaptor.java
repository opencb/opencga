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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;

import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogSampleDBAdaptor {

    /**
     * Samples methods
     * ***************************
     */

    boolean sampleExists(int sampleId);

    QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getAllSamples(QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getAllSamples(Map<String, Variable> variableMap, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<AclEntry> getSampleAcl(int sampleId, String userId) throws CatalogDBException;

    QueryResult<Map<String, AclEntry>> getSampleAcl(int sampleId, List<String> userIds) throws CatalogDBException;

    QueryResult<AclEntry> setSampleAcl(int sampleId, AclEntry acl) throws CatalogDBException;

    QueryResult<AclEntry> unsetSampleAcl(int sampleId, String userId) throws CatalogDBException;

    QueryResult<Sample> deleteSample(int sampleId) throws CatalogDBException;

    int getStudyIdBySampleId(int sampleId) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet, boolean overwrite) throws CatalogDBException;

    QueryResult<AnnotationSet> deleteAnnotation(int sampleId, String annotationId) throws CatalogDBException;

    /**
     * Cohort methods
     * ***************************
     */

    QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException;

    QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException;

    QueryResult<Cohort> getAllCohorts(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Cohort> modifyCohort(int cohortId, ObjectMap parameters) throws CatalogDBException;

    QueryResult<Cohort> deleteCohort(int cohortId, ObjectMap queryOptions) throws CatalogDBException;

    int getStudyIdByCohortId(int cohortId) throws CatalogDBException;

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
        battributes("attributes", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),;

        final private String _key;
        final private String _description;
        final private Type _type;
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
        bstats("stats", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),;


        final private String _key;
        final private String _description;
        final private Type _type;
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

    enum VariableSetFilterOption implements AbstractCatalogDBAdaptor.FilterOption {
        studyId(Type.NUMERICAL, ""),

        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        description(Type.TEXT, ""),
        attributes(Type.TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        nattributes("attributes", Type.NUMERICAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        battributes("attributes", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),;

        final private String _key;
        final private String _description;
        final private Type _type;
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
