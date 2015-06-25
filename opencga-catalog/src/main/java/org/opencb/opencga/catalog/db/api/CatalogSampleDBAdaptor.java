package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogSampleDBAdaptor {

    enum SampleFilterOption implements CatalogDBAdaptor.FilterOption {
//        studyId(Type.NUMERICAL, ""),
        annotationSetId(Type.TEXT, ""),
        variableSetId(Type.NUMERICAL, ""),

        annotation(Type.TEXT, "Format: [<VariableId>:[<operator><value>,]+;]+  -> ID:3,4,5;AGE:>30;NAME:Luke,Leia,Vader"),

        id(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        source(Type.TEXT, ""),
        individualId(Type.NUMERICAL, ""),

        attributes(Type.TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        nattributes("attributes", Type.NUMERICAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        battributes("attributes", Type.BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),

        ;

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
     * Samples methods
     * ***************************
     */

    boolean sampleExists(int sampleId);

    QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getAllSamples(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getAllSamples(int studyId, Map<String, Variable> variableMap, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<Integer> deleteSample(int sampleId) throws CatalogDBException;

    int getStudyIdBySampleId(int sampleId) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet) throws CatalogDBException;

    /**
     * Cohort methods
     * ***************************
     */

    QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException;

    QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException;

    QueryResult<Cohort> updateCohort(int cohortId, ObjectMap parameters) throws CatalogDBException;

    int getStudyIdByCohortId(int cohortId) throws CatalogDBException;

    /**
     * VariableSet Methods
     * ***************************
     */

    QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException;

    QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException;

    QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions) throws CatalogDBException;

    int getStudyIdByVariableSetId(int sampleId) throws CatalogDBException;

}
