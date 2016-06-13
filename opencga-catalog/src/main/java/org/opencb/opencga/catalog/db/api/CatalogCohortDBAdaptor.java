package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections.map.LinkedMap;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.acls.CohortAcl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 3/22/16.
 */
public interface CatalogCohortDBAdaptor extends CatalogDBAdaptor<Cohort> {

    enum QueryParams implements QueryParam {
        ID("id", DECIMAL, ""),
        NAME("name", TEXT, ""),
        TYPE("type", TEXT, ""),
        CREATION_DATE("creationDate", TEXT, ""),
        STATUS_STATUS("status.status", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),

        ACLS("acls", TEXT_ARRAY, ""),
        ACLS_USERS("acls.users", TEXT_ARRAY, ""),
        ACLS_PERMISSIONS("acls.permissions", TEXT_ARRAY, ""),
        SAMPLES("samples", DECIMAL, ""),

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        VARIABLE_SET_ID("variableSetId", INTEGER, ""),
        ANNOTATION_SET_ID("annotationSetId", TEXT_ARRAY, ""),
        ANNOTATION("annotation", TEXT_ARRAY, ""),

        ATTRIBUTES("attributes", TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        NATTRIBUTES("nattributes", DECIMAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        BATTRIBUTES("battributes", BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),

        STATS("stats", TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        NSTATS("nstats", DECIMAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        BSTATS("bstats", BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),

        STUDY_ID("studyId", DECIMAL, "");

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

    default boolean cohortExists(long cohortId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), cohortId)).first() > 0;
    }

    default void checkCohortId(long cohortId) throws CatalogDBException {
        if (cohortId < 0) {
            throw CatalogDBException.newInstance("Cohort id '{}' is not valid: ", cohortId);
        }

        if (!cohortExists(cohortId)) {
            throw CatalogDBException.newInstance("Cohort id '{}' does not exist", cohortId);
        }
    }

//    @Override
//    default QueryResult<Long> restore(Query query) throws CatalogDBException {
//        updateStatus();
//    }

    QueryResult<Cohort> createCohort(long studyId, Cohort cohort, QueryOptions options) throws CatalogDBException;

    QueryResult<Cohort> getCohort(long cohortId, QueryOptions options) throws CatalogDBException;

    QueryResult<Cohort> getAllCohorts(long studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Cohort> modifyCohort(long cohortId, ObjectMap parameters, QueryOptions options) throws CatalogDBException;

    QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions queryOptions) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateCohort(long cohortId, AnnotationSet annotationSet, boolean overwrite) throws CatalogDBException;

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

    QueryResult<AnnotationSet> deleteAnnotation(long cohortId, String annotationId) throws CatalogDBException;

    default QueryResult<CohortAcl> getCohortAcl(long cohortId, String member) throws CatalogDBException {
        return getCohortAcl(cohortId, Arrays.asList(member));
    }

    QueryResult<CohortAcl> getCohortAcl(long cohortId, List<String> members) throws CatalogDBException;

    QueryResult<CohortAcl> setCohortAcl(long cohortId, CohortAcl acl, boolean override) throws CatalogDBException;

    void unsetCohortAcl(long cohortId, List<String> members, List<String> permissions) throws CatalogDBException;

    void unsetCohortAclsInStudy(long studyId, List<String> members) throws CatalogDBException;

    long getStudyIdByCohortId(long cohortId) throws CatalogDBException;
}
