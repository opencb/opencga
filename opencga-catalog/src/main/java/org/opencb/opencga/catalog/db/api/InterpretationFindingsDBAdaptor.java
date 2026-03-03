package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface InterpretationFindingsDBAdaptor {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        VERSION("version", INTEGER, ""),
        INTERPRETATION_ID("interpretationId", TEXT, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, "");

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

    OpenCGAResult<ClinicalVariant> get(Query query, QueryOptions options);

//    default boolean exists(long interpretationId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
//        return count(new Query(QueryParams.UID.key(), interpretationId)).getNumMatches() > 0;
//    }
//
//    default void checkId(long interpretationId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
//        if (interpretationId < 0) {
//            throw CatalogDBException.newInstance("Interpretation id '{}' is not valid: ", interpretationId);
//        }
//
//        if (!exists(interpretationId)) {
//            throw CatalogDBException.newInstance("Interpretation id '{}' does not exist", interpretationId);
//        }
//    }
//
//    OpenCGAResult nativeInsert(Map<String, Object> interpretation, String userId) throws CatalogDBException;
//
//    OpenCGAResult insert(long studyId, Interpretation interpretation, ParamUtils.SaveInterpretationAs action,
//                         List<ClinicalAudit> clinicalAuditList) throws CatalogException;
//
//    OpenCGAResult update(long uid, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, ParamUtils.SaveInterpretationAs action,
//                         QueryOptions queryOptions) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
//
//    OpenCGAResult<Interpretation> update(long id, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
//            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
//
//    OpenCGAResult<Interpretation> update(Query query, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList,
//                                         QueryOptions queryOptions)
//            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
//
//    OpenCGAResult<Interpretation> revert(long id, int previousVersion, List<ClinicalAudit> clinicalAuditList)
//            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
//
//    OpenCGAResult<Interpretation> get(long interpretationUid, QueryOptions options)
//            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
//
//    OpenCGAResult<Interpretation> get(long studyUid, String interpretationId, QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult<Interpretation> delete(Interpretation interpretation, List<ClinicalAudit> clinicalAuditList) throws CatalogDBException;
//
//    OpenCGAResult<Interpretation> delete(Query query, List<ClinicalAudit> clinicalAuditList)
//            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
//
//    long getStudyId(long interpretationId) throws CatalogDBException;
//
//    OpenCGAResult updateProjectRelease(long studyId, int release)
//            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

}
