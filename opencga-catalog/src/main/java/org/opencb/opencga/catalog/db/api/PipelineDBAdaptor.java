package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.job.Pipeline;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface PipelineDBAdaptor extends CoreDBAdaptor<Pipeline> {

    default boolean exists(long pipelineId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), pipelineId)).getNumMatches() > 0;
    }

    default void checkId(long pipelineId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (pipelineId < 0) {
            throw CatalogDBException.newInstance("Pipeline id '{}' is not valid: ", pipelineId);
        }

        if (!exists(pipelineId)) {
            throw CatalogDBException.newInstance("Pipeline id '{}' does not exist", pipelineId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> pipeline, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, Pipeline pipeline, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default OpenCGAResult<Pipeline> get(long pipelineId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), pipelineId);
        OpenCGAResult<Pipeline> pipelineDataResult = get(query, options);
        if (pipelineDataResult == null || pipelineDataResult.getResults().size() == 0) {
            throw CatalogDBException.uidNotFound("Pipeline", pipelineId);
        }
        return pipelineDataResult;
    }

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID(MongoDBAdaptor.PRIVATE_UID, LONG, ""),
        UUID("uuid", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        DISABLED("disabled", BOOLEAN, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        VERSION("version", INTEGER, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        PARAMS("params", OBJECT, ""),
        CONFIG("config", OBJECT, ""),
        JOBS("jobs", OBJECT, ""),

        STUDY_UID(MongoDBAdaptor.PRIVATE_STUDY_UID, LONG, "");

        private static Map<String, QueryParams> map = new HashMap<>();

        static {
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private final Type type;
        private final String description;

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
}
