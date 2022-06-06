package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface ExecutionDBAdaptor extends CoreDBAdaptor<Execution> {

    default boolean exists(long executionId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), executionId)).getNumMatches() > 0;
    }

    default void checkId(long executionId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (executionId < 0) {
            throw CatalogDBException.newInstance("Execution id '{}' is not valid: ", executionId);
        }

        if (!exists(executionId)) {
            throw CatalogDBException.newInstance("Execution id '{}' does not exist", executionId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> execution, String userId) throws CatalogDBException;

    OpenCGAResult<Execution> insert(long studyId, Execution execution, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default OpenCGAResult<Execution> get(long executionId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), executionId);
        OpenCGAResult<Execution> executionDataResult = get(query, options);
        if (executionDataResult == null || executionDataResult.getResults().size() == 0) {
            throw CatalogDBException.uidNotFound("Execution", executionId);
        }
        return executionDataResult;
    }

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId          study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID(MongoDBAdaptor.PRIVATE_UID, LONG, ""),
        UUID("uuid", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        USER_ID("userId", TEXT, ""),
        TOOL_ID("toolId", TEXT, ""),
        PARAMS("params", MAP, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),

        VISITED("visited", BOOLEAN, ""),

        PIPELINE("pipeline", OBJECT, ""),
        PIPELINE_ID("pipeline.id", TEXT, ""),
        IS_PIPELINE("isPipeline", BOOLEAN, ""),

        PRIORITY("priority", TEXT, ""),

        INTERNAL("internal", OBJECT, ""),
        INTERNAL_STATUS("internal.status", OBJECT, ""),
        INTERNAL_STATUS_ID("internal.status.id", TEXT, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DESCRIPTION("internal.status.description", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        INTERNAL_WEBHOOK("internal.webhook", OBJECT, ""),
        INTERNAL_EVENTS("internal.events", OBJECT, ""),
        OUT_DIR("outDir", OBJECT, ""),

        INPUT("input", OBJECT, ""),
        OUTPUT("output", OBJECT, ""),
        DEPENDS_ON("dependsOn", TEXT_ARRAY, ""),
        TAGS("tags", TEXT_ARRAY, ""),
        JOBS("jobs", OBJECT, ""),

        STDOUT("stdout", OBJECT, ""),
        STDERR("stderr", OBJECT, ""),

        RELEASE("release", INTEGER, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"

        STUDY("study", OBJECT, ""),
        STUDY_ID("study.id", TEXT, ""),
        STUDY_OTHERS("study.others", TEXT, ""),
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
