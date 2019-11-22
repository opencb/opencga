package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.Task;
import org.opencb.opencga.core.results.OpenCGAResult;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface TaskDBAdaptor extends DBAdaptor<Task>  {

    default boolean exists(long taskUid) throws CatalogDBException {
        return count(new Query(QueryParams.UID.key(), taskUid)).first() > 0;
    }

    default void checkId(long taskUid) throws CatalogDBException {
        if (taskUid < 0) {
            throw CatalogDBException.newInstance("Task uid '{}' is not valid: ", taskUid);
        }

        if (!exists(taskUid)) {
            throw CatalogDBException.newInstance("Task uid '{}' does not exist", taskUid);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> task) throws CatalogDBException;

    OpenCGAResult<Task> insert(long studyId, Task task, QueryOptions options) throws CatalogDBException;

    default OpenCGAResult<Task> get(long taskUid, QueryOptions options) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), taskUid);
        OpenCGAResult<Task> taskDataResult = get(query, options);
        if (taskDataResult == null || taskDataResult.getResults().size() == 0) {
            throw CatalogDBException.uidNotFound("Task", taskUid);
        }
        return taskDataResult;
    }

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER_ARRAY, ""),
        UUID("uuid", TEXT, ""),
        USER_ID("userId", TEXT_ARRAY, ""),
        COMMAND_LINE("commandLine", TEXT_ARRAY, ""),
        ACTION("action", TEXT, ""),
        RESOURCE("resource", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        RESOURCE_MANAGER_ATTRIBUTES("resourceManagerAttributes", TEXT_ARRAY, ""),
        PARAMS("params", TEXT_ARRAY, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]",

        PRIORITY("priority", TEXT, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""); // Alias to studyId in the database. Only for the webservices.

        private static Map<String, QueryParams> map = new HashMap<>();
        static {
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

}
