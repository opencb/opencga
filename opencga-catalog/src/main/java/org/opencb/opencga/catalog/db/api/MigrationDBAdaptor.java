package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.migration.MigrationRun;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER;
import static org.opencb.commons.datastore.core.QueryParam.Type.STRING;

public interface MigrationDBAdaptor {

    enum QueryParams implements QueryParam {
        ID("id", STRING, ""),
        DATE("date", QueryParam.Type.DATE, ""),
        PATCH("patch", INTEGER, ""),
        STATUS("status", STRING, "");

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

    void insert(MigrationRun migrationRun) throws CatalogDBException;

    OpenCGAResult<MigrationRun> get(Query query) throws CatalogDBException;

    OpenCGAResult<MigrationRun> get(List<String> migrationRunIds) throws CatalogDBException;

    default OpenCGAResult<MigrationRun> get(String migrationRunId) throws CatalogDBException{
        return get(Collections.singletonList(migrationRunId));
    }

    void update(String migrationRunId, MigrationRun migrationRun) throws CatalogDBException;

    void delete(String migrationRunId) throws CatalogDBException;

}
