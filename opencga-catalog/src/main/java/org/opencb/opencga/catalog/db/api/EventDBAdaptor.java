package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface EventDBAdaptor {

    enum QueryParams implements QueryParam {
        UID("uid", LONG, ""),
        UUID("uuid", STRING, ""),
        ID("id", STRING, ""),
        SUBSCRIBERS("subscribers", OBJECT, ""),
        SUCCESSFUL("successful", BOOLEAN, ""),
        CREATION_DATE("creationDate", STRING, ""),
        MODIFICATION_DATE("modificationDate", STRING, ""),
        EVENT_STUDY_FQN("event.studyFqn", STRING, "");

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

    enum SubscribersQueryParams implements QueryParam {
        ID("id", STRING, ""),
        SUCCESSFUL("successful", BOOLEAN, ""),
        NUM_ATTEMPTS("numAttempts", INTEGER, "");

        private static Map<String, SubscribersQueryParams> map = new HashMap<>();
        static {
            for (SubscribersQueryParams params : SubscribersQueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        SubscribersQueryParams(String key, Type type, String description) {
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

        public static Map<String, SubscribersQueryParams> getMap() {
            return map;
        }

        public static SubscribersQueryParams getParam(String key) {
            return map.get(key);
        }
    }

    OpenCGAResult<CatalogEvent> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    void insert(CatalogEvent event) throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException;

    void updateSubscriber(CatalogEvent event, Enums.Resource resource, boolean successful)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    void finishEvent(CatalogEvent opencgaEvent) throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException;

    void archiveEvent(CatalogEvent opencgaEvent) throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException;

}
