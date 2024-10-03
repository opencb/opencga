package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.core.events.OpencgaEvent;
import org.opencb.opencga.core.events.OpencgaProcessedEvent;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER;
import static org.opencb.commons.datastore.core.QueryParam.Type.STRING;

public interface EventDBAdaptor {

    enum QueryParams implements QueryParam {
        ID("id", STRING, ""),
        VERSION("version", STRING, ""),
        START("date", QueryParam.Type.DATE, ""),
        END("date", QueryParam.Type.DATE, ""),
        PATCH("patch", INTEGER, ""),
        STATUS("status", STRING, "");

        private static Map<String, MigrationDBAdaptor.QueryParams> map = new HashMap<>();
        static {
            for (MigrationDBAdaptor.QueryParams params : MigrationDBAdaptor.QueryParams.values()) {
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

        public static Map<String, MigrationDBAdaptor.QueryParams> getMap() {
            return map;
        }

        public static MigrationDBAdaptor.QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    enum Status {
        SUCCESS,
        ERROR
    }

    void insert(OpencgaProcessedEvent event);

    void addSubscriber(OpencgaEvent event, String subscriber, Status status);

    void finishEvent(OpencgaEvent opencgaEvent);

}
