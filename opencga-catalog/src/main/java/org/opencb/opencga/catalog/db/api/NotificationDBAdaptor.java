package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.notification.Notification;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface NotificationDBAdaptor extends CoreDBAdaptor<Notification> {

    OpenCGAResult<Notification> insert(long studyUid, List<Notification> notificationList, QueryOptions options) throws CatalogException;

    OpenCGAResult<Notification> update(long studyUid, String notificationUuid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", LONG, ""),
        UUID("uuid", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_ID("internal.status.id", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        INTERNAL_NOTIFICATIONS("internal.notifications", TEXT_ARRAY, ""),
        TARGET("target", TEXT, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, "");

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

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
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
    }
}
