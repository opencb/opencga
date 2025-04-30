package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.notification.Notification;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface NotificationDBAdaptor extends DBAdaptor<Notification> {

    OpenCGAResult<Notification> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    DBIterator<Notification> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    OpenCGAResult<Notification> insert(List<Notification> notificationList, QueryOptions options) throws CatalogException;

    OpenCGAResult<Notification> update(String notificationUuid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Long> count(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Notification> groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    OpenCGAResult<?> distinct(String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<?> distinct(List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<FacetField> facet(Query query, String facet, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    enum QueryParams implements QueryParam {
        UID("uid", LONG, ""),
        UUID("uuid", TEXT, ""),
        TYPE("type", TEXT, ""),
        SCOPE("scope", TEXT, ""),
        SENDER("sender", TEXT, ""),
        RECEIVER("receiver", TEXT, ""),
        TARGET("target", TEXT, ""),
        FQN("fqn", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_ID("internal.status.id", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        INTERNAL_NOTIFICATOR_STATUSES("internal.notificatorStatuses", TEXT_ARRAY, ""),
        INTERNAL_VISITED("internal.visited", BOOLEAN, "");

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
