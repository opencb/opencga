package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.notes.Notes;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface NotesDBAdaptor extends DBAdaptor<Notes> {

    OpenCGAResult<Notes> insert(Notes notes)  throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    enum QueryParams implements QueryParam {
        UID("uid", LONG, ""),
        ID("id", STRING, ""),
        VERSION("version", INTEGER, ""),
        USER_ID("userId", STRING, ""),
        TAGS("tags", TEXT_ARRAY, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        VALUE_TYPE("valueType", STRING, ""),
        VALUE("value", OBJECT, "");

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
