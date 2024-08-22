package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface NoteDBAdaptor extends DBAdaptor<Note> {

    OpenCGAResult<Note> insert(Note note)  throws CatalogException;

    default OpenCGAResult<Note> get(long noteUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), noteUid);
        return get(query, options);
    }

    enum QueryParams implements QueryParam {
        UID("uid", LONG, ""),
        STUDY_UID("studyUid", LONG, ""),
        ID("id", STRING, ""),
        UUID("uuid", STRING, ""),
        SCOPE("scope", STRING, ""),
        STUDY("study", STRING, ""),
        TAGS("tags", TEXT_ARRAY, ""),
        USER_ID("userId", STRING, ""),
        VISIBILITY("visibility", STRING, ""),
        VERSION("version", INTEGER, ""),
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
