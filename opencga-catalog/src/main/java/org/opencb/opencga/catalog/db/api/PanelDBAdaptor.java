/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.db.api;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.Panel;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;


public interface PanelDBAdaptor extends DBAdaptor<Panel> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        NAME("name", TEXT, ""),
        VERSION("version", INTEGER, ""),
        DESCRIPTION("description", TEXT, ""),

        VARIANTS("variants", TEXT_ARRAY, ""),
        GENES("genes", TEXT_ARRAY, ""),
        REGIONS("regions", TEXT_ARRAY, ""),

        AUTHOR("author", TEXT, ""),
        STATUS("status", TEXT, ""),

        UID("uid", INTEGER, ""),
        STUDY_ID("studyId", INTEGER_ARRAY, ""),
        STUDY_UID("studyUid", INTEGER_ARRAY, "");

        private static Map<String, QueryParams> map;

        static {
            map = new HashMap<>();
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

    default boolean exists(String id) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), id)).first() > 0;
    }

    default void checkId(String id) throws CatalogDBException {
        if (StringUtils.isEmpty(id)) {
            throw CatalogDBException.newInstance("Panel id '{}' is not valid: ", id);
        }

        if (!exists(id)) {
            throw CatalogDBException.newInstance("Panel id '{}' does not exist", id);
        }
    }

    QueryResult<Panel> insert(long studyId, Panel panel, QueryOptions options) throws CatalogDBException;

    QueryResult<Panel> get(long panelId, QueryOptions options) throws CatalogDBException;

    long getStudyId(long panelId) throws CatalogDBException;

}
