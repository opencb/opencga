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

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.DiseasePanel;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 01/06/16.
 */
public interface PanelDBAdaptor extends DBAdaptor<DiseasePanel> {

    enum QueryParams implements QueryParam {
        ID("id", INTEGER, ""),
        NAME("name", TEXT, ""),
        DISEASE("disease", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),

        GENES("genes", TEXT_ARRAY, ""),
        REGIONS("regions", TEXT_ARRAY, ""),
        VARIANTS("variants", TEXT_ARRAY, ""),

        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),

        STUDY_ID("studyId", INTEGER_ARRAY, "");

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

    default boolean exists(long panelId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), panelId)).first() > 0;
    }

    default void checkId(long panelId) throws CatalogDBException {
        if (panelId < 0) {
            throw CatalogDBException.newInstance("Panel id '{}' is not valid: ", panelId);
        }

        if (!exists(panelId)) {
            throw CatalogDBException.newInstance("Panel id '{}' does not exist", panelId);
        }
    }

    QueryResult<DiseasePanel> insert(DiseasePanel diseasePanel, long studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<DiseasePanel> get(long diseasePanelId, QueryOptions options) throws CatalogDBException;

    long getStudyId(long panelId) throws CatalogDBException;

}
