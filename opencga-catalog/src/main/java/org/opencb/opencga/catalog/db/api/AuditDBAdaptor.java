/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuditDBAdaptor {

    enum QueryParams implements QueryParam {
        OPERATION_ID("operationId", TEXT, ""),
        USER_ID("userId", TEXT, ""),
        ACTION("action", TEXT, ""),
        RESOURCE("resource", TEXT, ""),
        RESOURCE_ID("resourceId", TEXT, ""),
        RESOURCE_UUID("resourceUuid", TEXT, ""),
        STUDY_ID("studyId", TEXT, ""),
        STUDY_UUID("studyUuid", TEXT, ""),
        STATUS("status", OBJECT, ""),
        STATUS_NAME("status.name", TEXT, ""),
        DATE("date", Type.DATE, "");

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


    OpenCGAResult<AuditRecord> insertAuditRecord(AuditRecord auditRecord) throws CatalogDBException;

    OpenCGAResult<AuditRecord> insertAuditRecords(List<AuditRecord> auditRecords) throws CatalogDBException;

    OpenCGAResult<AuditRecord> get(Query query, QueryOptions queryOptions) throws CatalogDBException;

    OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException;

}
