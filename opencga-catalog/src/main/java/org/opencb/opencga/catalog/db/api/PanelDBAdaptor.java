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

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;


public interface PanelDBAdaptor extends CoreDBAdaptor<Panel> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER, ""),
        UUID("uuid", TEXT, ""),
        NAME("name", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),

        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        RELEASE("release", INTEGER, ""), //  Release where the sample was created
        SNAPSHOT("snapshot", INTEGER, ""), // Last version of sample at release = snapshot
        VERSION("version", INTEGER, ""), // Version of the sample
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),

        SOURCE("source", TEXT_ARRAY, ""),

        STATS("stats", TEXT_ARRAY, ""),

        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        TAGS("tags", TEXT_ARRAY, ""),
        CATEGORIES("categories", TEXT_ARRAY, ""),
        CATEGORIES_NAME("categories.name", TEXT_ARRAY, ""),

        DISORDERS("disorders", TEXT_ARRAY, ""),
        DISORDERS_ID("disorders.id", TEXT, ""),
        DISORDERS_NAME("disorders.name", TEXT, ""),
        DISORDERS_SOURCE("disorders.source", TEXT, ""),

        VARIANTS("variants", TEXT_ARRAY, ""),
        VARIANTS_ID("variants.id", TEXT, ""),
        VARIANTS_PHENOTYPE("variants.phenotype", TEXT, ""),

        GENES("genes", TEXT_ARRAY, ""),
        GENES_ID("genes.id", TEXT, ""),
        GENES_NAME("genes.name", TEXT, ""),
        GENES_CONFIDENCE("genes.confidence", TEXT, ""),

        REGIONS("regions", TEXT_ARRAY, ""),
        REGIONS_ID("regions.id", TEXT, ""),
        REGIONS_SCORE("regions.score", DOUBLE, ""),

        SOURCE_ID("source.id", TEXT, ""),
        SOURCE_NAME("source.name", TEXT, ""),
        SOURCE_VERSION("source.version", TEXT, ""),
        SOURCE_AUTHOR("source.author", TEXT, ""),
        SOURCE_PROJECT("source.project", TEXT, ""),

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

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

    default boolean exists(long panelUid) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), panelUid)).getNumMatches() > 0;
    }

    default void checkUid(long panelUid) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (panelUid < 0) {
            throw CatalogDBException.newInstance("Panel uid '{}' is not valid: ", panelUid);
        }

        if (!exists(panelUid)) {
            throw CatalogDBException.newInstance("Panel uid '{}' does not exist", panelUid);
        }
    }

    OpenCGAResult insert(long studyUid, List<Panel> panelList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult insert(long studyId, Panel panel, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Panel> get(long panelId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyId(long panelId) throws CatalogDBException;

    OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId          study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

}
