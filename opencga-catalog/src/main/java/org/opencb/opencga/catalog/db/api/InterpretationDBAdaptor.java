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

import org.apache.commons.collections.map.LinkedMap;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface InterpretationDBAdaptor extends CoreDBAdaptor<Interpretation> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER, ""),
        UUID("uuid", TEXT, ""),
        CLINICAL_ANALYSIS_ID("clinicalAnalysisId", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        ANALYST("analyst", TEXT, ""),
        ANALYST_ID("analyst.id", TEXT, ""),
        METHODS("methods", TEXT_ARRAY, ""),
        METHODS_NAME("methods.name", TEXT_ARRAY, ""),
        PRIMARY_FINDINGS("primaryFindings", TEXT_ARRAY, ""),
        PRIMARY_FINDINGS_ID("primaryFindings.id", TEXT_ARRAY, ""),
        SECONDARY_FINDINGS("secondaryFindings", TEXT_ARRAY, ""),
        SECONDARY_FINDINGS_ID("secondaryFindings.id", TEXT_ARRAY, ""),
        COMMENTS("comments", TEXT_ARRAY, ""),
        STATUS("status", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        VERSION("version", INTEGER, ""),
        RELEASE("release", INTEGER, ""), //  Release where the sample was created
        SNAPSHOT("snapshot", INTEGER, ""), // Last version of sample at release = snapshot,

        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""), // Alias to studyId in the database. Only for the webservices.

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, "");

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

    default boolean exists(long interpretationId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), interpretationId)).getNumMatches() > 0;
    }

    default void checkId(long interpretationId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (interpretationId < 0) {
            throw CatalogDBException.newInstance("Interpretation id '{}' is not valid: ", interpretationId);
        }

        if (!exists(interpretationId)) {
            throw CatalogDBException.newInstance("Interpretation id '{}' does not exist", interpretationId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> interpretation, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, Interpretation interpretation, ParamUtils.SaveInterpretationAs action,
                         List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult update(long uid, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, ParamUtils.SaveInterpretationAs action,
                         QueryOptions queryOptions) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Interpretation> update(long id, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Interpretation> update(Query query, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList,
                                         QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Interpretation> merge(long interpretationUid, Interpretation interpretation, List<ClinicalAudit> clinicalAuditList,
                                        List<String> clinicalVariantList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Interpretation> get(long interpretationUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Interpretation> get(long studyUid, String interpretationId, QueryOptions options) throws CatalogDBException;

    OpenCGAResult<Interpretation> delete(Interpretation interpretation, List<ClinicalAudit> clinicalAuditList) throws CatalogDBException;

    OpenCGAResult<Interpretation> delete(Query query, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyId(long interpretationId) throws CatalogDBException;

}
