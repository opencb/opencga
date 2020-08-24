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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 05/06/17.
 */
public interface ClinicalAnalysisDBAdaptor extends DBAdaptor<ClinicalAnalysis> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER, ""),
        UUID("uuid", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        DUE_DATE("dueDate", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        DISORDER("disorder", TEXT_ARRAY, ""),
        TYPE("type", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        STATUS_DESCRIPTION("status.description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        CONSENT("consent", TEXT_ARRAY, ""),
        PRIORITY("priority", TEXT, ""),
        ANALYST("analyst", TEXT_ARRAY, ""),
        ANALYST_ASSIGNEE("analyst.assignee", TEXT, ""),
        ANALYST_ASSIGNED_BY("analyst.assignedBy", TEXT, ""),
        FLAGS("flags", TEXT_ARRAY, ""),
        RELEASE("release", INTEGER, ""),

        SAMPLE("sample", TEXT_ARRAY, ""), // Alias to search for samples within proband.samples or family.members.samples
        MEMBER("member", TEXT_ARRAY, ""), // Alias to search for members from proband or family.members

        FAMILY("family", TEXT_ARRAY, ""),
        FAMILY_ID("family.id", TEXT, ""),
        FAMILY_UID("family.uid", INTEGER, ""),
        FAMILY_MEMBERS_UID("family.members.uid", INTEGER_ARRAY, ""),
        FAMILY_MEMBERS_SAMPLES_UID("family.members.samples.uid", INTEGER_ARRAY, ""),
        FILES("files", TEXT_ARRAY, ""),
        COMMENTS("comments", TEXT_ARRAY, ""),
        ALERTS("alerts", TEXT_ARRAY, ""),
        PROBAND("proband", TEXT_ARRAY, ""),
        PROBAND_ID("proband.id", TEXT, ""),
        PROBAND_UID("proband.uid", INTEGER, ""),
        PROBAND_SAMPLES_ID("proband.samples.id", TEXT_ARRAY, ""),
        PROBAND_SAMPLES_UID("proband.samples.uid", INTEGER, ""),
        INTERPRETATION("interpretation", TEXT, ""),
        INTERPRETATION_ID("interpretation.id", TEXT, ""),
        SECONDARY_INTERPRETATIONS("secondaryInterpretations", TEXT_ARRAY, ""),
        SECONDARY_INTERPRETATIONS_ID("secondaryInterpretations.id", TEXT_ARRAY, ""),

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""), // Alias to studyId in the database. Only for the webservices.

        ACL("acl", TEXT_ARRAY, ""),
        ACL_MEMBER("acl.member", TEXT_ARRAY, ""),
        ACL_PERMISSIONS("acl.permissions", TEXT_ARRAY, "");

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

    default boolean exists(long clinicalAnalysisId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), clinicalAnalysisId)).getNumMatches() > 0;
    }

    default void checkId(long clinicalAnalysisId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (clinicalAnalysisId < 0) {
            throw CatalogDBException.newInstance("Clinical analysis id '{}' is not valid: ", clinicalAnalysisId);
        }

        if (!exists(clinicalAnalysisId)) {
            throw CatalogDBException.newInstance("Clinical analysis id '{}' does not exist", clinicalAnalysisId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> clinicalAnalysis, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, ClinicalAnalysis clinicalAnalysis, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<ClinicalAnalysis> get(long clinicalAnalysisUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<ClinicalAnalysis> get(long studyUid, String clinicalAnalysisId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyId(long clinicalAnalysisId) throws CatalogDBException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

}
