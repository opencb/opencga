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
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.commons.datastore.core.ObjectMap;
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

import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 05/06/17.
 */
public interface ClinicalAnalysisDBAdaptor extends CoreDBAdaptor<ClinicalAnalysis> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER, ""),
        UUID("uuid", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        DUE_DATE("dueDate", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        DISORDER("disorder", OBJECT, ""),
        DISORDER_ID("disorder.id", TEXT, ""),
        DISORDER_NAME("disorder.name", TEXT, ""),
        TYPE("type", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATUS("status", OBJECT, ""),
        STATUS_ID("status.id", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        STATUS_DESCRIPTION("status.description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        QUALITY_CONTROL("qualityControl", OBJECT, ""),
        QUALITY_CONTROL_SUMMARY("qualityControl.summary", TEXT, ""),
        CONSENT("consent", OBJECT, ""),
        PRIORITY("priority", OBJECT, ""),
        PRIORITY_ID("priority.id", TEXT, ""),
        ANALYST("analyst", TEXT_ARRAY, ""),
        ANALYST_ID("analyst.id", TEXT, ""),
        ANALYST_ASSIGNED_BY("analyst.assignedBy", TEXT, ""),
        REPORT("report", OBJECT, ""),
        FLAGS("flags", OBJECT, ""),
        FLAGS_ID("flags.id", TEXT, ""),
        RELEASE("release", INTEGER, ""),
        PANEL_LOCK("panelLock", BOOLEAN, ""),
        LOCKED("locked", BOOLEAN, ""),

        SAMPLE("sample", TEXT_ARRAY, ""), // Alias to search for samples within proband.samples or family.members.samples
        INDIVIDUAL("individual", TEXT_ARRAY, ""), // Alias to search for members from proband or family.members

        FAMILY("family", TEXT_ARRAY, ""),
        FAMILY_ID("family.id", TEXT, ""),
        FAMILY_UID("family.uid", LONG, ""),
        FAMILY_MEMBERS_UID("family.members.uid", LONG_ARRAY, ""),
        FAMILY_MEMBERS_SAMPLES_UID("family.members.samples.uid", LONG_ARRAY, ""),
        FILES("files", TEXT_ARRAY, ""),
        FILES_UID("files.uid", LONG_ARRAY, ""),
        PANELS("panels", TEXT_ARRAY, ""),
        PANELS_UID("panels.uid", LONG_ARRAY, ""),
        COMMENTS("comments", TEXT_ARRAY, ""),
        COMMENTS_DATE("comments.date", TEXT, ""),
        ALERTS("alerts", TEXT_ARRAY, ""),
        PROBAND("proband", TEXT_ARRAY, ""),
        PROBAND_ID("proband.id", TEXT, ""),
        PROBAND_UID("proband.uid", LONG, ""),
        PROBAND_SAMPLES_ID("proband.samples.id", TEXT_ARRAY, ""),
        PROBAND_SAMPLES_UID("proband.samples.uid", INTEGER, ""),
        INTERPRETATION("interpretation", TEXT, ""),
        INTERPRETATION_ID("interpretation.id", TEXT, ""),
        INTERPRETATION_UID("interpretation.uid", LONG, ""),
        SECONDARY_INTERPRETATIONS("secondaryInterpretations", TEXT_ARRAY, ""),
        SECONDARY_INTERPRETATIONS_ID("secondaryInterpretations.id", TEXT_ARRAY, ""),
        SECONDARY_INTERPRETATIONS_UID("secondaryInterpretations.uid", LONG, ""),

        AUDIT("audit", TEXT_ARRAY, ""),

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""); // Alias to studyId in the database. Only for the webservices.

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

    OpenCGAResult insert(long studyId, ClinicalAnalysis clinicalAnalysis, List<ClinicalAudit> clinicalAuditList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<ClinicalAnalysis> update(long id, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<ClinicalAnalysis> update(Query query, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList,
                                           QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<ClinicalAnalysis> get(long clinicalAnalysisUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<ClinicalAnalysis> get(long studyUid, String clinicalAnalysisId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<?> delete(ClinicalAnalysis id, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<ClinicalAnalysis> delete(Query query, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyId(long clinicalAnalysisId) throws CatalogDBException;

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
