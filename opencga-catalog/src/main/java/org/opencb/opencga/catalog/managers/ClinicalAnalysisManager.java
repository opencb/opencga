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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.ClinicalAnalysis;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisManager extends AbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(CohortManager.class);

    public ClinicalAnalysisManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                                   Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param clinicalStr Clinical analysis id in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one clinical analysis id is found.
     */
    public MyResourceId getId(String clinicalStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(clinicalStr)) {
            throw new CatalogException("Missing clinical analysis parameter");
        }

        String userId;
        long studyId;
        long clinicalAnalysisId;

        if (StringUtils.isNumeric(clinicalStr) && Long.parseLong(clinicalStr) > configuration.getCatalog().getOffset()) {
            clinicalAnalysisId = Long.parseLong(clinicalStr);
            clinicalDBAdaptor.exists(clinicalAnalysisId);
            studyId = clinicalDBAdaptor.getStudyId(clinicalAnalysisId);
            userId = catalogManager.getUserManager().getId(sessionId);
        } else {
            if (clinicalStr.contains(",")) {
                throw new CatalogException("More than one clinical analysis found");
            }

            userId = catalogManager.getUserManager().getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Query query = new Query()
                    .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(ClinicalAnalysisDBAdaptor.QueryParams.NAME.key(), clinicalStr);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, ClinicalAnalysisDBAdaptor.QueryParams.ID.key());
            QueryResult<ClinicalAnalysis> clinicalQueryResult = clinicalDBAdaptor.get(query, queryOptions);
            if (clinicalQueryResult.getNumResults() == 1) {
                clinicalAnalysisId = clinicalQueryResult.first().getId();
            } else {
                if (clinicalQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Clinical analysis " + clinicalStr + " not found in study " + studyStr);
                } else {
                    throw new CatalogException("More than one clinical analysis found under " + clinicalStr + " in study " + studyStr);
                }
            }
        }

        return new MyResourceId(userId, studyId, clinicalAnalysisId);
    }

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param clinicalStr Clinical analysis id in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    public MyResourceIds getIds(String clinicalStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(clinicalStr)) {
            throw new CatalogException("Missing clinical analysis parameter");
        }

        String userId;
        long studyId;
        List<Long> clinicalIds = new ArrayList<>();

        if (StringUtils.isNumeric(clinicalStr) && Long.parseLong(clinicalStr) > configuration.getCatalog().getOffset()) {
            clinicalIds = Arrays.asList(Long.parseLong(clinicalStr));
            clinicalDBAdaptor.checkId(clinicalIds.get(0));
            studyId = clinicalDBAdaptor.getStudyId(clinicalIds.get(0));
            userId = catalogManager.getUserManager().getId(sessionId);
        } else {
            userId = catalogManager.getUserManager().getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            List<String> clinicalSplit = Arrays.asList(clinicalStr.split(","));
            for (String clinicalStrAux : clinicalSplit) {
                if (StringUtils.isNumeric(clinicalStrAux)) {
                    long clinicalId = Long.parseLong(clinicalStrAux);
                    clinicalDBAdaptor.exists(clinicalId);
                    clinicalIds.add(clinicalId);
                }
            }

            Query query = new Query()
                    .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(ClinicalAnalysisDBAdaptor.QueryParams.NAME.key(), clinicalSplit);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, ClinicalAnalysisDBAdaptor.QueryParams.ID.key());
            QueryResult<ClinicalAnalysis> clinicalQueryResult = clinicalDBAdaptor.get(query, queryOptions);

            if (clinicalQueryResult.getNumResults() > 0) {
                clinicalIds.addAll(clinicalQueryResult.getResult().stream().map(ClinicalAnalysis::getId).collect(Collectors.toList()));
            }

            if (clinicalIds.size() < clinicalSplit.size()) {
                throw new CatalogException("Found only " + clinicalIds.size() + " out of the " + clinicalSplit.size()
                        + " clinical analysis looked for in study " + studyStr);
            }
        }

        return new MyResourceIds(userId, studyId, clinicalIds);
    }


    public QueryResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, QueryOptions options,
                                                String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
        ParamUtils.checkAlias(clinicalAnalysis.getName(), "name", configuration.getCatalog().getOffset());
        ParamUtils.checkObj(clinicalAnalysis.getType(), "type");

        clinicalAnalysis.setCreationDate(TimeUtils.getTime());
        clinicalAnalysis.setDescription(ParamUtils.defaultString(clinicalAnalysis.getDescription(), ""));

        clinicalAnalysis.setStatus(new Status());
        clinicalAnalysis.setAcl(Collections.emptyList());
        clinicalAnalysis.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));
        clinicalAnalysis.setAttributes(ParamUtils.defaultObject(clinicalAnalysis.getAttributes(), Collections.emptyMap()));

        MyResourceId familyResource = catalogManager.getFamilyManager().getId(clinicalAnalysis.getFamily().getName(),
                Long.toString(studyId), sessionId);
        clinicalAnalysis.getFamily().setId(familyResource.getResourceId());

        MyResourceId probandResource = catalogManager.getIndividualManager().getId(clinicalAnalysis.getProband().getName(),
                Long.toString(studyId), sessionId);
        clinicalAnalysis.getProband().setId(probandResource.getResourceId());

        MyResourceId sampleResource = catalogManager.getSampleManager().getId(clinicalAnalysis.getSample().getName(),
                Long.toString(studyId), sessionId);
        clinicalAnalysis.getSample().setId(sampleResource.getResourceId());

        return clinicalDBAdaptor.insert(studyId, clinicalAnalysis, options);
    }

    public List<QueryResult<ClinicalAnalysis>> get(String studyStr, String clinicalAnalysis, QueryOptions options, String sessionId)
            throws CatalogException {
        MyResourceIds resourceIds = getIds(clinicalAnalysis, studyStr, sessionId);

        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId());
        List<QueryResult<ClinicalAnalysis>> queryResults = new ArrayList<>(resourceIds.getResourceIds().size());
        for (Long clinicalAnalysisId : resourceIds.getResourceIds()) {
            query.append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), clinicalAnalysisId);
            QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = clinicalDBAdaptor.get(query, options, resourceIds.getUser());
            clinicalAnalysisQueryResult.setId(Long.toString(clinicalAnalysisId));
            queryResults.add(clinicalAnalysisQueryResult);
        }

        return queryResults;
    }

    public QueryResult<ClinicalAnalysis> search(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (query.containsKey("family")) {
            MyResourceId familyResource = catalogManager.getFamilyManager().getId(query.getString("family"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_ID.key(), familyResource.getResourceId());
            query.remove("family");
        }
        if (query.containsKey("sample")) {
            MyResourceId sampleResource = catalogManager.getSampleManager().getId(query.getString("sample"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE_ID.key(), sampleResource.getResourceId());
            query.remove("sample");
        }
        if (query.containsKey("proband")) {
            MyResourceId probandResource = catalogManager.getIndividualManager().getId(query.getString("proband"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_ID.key(), probandResource.getResourceId());
            query.remove("proband");
        }



        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);
//            authorizationManager.filterClinicalAnalysis(userId, studyId, queryResultAux.getResult());

        return queryResult;
    }

    public QueryResult<ClinicalAnalysis> count(String studyStr, Query query, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (query.containsKey("family")) {
            MyResourceId familyResource = catalogManager.getFamilyManager().getId(query.getString("family"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_ID.key(), familyResource.getResourceId());
            query.remove("family");
        }
        if (query.containsKey("sample")) {
            MyResourceId sampleResource = catalogManager.getSampleManager().getId(query.getString("sample"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE_ID.key(), sampleResource.getResourceId());
            query.remove("sample");
        }
        if (query.containsKey("proband")) {
            MyResourceId probandResource = catalogManager.getIndividualManager().getId(query.getString("proband"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_ID.key(), probandResource.getResourceId());
            query.remove("proband");
        }

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = clinicalDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }
}
