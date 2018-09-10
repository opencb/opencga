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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisManager extends ResourceManager<ClinicalAnalysis> {

    protected static Logger logger = LoggerFactory.getLogger(CohortManager.class);

    ClinicalAnalysisManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                                   Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    @Override
    ClinicalAnalysis smartResolutor(long studyUid, String entry, String user) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(),
                ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
                ClinicalAnalysisDBAdaptor.QueryParams.RELEASE.key(), ClinicalAnalysisDBAdaptor.QueryParams.ID.key(),
                ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<ClinicalAnalysis> analysisQueryResult = clinicalDBAdaptor.get(query, options, user);
        if (analysisQueryResult.getNumResults() == 0) {
            analysisQueryResult = clinicalDBAdaptor.get(query, options);
            if (analysisQueryResult.getNumResults() == 0) {
                throw new CatalogException("Clinical analysis " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the clinical analysis "
                        + entry);
            }
        } else if (analysisQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one clinical analysis found based on " + entry);
        } else {
            return analysisQueryResult.first();
        }
    }


    @Override
    public QueryResult<ClinicalAnalysis> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);

        if (queryResult.getNumResults() == 0 && query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.UID.key())) {
            List<Long> analysisList = query.getAsLongList(ClinicalAnalysisDBAdaptor.QueryParams.UID.key());
            for (Long analysisId : analysisList) {
                authorizationManager.checkClinicalAnalysisPermission(study.getUid(), analysisId, userId,
                        ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW);
            }
        }

        return queryResult;
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return clinicalDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, QueryOptions options,
                                                String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
        ParamUtils.checkAlias(clinicalAnalysis.getId(), "name");
        ParamUtils.checkObj(clinicalAnalysis.getType(), "type");

        validateSubjects(clinicalAnalysis, study, sessionId);
        validateFamilyAndSubjects(clinicalAnalysis, study, sessionId);
//        validateInterpretations(clinicalAnalysis.getInterpretations(), studyStr, sessionId);

        if (clinicalAnalysis.getGermline() != null && StringUtils.isNotEmpty(clinicalAnalysis.getGermline().getName())) {
            MyResource<File> resource = catalogManager.getFileManager().getUid(clinicalAnalysis.getGermline().getName(), studyStr,
                    sessionId);
            clinicalAnalysis.setGermline(resource.getResource());
        }

        if (clinicalAnalysis.getSomatic() != null && StringUtils.isNotEmpty(clinicalAnalysis.getSomatic().getName())) {
            MyResource<File> resource = catalogManager.getFileManager().getUid(clinicalAnalysis.getSomatic().getName(), studyStr,
                    sessionId);
            clinicalAnalysis.setSomatic(resource.getResource());
        }

        clinicalAnalysis.setCreationDate(TimeUtils.getTime());
        clinicalAnalysis.setDescription(ParamUtils.defaultString(clinicalAnalysis.getDescription(), ""));
        clinicalAnalysis.setStatus(new Status());
        clinicalAnalysis.setRelease(catalogManager.getStudyManager().getCurrentRelease(study, userId));
        clinicalAnalysis.setAttributes(ParamUtils.defaultObject(clinicalAnalysis.getAttributes(), Collections.emptyMap()));
        clinicalAnalysis.setInterpretations(ParamUtils.defaultObject(clinicalAnalysis.getInterpretations(), ArrayList::new));

        clinicalAnalysis.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.CLINICAL));
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.insert(study.getUid(), clinicalAnalysis, options);

        return queryResult;
    }

    private void validateInterpretations(List<ClinicalAnalysis.ClinicalInterpretation> interpretations, String studyStr, String sessionId)
            throws CatalogException {
        if (interpretations == null) {
            return;
        }

        for (ClinicalAnalysis.ClinicalInterpretation interpretation : interpretations) {
            ParamUtils.checkObj(interpretation.getId(), "interpretation id");
            ParamUtils.checkObj(interpretation.getName(), "interpretation name");
            ParamUtils.checkObj(interpretation.getFile(), "interpretation file");
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(studyStr, interpretation.getFile().getName(),
                    QueryOptions.empty(), sessionId);
            if (fileQueryResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation file not found");
            }
            if (fileQueryResult.first().getType() != File.Type.FILE) {
                throw new CatalogException("Interpretation file should point to a file. Detected " + fileQueryResult.first().getType());
            }
            interpretation.setFile(fileQueryResult.first());
        }
    }

    private void validateSubjects(ClinicalAnalysis clinicalAnalysis, Study study, String sessionId) throws CatalogException {
        if (clinicalAnalysis.getSubjects() == null || clinicalAnalysis.getSubjects().isEmpty()) {
            throw new CatalogException("Missing subjects in clinical analysis");
        }

        for (Individual individual : clinicalAnalysis.getSubjects()) {
            if (individual.getSamples() == null || individual.getSamples().isEmpty()) {
                throw new CatalogException("Missing samples from subject " + individual.getName());
            }
        }

        for (Individual subject : clinicalAnalysis.getSubjects()) {
            MyResource<Individual> resource = catalogManager.getIndividualManager().getUid(subject.getName(), study.getFqn(), sessionId);
            subject.setUid(resource.getResource().getUid());

            List<String> sampleIds = subject.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
            MyResources<Sample> sampleResources = catalogManager.getSampleManager().getUids(sampleIds, study.getFqn(), sessionId);
            if (sampleResources.getResourceList().size() < subject.getSamples().size()) {
                throw new CatalogException("Missing some samples. Found " + sampleResources.getResourceList().size() + " out of "
                        + subject.getSamples().size());
            }
            // We associate the samples to the subject
            subject.setSamples(sampleResources.getResourceList());

            // Check those samples are actually samples from the proband
            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.UID.key(), subject.getSamples().stream()
                            .map(Sample::getUid)
                            .collect(Collectors.toList()))
                    .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), subject.getUid());
            QueryResult<Sample> countSamples = catalogManager.getSampleManager().count(study.getFqn(), query, sessionId);
            if (countSamples.getNumTotalResults() < subject.getSamples().size()) {
                throw new CatalogException("Not all the samples belong to the proband. Only " + countSamples.getNumTotalResults()
                        + " out of the " + subject.getSamples().size() + " belong to the individual.");
            }
        }
    }

    private void validateFamilyAndSubjects(ClinicalAnalysis clinicalAnalysis, Study study, String sessionId) throws CatalogException {
        if (clinicalAnalysis.getFamily() != null && StringUtils.isNotEmpty(clinicalAnalysis.getFamily().getName())) {
            MyResource<Family> familyResource = catalogManager.getFamilyManager().getUid(clinicalAnalysis.getFamily().getName(),
                    study.getFqn(), sessionId);
            clinicalAnalysis.setFamily(familyResource.getResource());

            for (Individual subject : clinicalAnalysis.getSubjects()) {
                // Check the proband is an actual member of the family
                Query query = new Query()
                        .append(FamilyDBAdaptor.QueryParams.UID.key(), familyResource.getResource().getUid())
                        .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), subject.getUid());
                QueryResult<Family> count = catalogManager.getFamilyManager().count(study.getFqn(), query, sessionId);
                if (count.getNumTotalResults() == 0) {
                    throw new CatalogException("The member " + subject.getUid() + " does not belong to the family "
                            + clinicalAnalysis.getFamily().getName());
                }
            }
        }
    }

    @Override
    public QueryResult<ClinicalAnalysis> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options,
                                                String sessionId) throws CatalogException {
        MyResource<ClinicalAnalysis> resource = getUid(entryStr, studyStr, sessionId);
        authorizationManager.checkClinicalAnalysisPermission(resource.getStudy().getUid(), resource.getResource().getUid(),
                resource.getUser(), ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            ClinicalAnalysisDBAdaptor.QueryParams queryParam = ClinicalAnalysisDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case ID:
                    ParamUtils.checkAlias(parameters.getString(queryParam.key()), "name");
                    break;
                case INTERPRETATIONS:
                    // Get the file uid
                    List<LinkedHashMap<String, Object>> interpretationList = (List<LinkedHashMap<String, Object>>) param.getValue();
                    for (LinkedHashMap<String, Object> interpretationMap : interpretationList) {
                        LinkedHashMap<String, Object> fileMap = (LinkedHashMap<String, Object>) interpretationMap.get("file");
                        MyResource<File> fileResource = catalogManager.getFileManager().getUid(String.valueOf(fileMap.get("path")),
                                studyStr, sessionId);
                        fileMap.put(FileDBAdaptor.QueryParams.UID.key(), fileResource.getResource().getUid());
                    }
                    break;
                case FAMILY:
                case SUBJECTS:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())
                || parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key())) {
            // Fetch current information to autocomplete the validation
            Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), resource.getResource().getUid());
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
                            ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key()));
            QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = get(studyStr, query, queryOptions, sessionId);
            ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

            ObjectMapper jsonObjectMapper = getDefaultObjectMapper();

            try {
                if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())) {
                    String familyString = jsonObjectMapper.writeValueAsString(
                            parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key()));
                    Family family = jsonObjectMapper.readValue(familyString, Family.class);
                    clinicalAnalysis.setFamily(family);
                }
                if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key())) {
                    List<String> subjectStrings = new ArrayList<>();
                    for (Object o : parameters.getAsList(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key())) {
                        subjectStrings.add(jsonObjectMapper.writeValueAsString(o));
                    }
                    List<Individual> subjectList = new ArrayList<>(subjectStrings.size());
                    for (String subjectString : subjectStrings) {
                        subjectList.add(jsonObjectMapper.readValue(subjectString, Individual.class));
                    }
                    clinicalAnalysis.setSubjects(subjectList);
                }
                validateSubjects(clinicalAnalysis, resource.getStudy(), sessionId);
                validateFamilyAndSubjects(clinicalAnalysis, resource.getStudy(), sessionId);

                parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), clinicalAnalysis.getFamily());
                parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key(), clinicalAnalysis.getSubjects());

            } catch (IOException e) {
                logger.error("Error checking families and subjects: {}", e.getMessage(), e);
                throw new CatalogException(e);
            }
        }

        return clinicalDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());
    }

//    public QueryResult<ClinicalAnalysis> updateInterpretation(String studyStr, String clinicalAnalysisStr,
//                                                List<ClinicalAnalysis.ClinicalInterpretation> interpretations,
//                                                ClinicalAnalysis.Action action, QueryOptions queryOptions, String sessionId)
//            throws CatalogException {
//
//        MyResourceId resource = getId(clinicalAnalysisStr, studyStr, sessionId);
//        authorizationManager.checkClinicalAnalysisPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
//                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);
//
//        ParamUtils.checkObj(interpretations, "interpretations");
//        ParamUtils.checkObj(action, "action");
//
//        if (action != ClinicalAnalysis.Action.REMOVE) {
//            validateInterpretations(interpretations, String.valueOf(resource.getStudyId()), sessionId);
//        }
//
//        switch (action) {
//            case ADD:
//                for (ClinicalAnalysis.ClinicalInterpretation interpretation : interpretations) {
//                    clinicalDBAdaptor.addInterpretation(resource.getResourceId(), interpretation);
//                }
//                break;
//            case SET:
//                clinicalDBAdaptor.setInterpretations(resource.getResourceId(), interpretations);
//                break;
//            case REMOVE:
//                for (ClinicalAnalysis.ClinicalInterpretation interpretation : interpretations) {
//                    clinicalDBAdaptor.removeInterpretation(resource.getResourceId(), interpretation.getId());
//                }
//                break;
//            default:
//                throw new CatalogException("Unexpected action found");
//        }
//
//        return clinicalDBAdaptor.get(resource.getResourceId(), queryOptions);
//    }


    public QueryResult<ClinicalAnalysis> search(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(query, study, sessionId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);
//            authorizationManager.filterClinicalAnalysis(userId, studyId, queryResultAux.getResult());

        return queryResult;
    }

    private void fixQueryObject(Query query, Study study, String sessionId) throws CatalogException {
        if (query.containsKey("family")) {
            MyResource<Family> familyResource = catalogManager.getFamilyManager().getUid(query.getString("family"), study.getFqn(),
                    sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(), familyResource.getResource().getUid());
            query.remove("family");
        }
        if (query.containsKey("sample")) {
            MyResource<Sample> sampleResource = catalogManager.getSampleManager().getUid(query.getString("sample"), study.getFqn(),
                    sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE_UID.key(), sampleResource.getResource().getUid());
            query.remove("sample");
        }
        if (query.containsKey("subject")) {
            MyResource<Individual> probandResource = catalogManager.getIndividualManager().getUid(query.getString("subject"),
                    study.getFqn(), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECT_UID.key(), probandResource.getResource().getUid());
            query.remove("subject");
        }
        if (query.containsKey("germline")) {
            MyResource<File> resource = catalogManager.getFileManager().getUid(query.getString("germline"), study.getFqn(), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.GERMLINE_UID.key(), resource.getResource().getUid());
            query.remove("germline");
        }
        if (query.containsKey("somatic")) {
            MyResource<File> resource = catalogManager.getFileManager().getUid(query.getString("somatic"), study.getFqn(), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SOMATIC_UID.key(), resource.getResource().getUid());
            query.remove("somatic");
        }
    }

    public QueryResult<ClinicalAnalysis> count(String studyStr, Query query, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(query, study, sessionId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = clinicalDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        return null;
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(query, study, sessionId);

        // Add study id to the query
        query.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult queryResult = clinicalDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    private long getClinicalId(boolean silent, String clinicalStrAux) throws CatalogException {
        long clinicalId = Long.parseLong(clinicalStrAux);
        try {
            clinicalDBAdaptor.checkId(clinicalId);
        } catch (CatalogException e) {
            if (silent) {
                return -1L;
            } else {
                throw e;
            }
        }
        return clinicalId;
    }
}
