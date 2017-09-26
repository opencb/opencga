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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.ParamUtils;
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
            userId = catalogManager.getUserManager().getUserId(sessionId);
        } else {
            if (clinicalStr.contains(",")) {
                throw new CatalogException("More than one clinical analysis found");
            }

            userId = catalogManager.getUserManager().getUserId(sessionId);
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
            userId = catalogManager.getUserManager().getUserId(sessionId);
        } else {
            userId = catalogManager.getUserManager().getUserId(sessionId);
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

    @Override
    public Long getStudyId(long entryId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<ClinicalAnalysis> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);

        if (queryResult.getNumResults() == 0 && query.containsKey("id")) {
            List<Long> analysisList = query.getAsLongList("id");
            for (Long analysisId : analysisList) {
                authorizationManager.checkClinicalAnalysisPermission(studyId, analysisId, userId,
                        ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW);
            }
        }

        addMissingInformation(queryResult, studyId, sessionId);

        return queryResult;
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        return clinicalDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, QueryOptions options,
                                                String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
        ParamUtils.checkAlias(clinicalAnalysis.getName(), "name", configuration.getCatalog().getOffset());
        ParamUtils.checkObj(clinicalAnalysis.getType(), "type");
        ParamUtils.checkObj(clinicalAnalysis.getFamily(), "family");

        if (clinicalAnalysis.getSubjects() == null || clinicalAnalysis.getSubjects().isEmpty()) {
            throw new CatalogException("Missing subjects to clinical analysis");
        }

        for (Individual individual : clinicalAnalysis.getSubjects()) {
            if (individual.getSamples() == null || individual.getSamples().isEmpty()) {
                throw new CatalogException("Missing samples from subject " + individual.getName());
            }
        }

        if (clinicalAnalysis.getGermline() != null && StringUtils.isNotEmpty(clinicalAnalysis.getGermline().getName())) {
            MyResourceId resource = catalogManager.getFileManager().getId(clinicalAnalysis.getGermline().getName(), String.valueOf(studyId),
                    sessionId);
            clinicalAnalysis.getGermline().setId(resource.getResourceId());
        }

        if (clinicalAnalysis.getSomatic() != null && StringUtils.isNotEmpty(clinicalAnalysis.getSomatic().getName())) {
            MyResourceId resource = catalogManager.getFileManager().getId(clinicalAnalysis.getSomatic().getName(), String.valueOf(studyId),
                    sessionId);
            clinicalAnalysis.getSomatic().setId(resource.getResourceId());
        }

        clinicalAnalysis.setCreationDate(TimeUtils.getTime());
        clinicalAnalysis.setDescription(ParamUtils.defaultString(clinicalAnalysis.getDescription(), ""));

        clinicalAnalysis.setStatus(new Status());
        clinicalAnalysis.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));
        clinicalAnalysis.setAttributes(ParamUtils.defaultObject(clinicalAnalysis.getAttributes(), Collections.emptyMap()));
        clinicalAnalysis.setInterpretations(ParamUtils.defaultObject(clinicalAnalysis.getInterpretations(), ArrayList::new));
        validateInterpretations(clinicalAnalysis.getInterpretations(), String.valueOf(studyId), sessionId);

        MyResourceId familyResource = catalogManager.getFamilyManager().getId(clinicalAnalysis.getFamily().getName(),
                Long.toString(studyId), sessionId);
        clinicalAnalysis.getFamily().setId(familyResource.getResourceId());

        for (Individual subject : clinicalAnalysis.getSubjects()) {
            MyResourceId probandResources = catalogManager.getIndividualManager().getId(subject.getName(), Long.toString(studyId),
                    sessionId);

            subject.setId(probandResources.getResourceId());

            // Check the proband is an actual member of the family
            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.ID.key(), familyResource.getResourceId())
                    .append(FamilyDBAdaptor.QueryParams.MEMBER_ID.key(), probandResources.getResourceId());
            QueryResult<Family> count = catalogManager.getFamilyManager().count(Long.toString(studyId), query, sessionId);
            if (count.getNumTotalResults() == 0) {
                throw new CatalogException("The member " + subject.getName() + " does not belong to the family "
                        + clinicalAnalysis.getFamily().getName());
            }

            String sampleNames = subject.getSamples().stream().map(Sample::getName).collect(Collectors.joining(","));
            MyResourceIds sampleResources = catalogManager.getSampleManager().getIds(sampleNames, Long.toString(studyId), sessionId);
            if (sampleResources.getResourceIds().size() < subject.getSamples().size()) {
                throw new CatalogException("Missing some samples. Found " + sampleResources.getResourceIds().size() + " out of "
                        + subject.getSamples().size());
            }
            // We populate the sample array with only the ids
            List<Sample> samples = sampleResources.getResourceIds().stream()
                    .map(sampleId -> new Sample().setId(sampleId))
                    .collect(Collectors.toList());
            subject.setSamples(samples);

            // Check those samples are actually samples from the proband
            query = new Query()
                    .append(SampleDBAdaptor.QueryParams.ID.key(), sampleResources.getResourceIds())
                    .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), probandResources.getResourceId());
            QueryResult<Sample> countSamples = catalogManager.getSampleManager().count(Long.toString(studyId), query, sessionId);
            if (countSamples.getNumTotalResults() < sampleResources.getResourceIds().size()) {
                throw new CatalogException("Not all the samples belong to the proband. Only " + countSamples.getNumTotalResults()
                        + " out of the " + sampleResources.getResourceIds().size() + " belong to the individual.");
            }
        }

        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.insert(studyId, clinicalAnalysis, options);

        addMissingInformation(queryResult, studyId, sessionId);
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

    @Override
    public QueryResult<ClinicalAnalysis> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options,
                                                String sessionId) throws CatalogException {
        throw new NotImplementedException("Update clinical analysis not implemented");
    }

    public QueryResult<ClinicalAnalysis> updateInterpretation(String studyStr, String clinicalAnalysisStr,
                                                List<ClinicalAnalysis.ClinicalInterpretation> interpretations,
                                                ClinicalAnalysis.Action action, QueryOptions queryOptions, String sessionId)
            throws CatalogException {

        MyResourceId resource = getId(clinicalAnalysisStr, studyStr, sessionId);
        authorizationManager.checkClinicalAnalysisPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        ParamUtils.checkObj(interpretations, "interpretations");
        ParamUtils.checkObj(action, "action");

        if (action != ClinicalAnalysis.Action.REMOVE) {
            validateInterpretations(interpretations, String.valueOf(resource.getStudyId()), sessionId);
        }

        switch (action) {
            case ADD:
                for (ClinicalAnalysis.ClinicalInterpretation interpretation : interpretations) {
                    clinicalDBAdaptor.addInterpretation(resource.getResourceId(), interpretation);
                }
                break;
            case SET:
                clinicalDBAdaptor.setInterpretations(resource.getResourceId(), interpretations);
                break;
            case REMOVE:
                for (ClinicalAnalysis.ClinicalInterpretation interpretation : interpretations) {
                    clinicalDBAdaptor.removeInterpretation(resource.getResourceId(), interpretation.getId());
                }
                break;
            default:
                throw new CatalogException("Unexpected action found");
        }

        return clinicalDBAdaptor.get(resource.getResourceId(), queryOptions);
    }


    public QueryResult<ClinicalAnalysis> search(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
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
        if (query.containsKey("subject")) {
            MyResourceId probandResource = catalogManager.getIndividualManager().getId(query.getString("subject"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECT_ID.key(), probandResource.getResourceId());
            query.remove("subject");
        }
        if (query.containsKey("germline")) {
            MyResourceId resource = catalogManager.getFileManager().getId(query.getString("germline"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.GERMLINE_ID.key(), resource.getResourceId());
            query.remove("germline");
        }
        if (query.containsKey("somatic")) {
            MyResourceId resource = catalogManager.getFileManager().getId(query.getString("somatic"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SOMATIC_ID.key(), resource.getResourceId());
            query.remove("somatic");
        }

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);
//            authorizationManager.filterClinicalAnalysis(userId, studyId, queryResultAux.getResult());

        addMissingInformation(queryResult, studyId, sessionId);
        return queryResult;
    }

    public QueryResult<ClinicalAnalysis> count(String studyStr, Query query, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
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
        if (query.containsKey("subject")) {
            MyResourceId probandResource = catalogManager.getIndividualManager().getId(query.getString("subject"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECT_ID.key(), probandResource.getResourceId());
            query.remove("subject");
        }
        if (query.containsKey("germline")) {
            MyResourceId resource = catalogManager.getFileManager().getId(query.getString("germline"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.GERMLINE_ID.key(), resource.getResourceId());
            query.remove("germline");
        }
        if (query.containsKey("somatic")) {
            MyResourceId resource = catalogManager.getFileManager().getId(query.getString("somatic"),
                    Long.toString(studyId), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SOMATIC_ID.key(), resource.getResourceId());
            query.remove("somatic");
        }

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = clinicalDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public List<QueryResult<ClinicalAnalysis>> delete(@Nullable String studyStr, String entries, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
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
        return null;
    }

    private void addMissingInformation(QueryResult<ClinicalAnalysis> queryResult, long studyId, String sessionId) {
        if (queryResult.getNumResults() == 0) {
            return;
        }

        List<String> warningMessages = new ArrayList<>();
        for (ClinicalAnalysis clinicalAnalysis : queryResult.getResult()) {

            // Complete somatic file information
            if (clinicalAnalysis.getSomatic() != null && clinicalAnalysis.getSomatic().getId() > 0) {
                try {
                    QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(String.valueOf(studyId),
                            String.valueOf(clinicalAnalysis.getSomatic().getId()), QueryOptions.empty(), sessionId);
                    if (fileQueryResult.getNumResults() == 1) {
                        clinicalAnalysis.setSomatic(fileQueryResult.first());
                    }
                } catch (CatalogException e) {
                    String message = "Could not fetch somatic file information to complete Clinical Analysis object";
                    logger.warn("{}: {}", message, e.getMessage(), e);
                    warningMessages.add(message);
                }
            }

            // Complete germline file information
            if (clinicalAnalysis.getGermline() != null && clinicalAnalysis.getGermline().getId() > 0) {
                try {
                    QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(String.valueOf(studyId),
                            String.valueOf(clinicalAnalysis.getGermline().getId()), QueryOptions.empty(), sessionId);
                    if (fileQueryResult.getNumResults() == 1) {
                        clinicalAnalysis.setGermline(fileQueryResult.first());
                    }
                } catch (CatalogException e) {
                    String message = "Could not fetch germline file information to complete Clinical Analysis object";
                    logger.warn("{}: {}", message, e.getMessage(), e);
                    warningMessages.add(message);
                }
            }

            // Complete family information
            if (clinicalAnalysis.getFamily() != null && clinicalAnalysis.getFamily().getId() > 0) {
                try {
                    QueryResult<Family> familyQueryResult = catalogManager.getFamilyManager().get(String.valueOf(studyId),
                            String.valueOf(clinicalAnalysis.getFamily().getId()), QueryOptions.empty(), sessionId);
                    if (familyQueryResult.getNumResults() == 1) {
                        clinicalAnalysis.setFamily(familyQueryResult.first());
                    }
                } catch (CatalogException e) {
                    String message = "Could not fetch family information to complete Clinical Analysis object";
                    logger.warn("{}: {}", message, e.getMessage(), e);
                    warningMessages.add(message);
                }
            }

            // Complete subject information
            if (clinicalAnalysis.getSubjects() != null && !clinicalAnalysis.getSubjects().isEmpty()) {
                try {
                    List<Long> individualIds = clinicalAnalysis.getSubjects().stream()
                            .map(Individual::getId)
                            .filter(id -> id > 0)
                            .collect(Collectors.toList());
                    Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualIds);
                    QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(String.valueOf(studyId),
                            query, QueryOptions.empty(), sessionId);

                    // We create a map of individual id - individual to find the results easily
                    Map<Long, Individual> individualMap = new HashMap<>();
                    for (Individual individual : individualQueryResult.getResult()) {
                        individualMap.put(individual.getId(), individual);
                    }

                    // We create a list of individuals (subjects) to be replaced in the clinicalAnalysis
                    List<Individual> subjectList = new ArrayList<>();
                    for (Individual subject : clinicalAnalysis.getSubjects()) {
                        Individual completeIndividual = individualMap.get(subject.getId());

                        // We need to filter out from the individuals fetched, the samples that are not of interest for the analysis
                        if (subject.getSamples() != null && !subject.getSamples().isEmpty()) {
                            // We create a set of the sample ids of interest
                            Set<Long> samplesOfInterest = subject.getSamples().stream().map(Sample::getId).collect(Collectors.toSet());

                            // And we now filter out those that are not of interest
                            completeIndividual.getSamples().removeIf(sample -> !samplesOfInterest.contains(sample.getId()));
                        }

                        subjectList.add(completeIndividual);
                    }

                    // We now assign the list of individuals
                    clinicalAnalysis.setSubjects(subjectList);
                } catch (CatalogException e) {
                    String message = "Could not fetch subject information to complete Clinical Analysis object";
                    logger.warn("{}: {}", message, e.getMessage(), e);
                    warningMessages.add(message);
                }
            }
        }

        if (warningMessages.size() > 0) {
            queryResult.setWarningMsg(StringUtils.join(warningMessages, "\n"));
        }
    }

}
