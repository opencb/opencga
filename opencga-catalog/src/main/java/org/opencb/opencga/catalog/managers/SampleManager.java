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
import org.apache.commons.lang3.math.NumberUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleManager extends AnnotationSetManager<Sample> {

    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    SampleManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                  Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    public Long getStudyId(long sampleId) throws CatalogException {
        return sampleDBAdaptor.getStudyId(sampleId);
    }

    @Override
    public QueryResult<Sample> create(String studyStr, Sample sample, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkAlias(sample.getName(), "name", configuration.getCatalog().getOffset());
        sample.setSource(ParamUtils.defaultString(sample.getSource(), ""));
        sample.setDescription(ParamUtils.defaultString(sample.getDescription(), ""));
        sample.setType(ParamUtils.defaultString(sample.getType(), ""));
        sample.setPhenotypes(ParamUtils.defaultObject(sample.getPhenotypes(), Collections.emptyList()));
        sample.setAnnotationSets(ParamUtils.defaultObject(sample.getAnnotationSets(), Collections.emptyList()));
        sample.setStats(ParamUtils.defaultObject(sample.getStats(), Collections.emptyMap()));
        sample.setAttributes(ParamUtils.defaultObject(sample.getAttributes(), Collections.emptyMap()));
        sample.setStatus(new Status());
        sample.setCreationDate(TimeUtils.getTime());
        sample.setVersion(1);

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        List<VariableSet> variableSetList = validateNewAnnotationSetsAndExtractVariableSets(studyId, sample.getAnnotationSets());

        // 1. We check everything can be done
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_SAMPLES);

        // We will store the individual information if the individual already exists
        Individual individual = null;

        // if 0, it means there is no individual passed with the sample.
        // if 1, it means there is an individual that already exists, and we will need to update the sample array.
        // if 2, it means the individual does not exist and we will have to create it from scratch containing the current sample.
        int individualInfo = 0;

        if (sample.getIndividual() != null && StringUtils.isNotEmpty(sample.getIndividual().getName())) {
            try {
                QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(String.valueOf(studyId),
                        sample.getIndividual().getName(),
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                IndividualDBAdaptor.QueryParams.ID.key(),
                                IndividualDBAdaptor.QueryParams.SAMPLES.key())),
                        sessionId);

                // Check if the user can update the individual
                authorizationManager.checkIndividualPermission(studyId, individualQueryResult.first().getId(), userId,
                        IndividualAclEntry.IndividualPermissions.UPDATE);

                individual = individualQueryResult.first();
                individualInfo = 1;

            } catch (CatalogException e) {
                if (e instanceof CatalogAuthorizationException) {
                    throw e;
                }

                // The individual does not exist so we check if the user will be able to create it
                authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);
                individualInfo = 2;
            }
        }

        // 2. We create the sample
        sample.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));
        QueryResult<Sample> queryResult = sampleDBAdaptor.insert(studyId, sample, variableSetList, options);
        auditManager.recordCreation(AuditRecord.Resource.sample, queryResult.first().getId(), userId, queryResult.first(), null, null);

        // 3. We update or create an individual if any..
        // We check if we have to update or create a new individual containing the sample
        if (individualInfo > 0) {
            if (individualInfo == 1) { // Update individual info
                List<Sample> sampleList = new ArrayList<>(individual.getSamples().size() + 1);
                sampleList.addAll(individual.getSamples());
                sampleList.add(queryResult.first());

                ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
                try {
                    individualDBAdaptor.update(individual.getId(), params, QueryOptions.empty());
                } catch (CatalogDBException e) {
                    logger.error("Internal error. The sample was created but the sample could not be associated to the individual. {}",
                            e.getMessage(), e);
                    queryResult.setErrorMsg("Internal error. The sample was created but the sample could not be associated to the "
                            + "individual. " + e.getMessage());
                }
            } else { // = 2 - Create new individual
                sample.getIndividual().setSamples(Arrays.asList(queryResult.first()));
                try {
                    catalogManager.getIndividualManager().create(String.valueOf(studyId), sample.getIndividual(), QueryOptions.empty(),
                            sessionId);
                } catch (CatalogException e) {
                    logger.error("Internal error. The sample was created but the individual could not be created. {}", e.getMessage(), e);
                    queryResult.setErrorMsg("Internal error. The sample was created but the individual could not be created. "
                            + e.getMessage());
                }
            }
        }

        return queryResult;
    }

    @Deprecated
    public QueryResult<Sample> create(String studyStr, String name, String source, String description, String type, boolean somatic,
                                      Individual individual, Map<String, Object> stats, Map<String, Object> attributes,
                                      QueryOptions options, String sessionId)
            throws CatalogException {
        Sample sample = new Sample(-1, name, source, individual, description, type, somatic, -1, 1, Collections.emptyList(),
                Collections.emptyList(), stats, attributes);
        return create(studyStr, sample, options, sessionId);
    }

    @Override
    public MyResourceId getId(String sampleStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(sampleStr)) {
            throw new CatalogException("Missing sample parameter");
        }

        String userId;
        long studyId;
        long sampleId;

        if (StringUtils.isNumeric(sampleStr) && Long.parseLong(sampleStr) > configuration.getCatalog().getOffset()) {
            sampleId = Long.parseLong(sampleStr);
            sampleDBAdaptor.exists(sampleId);
            studyId = sampleDBAdaptor.getStudyId(sampleId);
            userId = userManager.getUserId(sessionId);
        } else {
            if (sampleStr.contains(",")) {
                throw new CatalogException("More than one sample found");
            }

            userId = userManager.getUserId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(SampleDBAdaptor.QueryParams.NAME.key(), sampleStr);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());
            QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);
            if (sampleQueryResult.getNumResults() == 1) {
                sampleId = sampleQueryResult.first().getId();
            } else {
                if (sampleQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Sample " + sampleStr + " not found in study " + studyStr);
                } else {
                    throw new CatalogException("More than one sample found under " + sampleStr + " in study " + studyStr);
                }
            }
        }

        return new MyResourceId(userId, studyId, sampleId);
    }

    @Override
    public MyResourceIds getIds(List<String> sampleList, @Nullable String studyStr, boolean silent, String sessionId)
            throws CatalogException {
        if (sampleList == null || sampleList.isEmpty()) {
            throw new CatalogException("Missing sample parameter");
        }

        String userId;
        long studyId;
        List<Long> sampleIds = new ArrayList<>();

        if (sampleList.size() == 1 && StringUtils.isNumeric(sampleList.get(0))
                && Long.parseLong(sampleList.get(0)) > configuration.getCatalog().getOffset()) {
            sampleIds.add(Long.parseLong(sampleList.get(0)));
            sampleDBAdaptor.checkId(sampleIds.get(0));
            studyId = sampleDBAdaptor.getStudyId(sampleIds.get(0));
            userId = userManager.getUserId(sessionId);
        } else {
            userId = userManager.getUserId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Map<String, Long> myIds = new HashMap<>();
            for (String sampleStrAux : sampleList) {
                if (StringUtils.isNumeric(sampleStrAux)) {
                    long sampleId = getSampleId(silent, sampleStrAux);
                    myIds.put(sampleStrAux, sampleId);
                }
            }

            if (myIds.size() < sampleList.size()) {
                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(SampleDBAdaptor.QueryParams.NAME.key(), sampleList);

                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.NAME.key()));
                QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);

                if (sampleQueryResult.getNumResults() > 0) {
                    myIds.putAll(sampleQueryResult.getResult().stream().collect(Collectors.toMap(Sample::getName, Sample::getId)));
                }
            }
            if (myIds.size() < sampleList.size() && !silent) {
                throw new CatalogException("Found only " + myIds.size() + " out of the " + sampleList.size()
                        + " samples looked for in study " + studyStr);
            }
            for (String sampleStrAux : sampleList) {
                sampleIds.add(myIds.getOrDefault(sampleStrAux, -1L));
            }
        }

        return new MyResourceIds(userId, studyId, sampleIds);
    }

    public QueryResult<Sample> get(Long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(sampleId), options, sessionId);
    }

    public QueryResult<Sample> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return get(String.valueOf(studyId), query, options, sessionId);
    }

    @Override
    public QueryResult<Sample> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, query);
        fixQueryOptionAnnotation(options);

        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, options, userId);

        if (sampleQueryResult.getNumResults() == 0 && query.containsKey("id")) {
            List<Long> sampleIds = query.getAsLongList("id");
            for (Long sampleId : sampleIds) {
                authorizationManager.checkSamplePermission(studyId, sampleId, userId, SampleAclEntry.SamplePermissions.VIEW);
            }
        }
        addIndividualInformation(sampleQueryResult, studyId, options, sessionId);

        return sampleQueryResult;
    }

    @Override
    public DBIterator<Sample> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        return sampleDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<Sample> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, query);
        fixQueryOptionAnnotation(options);

        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager().getIds(
                    query.getAsStringList(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()), Long.toString(studyId), sessionId);
            query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), resourceIds.getResourceIds());
            query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, options, userId);
        addIndividualInformation(queryResult, studyId, options, sessionId);
        return queryResult;
    }

    @Override
    public QueryResult<Sample> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, query);

        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager().getIds(
                    query.getAsStringList(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()), Long.toString(studyId), sessionId);
            query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), resourceIds.getResourceIds());
            query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = sampleDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_SAMPLES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    // TODO
    // This implementation should be changed and made better. Check the comment in IndividualManager -> delete(). Those changes
    // will probably make the delete from individualManager to be changed.
    public List<QueryResult<Sample>> delete(@Nullable String studyStr, String sampleIdStr, ObjectMap options, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(sampleIdStr, "id");
//        options = ParamUtils.defaultObject(options, QueryOptions::new);
        MyResourceIds resourceId = getIds(Arrays.asList(StringUtils.split(sampleIdStr, ",")), studyStr, sessionId);

        List<QueryResult<Sample>> queryResultList = new ArrayList<>(resourceId.getResourceIds().size());
        for (Long sampleId : resourceId.getResourceIds()) {
            QueryResult<Sample> queryResult = null;
            try {
                MyResourceIds myResourceId = new MyResourceIds(resourceId.getUser(), resourceId.getStudyId(), Arrays.asList(sampleId));
                checkCanDeleteSamples(myResourceId);

                // Get the sample info before the update
                QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(sampleId, QueryOptions.empty());

                String newSampleName = sampleQueryResult.first().getName() + ".DELETED_" + TimeUtils.getTime();
                ObjectMap updateParams = new ObjectMap()
                        .append(SampleDBAdaptor.QueryParams.NAME.key(), newSampleName)
                        .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);
                queryResult = sampleDBAdaptor.update(sampleId, updateParams, QueryOptions.empty());

                auditManager.recordDeletion(AuditRecord.Resource.sample, sampleId, resourceId.getUser(), sampleQueryResult.first(),
                        queryResult.first(), null, null);

                // Remove the references to the sample id from the array of files
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), resourceId.getStudyId());
                fileDBAdaptor.extractSampleFromFiles(query, Arrays.asList(sampleId));

            } catch (CatalogAuthorizationException e) {
                auditManager.recordDeletion(AuditRecord.Resource.sample, sampleId, resourceId.getUser(), null, e.getMessage(), null);

                queryResult = new QueryResult<>("Delete sample " + sampleId);
                queryResult.setErrorMsg(e.getMessage());
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
                queryResult = new QueryResult<>("Delete sample " + sampleId);
                queryResult.setErrorMsg(e.getMessage());
            } finally {
                queryResultList.add(queryResult);
            }
        }

        return queryResultList;
    }

//    public List<QueryResult<Sample>> restore(String sampleIdStr, QueryOptions options, String sessionId) throws CatalogException {
//        ParamUtils.checkParameter(sampleIdStr, "id");
//        options = ParamUtils.defaultObject(options, QueryOptions::new);
//
//        MyResourceIds resource = getIds(sampleIdStr, null, sessionId);
//
//        List<QueryResult<Sample>> queryResultList = new ArrayList<>(resource.getResourceIds().size());
//        for (Long sampleId : resource.getResourceIds()) {
//            QueryResult<Sample> queryResult = null;
//            try {
//                authorizationManager.checkSamplePermission(resource.getStudyId(), sampleId, resource.getUser(),
//                        SampleAclEntry.SamplePermissions.DELETE);
//                queryResult = sampleDBAdaptor.restore(sampleId, options);
//                auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.restore, AuditRecord.Magnitude.medium, sampleId,
//                        resource.getUser(), Status.DELETED, Status.READY, "Sample restore", new ObjectMap());
//            } catch (CatalogAuthorizationException e) {
//                auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.restore, AuditRecord.Magnitude.high, sampleId,
//                        resource.getUser(), null, null, e.getMessage(), null);
//                queryResult = new QueryResult<>("Restore sample " + sampleId);
//                queryResult.setErrorMsg(e.getMessage());
//            } catch (CatalogException e) {
//                e.printStackTrace();
//                queryResult = new QueryResult<>("Restore sample " + sampleId);
//                queryResult.setErrorMsg(e.getMessage());
//            } finally {
//                queryResultList.add(queryResult);
//            }
//        }
//
//        return queryResultList;
//    }
//
//    public List<QueryResult<Sample>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
//        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());
//        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);
//        List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
//        String sampleIdStr = StringUtils.join(sampleIds, ",");
//        return restore(sampleIdStr, options, sessionId);
//    }


    @Override
    public QueryResult<Sample> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        MyResourceId resource = getId(entryStr, studyStr, sessionId);

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            authorizationManager.checkSamplePermission(resource.getStudyId(), resource.getResourceId(), userId,
                    SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkSamplePermission(resource.getStudyId(), resource.getResourceId(), userId,
                    SampleAclEntry.SamplePermissions.UPDATE);
        }

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> SampleDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }
        if (parameters.containsKey(SampleDBAdaptor.UpdateParams.NAME.key())) {
            ParamUtils.checkAlias(parameters.getString(SampleDBAdaptor.UpdateParams.NAME.key()), "name",
                    configuration.getCatalog().getOffset());
        }

        if (StringUtils.isNotEmpty(parameters.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            Individual individual = null;

            String individualStr = parameters.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());

            // Look for the individual where the sample is assigned
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.SAMPLES_ID.key(), resource.getResourceId())
                    .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), resource.getStudyId());
            QueryOptions indOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    IndividualDBAdaptor.QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.SAMPLES.key()));
            QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, indOptions);

            if (NumberUtils.isCreatable(individualStr) && Long.parseLong(individualStr) <= 0) {
                // Take out sample from individual

                if (individualQueryResult.getNumResults() == 1) {
                    individual = individualQueryResult.first();

                    authorizationManager.checkIndividualPermission(resource.getStudyId(), individual.getId(), userId,
                            IndividualAclEntry.IndividualPermissions.UPDATE);

                    List<Sample> sampleList = new ArrayList<>(individual.getSamples().size() - 1);
                    for (Sample sample : individual.getSamples()) {
                        if (sample.getId() != resource.getResourceId()) {
                            sampleList.add(sample);
                        }
                    }

                    individual.setSamples(sampleList);
                } // else - nothing to do

            } else {
                // Obtain the individual where the sample is intended to be associated to
                QueryResult<Individual> newIndividualQueryResult = catalogManager.getIndividualManager().get(
                        String.valueOf(resource.getStudyId()), individualStr, indOptions, sessionId);

                if (newIndividualQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Individual " + individualStr + " not found");
                }

                // Check if the sample is not already assigned to other individual
                if (individualQueryResult.getNumResults() == 1) {
                    if (individualQueryResult.first().getId() != newIndividualQueryResult.first().getId()) {
                        throw new CatalogException("Cannot update sample. The sample is already associated to other individual ("
                                + individualQueryResult.first().getId() + "). Please, first remove the sample from the individual.");
                    }
                } else {
                    individual = newIndividualQueryResult.first();

                    authorizationManager.checkIndividualPermission(resource.getStudyId(), individual.getId(), userId,
                            IndividualAclEntry.IndividualPermissions.UPDATE);

                    // We can freely assign the sample to the individual
                    List<Sample> sampleList = new ArrayList<>(individual.getSamples().size() + 1);
                    sampleList.addAll(individual.getSamples());

                    // Get the current sample version
                    QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(resource.getResourceId(),
                            new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.VERSION.key()));

                    // Add current sample
                    sampleList.add(new Sample()
                            .setId(resource.getResourceId())
                            .setVersion(sampleQueryResult.first().getVersion()));

                    individual.setSamples(sampleList);
                }
            }

            if (individual != null) {
                // We need to update the sample array from the individual
                ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), individual.getSamples());
                try {
                    individualDBAdaptor.update(individual.getId(), params, QueryOptions.empty());
                } catch (CatalogDBException e) {
                    logger.error("Could not update sample information: {}", e.getMessage(), e);
                    throw new CatalogException("Could not update sample information: " + e.getMessage());
                }
            }

            parameters.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        List<VariableSet> variableSetList = checkUpdateAnnotationsAndExtractVariableSets(resource, parameters, sampleDBAdaptor);

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(resource.getStudyId()));
        }

        QueryResult<Sample> queryResult = sampleDBAdaptor.update(resource.getResourceId(), parameters, variableSetList, options);
        auditManager.recordUpdate(AuditRecord.Resource.sample, resource.getResourceId(), userId, parameters, null, null);

        addIndividualInformation(queryResult, resource.getStudyId(), options, sessionId);
        return queryResult;
    }

    private void addIndividualInformation(QueryResult<Sample> queryResult, long studyId, QueryOptions options, String sessionId) {
        if (options == null || options.getBoolean("lazy", true)) {
            return;
        }

        List<Long> sampleIds = queryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
        if (sampleIds.size() == 0) {
            return;
        }

        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleIds);
        try {
            QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(String.valueOf(studyId), query,
                    QueryOptions.empty(), sessionId);
            // We create a map of sampleId - corresponding Individual
            Map<Long, Individual> sampleIndividualMap = new HashMap<>();
            for (Individual individual : individualQueryResult.getResult()) {
                for (Sample sample : individual.getSamples()) {
                    sampleIndividualMap.put(sample.getId(), individual);
                }
            }

            // And now we set the corresponding individuals where possible
            for (Sample sample : queryResult.getResult()) {
                if (sampleIndividualMap.containsKey(sample.getId())) {
                    sample.setIndividual(sampleIndividualMap.get(sample.getId()));
                    if (sample.getAttributes() == null) {
                        sample.setAttributes(new HashMap<>());
                    }
                    sample.getAttributes().put("individual", sample.getIndividual());
                }
            }
        } catch (CatalogException e) {
            logger.error("Could not fetch individual information to complete sample result: {}", e.getMessage(), e);
            queryResult.setWarningMsg("Could not fetch individual information to complete sample result: " + e.getMessage());
        }

    }

    @Deprecated
    public QueryResult<Sample> update(Long sampleId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        return update(null, String.valueOf(sampleId), parameters, options, sessionId);
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Long studyId = studyManager.getId(userId, studyStr);

        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_SAMPLES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // Add study id to the query
        query.put(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            String individualStr = query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
            if (NumberUtils.isCreatable(individualStr) && Long.parseLong(individualStr) <= 0) {
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), -1);
            } else {
                MyResourceId resource = catalogManager.getIndividualManager().getId(individualStr, Long.toString(studyId), sessionId);
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), resource.getResourceId());
            }
            query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        QueryResult queryResult = sampleDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<SampleAclEntry>> getAcls(String studyStr, List<String> sampleList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        MyResourceIds resource = getIds(sampleList, studyStr, silent, sessionId);

        List<QueryResult<SampleAclEntry>> sampleAclList = new ArrayList<>(resource.getResourceIds().size());
        List<Long> resourceIds = resource.getResourceIds();
        for (int i = 0; i < resourceIds.size(); i++) {
            Long sampleId = resourceIds.get(i);
            try {
                QueryResult<SampleAclEntry> allSampleAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allSampleAcls =
                            authorizationManager.getSampleAcl(resource.getStudyId(), sampleId, resource.getUser(), member);
                } else {
                    allSampleAcls = authorizationManager.getAllSampleAcls(resource.getStudyId(), sampleId, resource.getUser());
                }
                allSampleAcls.setId(String.valueOf(sampleId));
                sampleAclList.add(allSampleAcls);
            } catch (CatalogException e) {
                if (silent) {
                    sampleAclList.add(new QueryResult<>(sampleList.get(i), 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return sampleAclList;
    }

    public List<QueryResult<SampleAclEntry>> updateAcl(String studyStr, List<String> sampleList, String memberIds,
                                                       Sample.SampleAclParams sampleAclParams, String sessionId) throws CatalogException {
        int count = 0;
        count += sampleList != null && !sampleList.isEmpty() ? 1 : 0;
        count += StringUtils.isNotEmpty(sampleAclParams.getIndividual()) ? 1 : 0;
        count += StringUtils.isNotEmpty(sampleAclParams.getCohort()) ? 1 : 0;
        count += StringUtils.isNotEmpty(sampleAclParams.getFile()) ? 1 : 0;

        if (count > 1) {
            throw new CatalogException("Update ACL: Only one of these parameters are allowed: sample, individual, file or cohort per "
                    + "query.");
        } else if (count == 0) {
            throw new CatalogException("Update ACL: At least one of these parameters should be provided: sample, individual, file or "
                    + "cohort");
        }

        if (sampleAclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(sampleAclParams.getPermissions())) {
            permissions = Arrays.asList(sampleAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, SampleAclEntry.SamplePermissions::valueOf);
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getIndividual())) {
            // Obtain the individual ids
            MyResourceIds ids = catalogManager.getIndividualManager().getIds(
                    Arrays.asList(StringUtils.split(sampleAclParams.getIndividual(), ",")), studyStr, sessionId);

            // I do this to make faster the search of the studyId when looking for the individuals
            studyStr = Long.toString(ids.getStudyId());

            Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key());
            QueryResult<Individual> indQueryResult = catalogManager.getIndividualManager().get(studyStr, query, options, sessionId);

            Set<String> sampleSet = new HashSet<>();
            for (Individual individual : indQueryResult.getResult()) {
                sampleSet.addAll(individual.getSamples().stream().map(Sample::getId).map(String::valueOf).collect(Collectors.toSet()));
            }
            sampleList = new ArrayList<>();
            sampleList.addAll(sampleSet);

            // I do this to make faster the search of the studyId when looking for the individuals
            studyStr = Long.toString(ids.getStudyId());
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getFile())) {
            // Obtain the file ids
            MyResourceIds ids = catalogManager.getFileManager().getIds(Arrays.asList(StringUtils.split(sampleAclParams.getFile(), ",")),
                    studyStr, sessionId);

            Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.SAMPLE_IDS.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(ids.getStudyId(), query, options, sessionId);

            Set<String> sampleSet = new HashSet<>();
            for (File file : fileQueryResult.getResult()) {
                sampleSet.addAll(file.getSamples().stream().map(Sample::getId).map(String::valueOf).collect(Collectors.toList()));
            }
            sampleList = new ArrayList<>();
            sampleList.addAll(sampleSet);

            // I do this to make faster the search of the studyId when looking for the individuals
            studyStr = Long.toString(ids.getStudyId());
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getCohort())) {
            // Obtain the cohort ids
            MyResourceIds ids = catalogManager.getCohortManager().getIds(Arrays.asList(StringUtils.split(sampleAclParams.getCohort(), ",")),
                    studyStr, sessionId);

            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());
            QueryResult<Cohort> cohortQueryResult = catalogManager.getCohortManager().get(ids.getStudyId(), query, options, sessionId);

            Set<String> sampleSet = new HashSet<>();
            for (Cohort cohort : cohortQueryResult.getResult()) {
                sampleSet.addAll(cohort.getSamples().stream().map(Sample::getId).map(String::valueOf).collect(Collectors.toList()));
            }
            sampleList = new ArrayList<>();
            sampleList.addAll(sampleSet);

            // I do this to make faster the search of the studyId when looking for the individuals
            studyStr = Long.toString(ids.getStudyId());
        }

        MyResourceIds resourceIds = getIds(sampleList, studyStr, sessionId);
        authorizationManager.checkCanAssignOrSeePermissions(resourceIds.getStudyId(), resourceIds.getUser());

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resourceIds.getStudyId(), members);

        List<QueryResult<SampleAclEntry>> queryResults;
        switch (sampleAclParams.getAction()) {
            case SET:
                queryResults = authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.SET, StringUtils.join(resourceIds.getResourceIds(), ","), false);

                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            case ADD:
                queryResults = authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.ADD, StringUtils.join(resourceIds.getResourceIds(), ","), false);
                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            case REMOVE:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.REMOVE, StringUtils.join(resourceIds.getResourceIds(), ","), false);

                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            case RESET:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.RESET, StringUtils.join(resourceIds.getResourceIds(), ","), false);

                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }

        return queryResults;
    }


    // **************************   Private methods  ******************************** //

    void checkCanDeleteSamples(MyResourceIds resources) throws CatalogException {
        for (Long sampleId : resources.getResourceIds()) {
            authorizationManager.checkSamplePermission(resources.getStudyId(), sampleId, resources.getUser(),
                    SampleAclEntry.SamplePermissions.DELETE);
        }

        // Check that the samples are not being used in cohorts
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), resources.getStudyId())
                .append(CohortDBAdaptor.QueryParams.SAMPLE_IDS.key(), resources.getResourceIds())
                .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED);
        long count = cohortDBAdaptor.count(query).first();
        if (count > 0) {
            if (resources.getResourceIds().size() == 1) {
                throw new CatalogException("The sample " + resources.getResourceIds().get(0) + " is part of " + count + " cohorts. Please, "
                        + "first update or delete the cohorts");
            } else {
                throw new CatalogException("Some samples are part of " + count + " cohorts. Please, first update or delete the cohorts");
            }
        }
    }

//    private void addIndividualInformation(QueryResult<Sample> sampleQueryResult, String sessionId) {
//        if (sampleQueryResult.getNumResults() == 0) {
//            return;
//        }
//        String individualIdList = sampleQueryResult.getResult().stream()
//                .map(Sample::getIndividual).filter(Objects::nonNull)
//                .map(Individual::getId).filter(id -> id > 0)
//                .map(String::valueOf)
//                .collect(Collectors.joining(","));
//        if (StringUtils.isEmpty(individualIdList)) {
//            return;
//        }
//        try {
//            QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(null, individualIdList,
//                    QueryOptions.empty(), sessionId);
//
//            // We create a map of individualId - Individual
//            Map<Long, Individual> individualMap = new HashMap<>();
//            for (Individual individual : individualQueryResult.getResult()) {
//                individualMap.put(individual.getId(), individual);
//            }
//
//            // And set the individual information in the sample result
//            for (Sample sample : sampleQueryResult.getResult()) {
//                if (sample.getIndividual() != null && sample.getIndividual().getId() > 0) {
//                    sample.setIndividual(individualMap.get(sample.getIndividual().getId()));
//                }
//            }
//
//        } catch (CatalogException e) {
//            logger.warn("Could not retrieve individual information to complete sample object, {}", e.getMessage(), e);
//            sampleQueryResult.setWarningMsg("Could not retrieve individual information to complete sample object" + e.getMessage());
//        }
//    }

    private long getSampleId(boolean silent, String sampleStrAux) throws CatalogException {
        long sampleId = Long.parseLong(sampleStrAux);
        try {
            sampleDBAdaptor.checkId(sampleId);
        } catch (CatalogException e) {
            if (silent) {
                return -1L;
            } else {
                throw e;
            }
        }
        return sampleId;
    }
}
