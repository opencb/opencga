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
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.ISampleManager;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.AnnotationManager;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
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
public class SampleManager extends AbstractManager implements ISampleManager {

    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);
    private IUserManager userManager;

    @Deprecated
    public SampleManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                         DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         Properties catalogProperties) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public SampleManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                         DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
        this.userManager = catalogManager.getUserManager();
    }

    @Override
    public Long getStudyId(long sampleId) throws CatalogException {
        return sampleDBAdaptor.getStudyId(sampleId);
    }

    @Override
    public Long getId(String userId, String sampleStr) throws CatalogException {
        if (StringUtils.isNumeric(sampleStr)) {
            return Long.parseLong(sampleStr);
        }

        ObjectMap parsedSampleStr = parseFeatureId(userId, sampleStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String sampleName = parsedSampleStr.getString("featureName");

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(SampleDBAdaptor.QueryParams.NAME.key(), sampleName)
                .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.samples.id");
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one sample id found based on " + sampleName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Deprecated
    @Override
    public Long getId(String id) throws CatalogException {
        if (StringUtils.isNumeric(id)) {
            return Long.parseLong(id);
        }

        Query query = new Query(SampleDBAdaptor.QueryParams.NAME.key(), id);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());

        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, options);
        if (sampleQueryResult.getNumResults() == 1) {
            return sampleQueryResult.first().getId();
        } else {
            return -1L;
        }
    }

    @Override
    public QueryResult<Sample> create(String studyStr, Sample sample, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkAlias(sample.getName(), "name", configuration.getCatalog().getOffset());
        sample.setSource(ParamUtils.defaultString(sample.getSource(), ""));
        sample.setDescription(ParamUtils.defaultString(sample.getDescription(), ""));
        sample.setType(ParamUtils.defaultString(sample.getType(), ""));
        sample.setAcl(Collections.emptyList());
        sample.setOntologyTerms(ParamUtils.defaultObject(sample.getOntologyTerms(), Collections.emptyList()));
        sample.setAnnotationSets(ParamUtils.defaultObject(sample.getAnnotationSets(), Collections.emptyList()));
        sample.setAnnotationSets(AnnotationManager.validateAnnotationSets(sample.getAnnotationSets(), studyDBAdaptor));
        sample.setAttributes(ParamUtils.defaultObject(sample.getAttributes(), Collections.emptyMap()));
        sample.setStatus(new Status());
        sample.setCreationDate(TimeUtils.getTime());

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_SAMPLES);

        sample.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));

        long individualId = 0;

        if (sample.getIndividual() != null) {
            if (sample.getIndividual().getId() > 0) {
                individualDBAdaptor.checkId(sample.getIndividual().getId());

                // Check studyId of the individual
                long studyIdIndividual = individualDBAdaptor.getStudyId(sample.getIndividual().getId());
                if (studyId != studyIdIndividual) {
                    throw new CatalogException("Cannot associate sample from one study with an individual of a different study.");
                }

                individualId = sample.getIndividual().getId();
            } else {
                if (StringUtils.isEmpty(sample.getIndividual().getName())) {
                    throw new CatalogException("Missing individual name. If the sample is not intended to be associated to any "
                            + "individual, please do not include any individual parameter.");
                }

                Query query = new Query()
                        .append(IndividualDBAdaptor.QueryParams.NAME.key(), sample.getIndividual().getName())
                        .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                QueryOptions options1 = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
                QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options1);
                if (individualQueryResult.getNumResults() == 1) {
                    // We set the id
                    individualId = individualQueryResult.first().getId();
                } else {
                    // We create the individual
                    individualQueryResult = catalogManager.getIndividualManager().create(Long.toString(studyId),
                            sample.getIndividual(), new QueryOptions(), sessionId);

                    if (individualQueryResult.getNumResults() == 0) {
                        throw new CatalogException("Unexpected error occurred when creating the individual");
                    } else {
                        // We set the id
                        individualId = individualQueryResult.first().getId();
                    }
                }
            }
        }

        if (individualId > 0) {
            sample.setIndividual(new Individual().setId(individualId));
        }

        QueryResult<Sample> queryResult = sampleDBAdaptor.insert(sample, studyId, options);
        auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    public QueryResult<Sample> create(String studyStr, String name, String source, String description, String type, boolean somatic,
                                      Individual individual, Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(name, "name", configuration.getCatalog().getOffset());
        source = ParamUtils.defaultString(source, "");
        description = ParamUtils.defaultString(description, "");
        type = ParamUtils.defaultString(type, "");
        attributes = ParamUtils.defaultObject(attributes, Collections.<String, Object>emptyMap());

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_SAMPLES);

        if (individual != null) {
            if (individual.getId() <= 0) {
                individual.setId(catalogManager.getIndividualManager().getId(individual.getName(), Long.toString(studyId), sessionId)
                        .getResourceId());
            }
        }

        Sample sample = new Sample(-1, name, source, individual, description, type, somatic,
                catalogManager.getStudyManager().getCurrentRelease(studyId), Collections.emptyList(), Collections.emptyList(),
                new ArrayList<>(), attributes);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        QueryResult<Sample> queryResult = sampleDBAdaptor.insert(sample, studyId, options);
//        auditManager.recordCreation(AuditRecord.Resource.sample, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Annotation> load(File file) throws CatalogException {
        throw new UnsupportedOperationException();
    }


    @Deprecated
    @Override
    public QueryResult<Sample> create(String studyStr, String name, String source, String description,
                                      Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        return create(studyStr, name, source, description, null, false, null, attributes, options, sessionId);
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
            userId = userManager.getId(sessionId);
        } else {
            if (sampleStr.contains(",")) {
                throw new CatalogException("More than one sample found");
            }

            userId = userManager.getId(sessionId);
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
    public MyResourceIds getIds(String sampleStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(sampleStr)) {
            throw new CatalogException("Missing sample parameter");
        }

        String userId;
        long studyId;
        List<Long> sampleIds = new ArrayList<>();

        if (StringUtils.isNumeric(sampleStr) && Long.parseLong(sampleStr) > configuration.getCatalog().getOffset()) {
            sampleIds = Arrays.asList(Long.parseLong(sampleStr));
            sampleDBAdaptor.exists(sampleIds.get(0));
            studyId = sampleDBAdaptor.getStudyId(sampleIds.get(0));
            userId = userManager.getId(sessionId);
        } else {
            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            List<String> sampleSplit = Arrays.asList(sampleStr.split(","));
            for (String sampleStrAux : sampleSplit) {
                if (StringUtils.isNumeric(sampleStrAux)) {
                    long sampleId = Long.parseLong(sampleStrAux);
                    sampleDBAdaptor.exists(sampleId);
                    sampleIds.add(sampleId);
                }
            }

            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(SampleDBAdaptor.QueryParams.NAME.key(), sampleSplit);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());
            QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);

            if (sampleQueryResult.getNumResults() > 0) {
                sampleIds.addAll(sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList()));
            }

            if (sampleIds.size() < sampleSplit.size()) {
                throw new CatalogException("Found only " + sampleIds.size() + " out of the " + sampleSplit.size()
                        + " samples looked for in study " + studyStr);
            }
        }

        return new MyResourceIds(userId, studyId, sampleIds);
    }

    @Override
    public QueryResult<Sample> get(Long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, options, userId);
        if (sampleQueryResult.getNumResults() <= 0) {
            throw CatalogAuthorizationException.deny(userId, "view", "sample", sampleId, "");
        }
        return sampleQueryResult;
    }

    @Override
    public QueryResult<Sample> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);

        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    @Override
    public DBIterator<Sample> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);

        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        return sampleDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<Sample> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager().getIds(
                    query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()), Long.toString(studyId), sessionId);
            query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), resourceIds.getResourceIds());
            query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        query.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    @Override
    public QueryResult<Sample> count(String studyStr, Query query, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager().getIds(
                    query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()), Long.toString(studyId), sessionId);
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
    @Override
    public List<QueryResult<Sample>> delete(String sampleIdStr, @Nullable String studyStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(sampleIdStr, "id");
//        options = ParamUtils.defaultObject(options, QueryOptions::new);
        MyResourceIds resourceId = getIds(sampleIdStr, studyStr, sessionId);

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
                queryResult = sampleDBAdaptor.update(sampleId, updateParams);

                auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.delete, AuditRecord.Magnitude.high, sampleId,
                        resourceId.getUser(), sampleQueryResult.first(), queryResult.first(), "", null);

                // Remove the references to the sample id from the array of files
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), resourceId.getStudyId());
                fileDBAdaptor.extractSampleFromFiles(query, Arrays.asList(sampleId));

            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.delete, AuditRecord.Magnitude.high, sampleId,
                        resourceId.getUser(), null, null, e.getMessage(), null);
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

    @Override
    public void checkCanDeleteSamples(MyResourceIds resources) throws CatalogException {
        for (Long sampleId : resources.getResourceIds()) {
            authorizationManager.checkSamplePermission(resources.getStudyId(), sampleId, resources.getUser(),
                    SampleAclEntry.SamplePermissions.DELETE);
        }

        // Check that the samples are not being used in cohorts
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), resources.getStudyId())
                .append(CohortDBAdaptor.QueryParams.SAMPLE_IDS.key(), resources.getResourceIds());
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

    @Override
    public List<QueryResult<Sample>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);
        List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
        String sampleIdStr = StringUtils.join(sampleIds, ",");
        return delete(sampleIdStr, null, options, sessionId);
    }

    @Override
    public List<QueryResult<Sample>> restore(String sampleIdStr, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sampleIdStr, "id");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceIds resource = getIds(sampleIdStr, null, sessionId);

        List<QueryResult<Sample>> queryResultList = new ArrayList<>(resource.getResourceIds().size());
        for (Long sampleId : resource.getResourceIds()) {
            QueryResult<Sample> queryResult = null;
            try {
                authorizationManager.checkSamplePermission(resource.getStudyId(), sampleId, resource.getUser(),
                        SampleAclEntry.SamplePermissions.DELETE);
                queryResult = sampleDBAdaptor.restore(sampleId, options);
                auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.restore, AuditRecord.Magnitude.medium, sampleId,
                        resource.getUser(), Status.DELETED, Status.READY, "Sample restore", new ObjectMap());
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.restore, AuditRecord.Magnitude.high, sampleId,
                        resource.getUser(), null, null, e.getMessage(), null);
                queryResult = new QueryResult<>("Restore sample " + sampleId);
                queryResult.setErrorMsg(e.getMessage());
            } catch (CatalogException e) {
                e.printStackTrace();
                queryResult = new QueryResult<>("Restore sample " + sampleId);
                queryResult.setErrorMsg(e.getMessage());
            } finally {
                queryResultList.add(queryResult);
            }
        }

        return queryResultList;
    }

    @Override
    public List<QueryResult<Sample>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);
        List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
        String sampleIdStr = StringUtils.join(sampleIds, ",");
        return restore(sampleIdStr, options, sessionId);
    }

    @Override
    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        throw new NotImplementedException("Project: Operation not yet supported");
    }


    @Override
    public QueryResult<Sample> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(query, "query");
        QueryResult<Sample> result =
                get(query.getLong(SampleDBAdaptor.QueryParams.STUDY_ID.key(), -1), query, options, sessionId);
//        auditManager.recordRead(AuditRecord.Resource.sample, , userId, parameters, null, null);
        return result;
    }

    @Override
    public QueryResult<Sample> update(Long sampleId, ObjectMap parameters, QueryOptions options, String sessionId) throws
            CatalogException {
        parameters = ParamUtils.defaultObject(parameters, ObjectMap::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        authorizationManager.checkSamplePermission(studyId, sampleId, userId, SampleAclEntry.SamplePermissions.UPDATE);

        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            SampleDBAdaptor.QueryParams queryParam = SampleDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case NAME:
                    ParamUtils.checkAlias(parameters.getString(queryParam.key()), "name", configuration.getCatalog().getOffset());
                    break;
                case SOURCE:
                case INDIVIDUAL:
                case TYPE:
                case SOMATIC:
                case DESCRIPTION:
                case ATTRIBUTES:
                case ONTOLOGY_TERMS:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        if (StringUtils.isNotEmpty(parameters.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            String individualStr = parameters.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
            if (NumberUtils.isCreatable(individualStr) && Long.parseLong(individualStr) <= 0) {
                parameters.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), -1);
            } else {
                MyResourceId resource = catalogManager.getIndividualManager().getId(individualStr, Long.toString(studyId), sessionId);
                parameters.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), resource.getResourceId());
            }
            parameters.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        QueryResult<Sample> queryResult = sampleDBAdaptor.update(sampleId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, parameters, null, null);
        return queryResult;

    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_SAMPLES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
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

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_SAMPLES);

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

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public List<QueryResult<SampleAclEntry>> updateAcl(String sample, String studyStr, String memberIds,
                                                       Sample.SampleAclParams sampleAclParams, String sessionId) throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(sample) ? 1 : 0;
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
            MyResourceIds ids = catalogManager.getIndividualManager().getIds(sampleAclParams.getIndividual(), studyStr, sessionId);

            Query query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());
            QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(ids.getStudyId(), query, options, sessionId);

            Set<Long> sampleSet = sampleQueryResult.getResult().stream().map(Sample::getId)
                    .collect(Collectors.toSet());
            sample = StringUtils.join(sampleSet, ",");

            // I do this to make faster the search of the studyId when looking for the individuals
            studyStr = Long.toString(ids.getStudyId());
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getFile())) {
            // Obtain the file ids
            MyResourceIds ids = catalogManager.getFileManager().getIds(sampleAclParams.getFile(), studyStr, sessionId);

            Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.SAMPLE_IDS.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(ids.getStudyId(), query, options, sessionId);

            Set<Long> sampleSet = new HashSet<>();
            for (File file : fileQueryResult.getResult()) {
                sampleSet.addAll(file.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            }
            sample = StringUtils.join(sampleSet, ",");

            // I do this to make faster the search of the studyId when looking for the individuals
            studyStr = Long.toString(ids.getStudyId());
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getCohort())) {
            // Obtain the cohort ids
            MyResourceIds ids = catalogManager.getCohortManager().getIds(sampleAclParams.getCohort(), studyStr, sessionId);

            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());
            QueryResult<Cohort> cohortQueryResult = catalogManager.getCohortManager().get(ids.getStudyId(), query, options, sessionId);

            Set<Long> sampleSet = new HashSet<>();
            for (Cohort cohort : cohortQueryResult.getResult()) {
                sampleSet.addAll(cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            }
            sample = StringUtils.join(sampleSet, ",");

            // I do this to make faster the search of the studyId when looking for the individuals
            studyStr = Long.toString(ids.getStudyId());
        }

        MyResourceIds resourceIds = getIds(sample, studyStr, sessionId);

        // Check the user has the permissions needed to change permissions over those samples
        for (Long sampleId : resourceIds.getResourceIds()) {
            authorizationManager.checkSamplePermission(resourceIds.getStudyId(), sampleId, resourceIds.getUser(),
                    SampleAclEntry.SamplePermissions.SHARE);
        }

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        CatalogMemberValidator.checkMembers(catalogDBAdaptorFactory, resourceIds.getStudyId(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        String collectionName = MongoDBAdaptorFactory.SAMPLE_COLLECTION;

        List<QueryResult<SampleAclEntry>> queryResults;
        switch (sampleAclParams.getAction()) {
            case SET:
                queryResults = authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
                if (sampleAclParams.isPropagate()) {
                    Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                            AclParams.Action.SET, StringUtils.join(resourceIds.getResourceIds(), ","), false);
                    catalogManager.getIndividualManager().updateAcl(null, studyStr, memberIds, aclParams, sessionId);
                }
                break;
            case ADD:
                queryResults = authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
                if (sampleAclParams.isPropagate()) {
                    Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                            AclParams.Action.ADD, StringUtils.join(resourceIds.getResourceIds(), ","), false);
                    catalogManager.getIndividualManager().updateAcl(null, studyStr, memberIds, aclParams, sessionId);
                }
                break;
            case REMOVE:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
                if (sampleAclParams.isPropagate()) {
                    Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                            AclParams.Action.REMOVE, StringUtils.join(resourceIds.getResourceIds(), ","), false);
                    catalogManager.getIndividualManager().updateAcl(null, studyStr, memberIds, aclParams, sessionId);
                }
                break;
            case RESET:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, collectionName);
                if (sampleAclParams.isPropagate()) {
                    Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                            AclParams.Action.RESET, StringUtils.join(resourceIds.getResourceIds(), ","), false);
                    catalogManager.getIndividualManager().updateAcl(null, studyStr, memberIds, aclParams, sessionId);
                }
                break;
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }

        return queryResults;
    }

    private MyResourceId commonGetAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        return catalogManager.getSampleManager().getId(id, studyStr, sessionId);
//        authorizationManager.checkSamplePermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS);
//        return resourceId.getResourceId();
    }

    private MyResourceId commonGetAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkAlias(annotationSetName, "annotationSetName", configuration.getCatalog().getOffset());
        return catalogManager.getSampleManager().getId(id, studyStr, sessionId);
//        authorizationManager.checkSamplePermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS);
//        return resourceId.getResourceId();
    }

    @Override
    public QueryResult<AnnotationSet> createAnnotationSet(String id, @Nullable String studyStr, String variableSetId,
                                                          String annotationSetName, Map<String, Object> annotations,
                                                          Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        MyResourceId resourceId = catalogManager.getSampleManager().getId(id, studyStr, sessionId);
        authorizationManager.checkSamplePermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
                SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS);
        MyResourceId variableSetResource = catalogManager.getStudyManager().getVariableSetId(variableSetId,
                Long.toString(resourceId.getStudyId()), sessionId);
        QueryResult<VariableSet> variableSet = studyDBAdaptor.getVariableSet(variableSetResource.getResourceId(), null,
                resourceId.getUser(), null);
        if (variableSet.getNumResults() == 0) {
            // Variable set must be confidential and the user does not have those permissions
            throw new CatalogAuthorizationException("Permission denied: User " + resourceId.getUser() + " cannot create annotations over "
                    + "that variable set");
        }

        QueryResult<AnnotationSet> annotationSet = AnnotationManager.createAnnotationSet(resourceId.getResourceId(), variableSet.first(),
                annotationSetName, annotations, catalogManager.getStudyManager().getCurrentRelease(resourceId.getStudyId()), attributes,
                sampleDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.sample, resourceId.getResourceId(), resourceId.getUser(),
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> getAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        MyResourceId resource = commonGetAllAnnotationSets(id, studyStr, sessionId);
        return sampleDBAdaptor.getAnnotationSet(resource, null,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAllAnnotationSets(id, studyStr, sessionId);
        return sampleDBAdaptor.getAnnotationSetAsMap(resource, null,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return sampleDBAdaptor.getAnnotationSet(resource, annotationSetName,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<ObjectMap> getAnnotationSetAsMap(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return sampleDBAdaptor.getAnnotationSetAsMap(resource, annotationSetName,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr, String annotationSetName,
                                                          Map<String, Object> newAnnotations, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        MyResourceId resourceId = catalogManager.getSampleManager().getId(id, studyStr, sessionId);
        authorizationManager.checkSamplePermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
                SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS);

        // Update the annotation
        QueryResult<AnnotationSet> queryResult =
                AnnotationManager.updateAnnotationSet(resourceId, annotationSetName, newAnnotations, sampleDBAdaptor, studyDBAdaptor);

        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("There was an error with the update");
        }

        AnnotationSet annotationSet = queryResult.first();

        // Audit the changes
        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getCreationDate(), 1, null);
        auditManager.recordUpdate(AuditRecord.Resource.sample, resourceId.getResourceId(), resourceId.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSetUpdate)), "update annotation", null);

        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String
            sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");

        MyResourceId resourceId = catalogManager.getSampleManager().getId(id, studyStr, sessionId);
        authorizationManager.checkSamplePermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
                SampleAclEntry.SamplePermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> annotationSet = sampleDBAdaptor.getAnnotationSet(resourceId.getResourceId(), annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }
        // We make this query because it will check the proper permissions in case the variable set is confidential
        studyDBAdaptor.getVariableSet(annotationSet.first().getVariableSetId(), new QueryOptions(), resourceId.getUser(), null);

        sampleDBAdaptor.deleteAnnotationSet(resourceId.getResourceId(), annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.sample, resourceId.getResourceId(), resourceId.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }

    @Override
    public QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, @Nullable String studyStr, String variableSetStr,
                                                           @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");

        AbstractManager.MyResourceId resourceId = getId(id, studyStr, sessionId);
//        authorizationManager.checkSamplePermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS);


        long variableSetId = -1;
        if (StringUtils.isNotEmpty(variableSetStr)) {
            variableSetId = catalogManager.getStudyManager()
                    .getVariableSetId(variableSetStr, Long.toString(resourceId.getStudyId()), sessionId).getResourceId();
        }

        return sampleDBAdaptor.searchAnnotationSetAsMap(resourceId, variableSetId, annotation,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> searchAnnotationSet(String id, @Nullable String studyStr, String variableSetStr,
                                                          @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");

        AbstractManager.MyResourceId resourceId = getId(id, studyStr, sessionId);
//        authorizationManager.checkSamplePermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS);


        long variableSetId = -1;
        if (StringUtils.isNotEmpty(variableSetStr)) {
            variableSetId = catalogManager.getStudyManager()
                    .getVariableSetId(variableSetStr, Long.toString(resourceId.getStudyId()), sessionId).getResourceId();
        }

        return sampleDBAdaptor.searchAnnotationSet(resourceId, variableSetId, annotation,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.toString());
    }
}
