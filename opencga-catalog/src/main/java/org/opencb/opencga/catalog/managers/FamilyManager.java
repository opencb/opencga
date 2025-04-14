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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.*;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by pfurio on 02/05/17.
 */
public class FamilyManager extends AnnotationSetManager<Family, FamilyPermissions> {

    public static final QueryOptions INCLUDE_FAMILY_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.UUID.key(),
            FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final QueryOptions INCLUDE_FAMILY_MEMBERS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.UUID.key(),
            FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
            FamilyDBAdaptor.QueryParams.MEMBERS.key()));
    protected static Logger logger = LoggerFactory.getLogger(FamilyManager.class);
    private final String defaultFacet = "creationYear>>creationMonth;status;phenotypes;expectedSize;numMembers[0..20]:2";
    private UserManager userManager;
    private StudyManager studyManager;

    FamilyManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                  CatalogManager catalogManager, DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    public static Pedigree getPedigreeFromFamily(Family family, String probandId) {
        List<Individual> members = family.getMembers();
        Map<String, Member> individualMap = new HashMap<>();

        // Parse all the individuals
        for (Individual member : members) {
            Member individual = new Member(member.getId(), member.getName(), null, null, null, member.getSex(), member.getLifeStatus(),
                    member.getPhenotypes(), member.getDisorders(), member.getAttributes());
            individualMap.put(individual.getId(), individual);
        }

        // Fill parent information
        for (Individual member : members) {
            if (member.getFather() != null && StringUtils.isNotEmpty(member.getFather().getId())) {
                individualMap.get(member.getId()).setFather(individualMap.get(member.getFather().getId()));
            }
            if (member.getMother() != null && StringUtils.isNotEmpty(member.getMother().getId())) {
                individualMap.get(member.getId()).setMother(individualMap.get(member.getMother().getId()));
            }
        }

        Member proband = null;
        if (StringUtils.isNotEmpty(probandId)) {
            proband = individualMap.get(probandId);
        }

        List<Member> individuals = new ArrayList<>(individualMap.values());
        return new Pedigree(family.getId(), individuals, proband, family.getPhenotypes(), family.getDisorders(), family.getAttributes());
    }

    @Override
    Enums.Resource getResource() {
        return Enums.Resource.FAMILY;
    }

    @Override
    InternalGetDataResult<Family> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
                                              QueryOptions options, String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing family entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(FamilyDBAdaptor.QueryParams.VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one family allowed when requesting multiple versions");
        }

        FamilyDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Family> familyDataResult = getFamilyDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        Function<Family, String> familyStringFunction = Family::getId;
        if (idQueryParam.equals(FamilyDBAdaptor.QueryParams.UUID)) {
            familyStringFunction = Family::getUuid;
        }

        if (ignoreException || familyDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, familyStringFunction, familyDataResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Family> resultsNoCheck = getFamilyDBAdaptor(organizationId).get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == familyDataResult.getNumResults()) {
            throw CatalogException.notFound("families",
                    getMissingFields(uniqueList, familyDataResult.getResults(), familyStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the families.");
        }
    }

    FamilyDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        FamilyDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            FamilyDBAdaptor.QueryParams param = FamilyDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = FamilyDBAdaptor.QueryParams.UUID;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        return idQueryParam;
    }

    private OpenCGAResult<Family> getFamily(String organizationId, long studyUid, String familyUuid, QueryOptions options)
            throws CatalogException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FamilyDBAdaptor.QueryParams.UUID.key(), familyUuid);
        return getFamilyDBAdaptor(organizationId).get(query, options);
    }

    @Override
    public DBIterator<Family> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return iterator(studyStr, query, options, StudyManager.INCLUDE_VARIABLE_SET, token, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();

            fixQueryObject(organizationId, study, finalQuery, userId, token);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
            AnnotationUtils.fixQueryOptionAnnotation(finalOptions);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getFamilyDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, finalOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        return facet(studyStr, query, facet, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(organizationId, study, finalQuery, userId, token);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getFamilyDBAdaptor(organizationId).facet(study.getUid(), finalQuery, facet, userId);
        });
    }

    @Override
    public OpenCGAResult<Family> create(String studyStr, Family family, QueryOptions options, String token) throws CatalogException {
        return create(studyStr, family, null, options, token);
    }

    public OpenCGAResult<Family> create(String studyStr, Family family, List<String> members, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap methodParams = new ObjectMap()
                .append("members", members);
        return create(methodParams, studyStr, family, options, token, StudyManager.INCLUDE_VARIABLE_SET,
                (organizationId, study, userId, entryParam) -> {
                    entryParam.setId(family.getId());
                    authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId,
                            StudyPermissions.Permissions.WRITE_FAMILIES);

                    ParamUtils.checkObj(family, "family");
                    ParamUtils.checkIdentifier(family.getId(), "id");
                    family.setName(ParamUtils.defaultObject(family.getName(), family.getId()));
                    family.setMembers(ParamUtils.defaultObject(family.getMembers(), Collections.emptyList()));
                    family.setPhenotypes(ParamUtils.defaultObject(family.getPhenotypes(), Collections.emptyList()));
                    family.setDisorders(ParamUtils.defaultObject(family.getDisorders(), Collections.emptyList()));
                    family.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(family.getCreationDate(),
                            FamilyDBAdaptor.QueryParams.CREATION_DATE.key()));
                    family.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(family.getModificationDate(),
                            FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
                    family.setDescription(ParamUtils.defaultString(family.getDescription(), ""));
                    family.setInternal(FamilyInternal.init());
                    family.setAnnotationSets(ParamUtils.defaultObject(family.getAnnotationSets(), Collections.emptyList()));
                    family.setStatus(ParamUtils.defaultObject(family.getStatus(), Status::new));
                    family.setQualityControl(ParamUtils.defaultObject(family.getQualityControl(), FamilyQualityControl::new));
                    family.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
                    family.setVersion(1);
                    family.setAttributes(ParamUtils.defaultObject(family.getAttributes(), Collections.emptyMap()));

                    // Check the id is not in use
                    Query query = new Query()
                            .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(FamilyDBAdaptor.QueryParams.ID.key(), family.getId());
                    if (getFamilyDBAdaptor(organizationId).count(query).getNumMatches() > 0) {
                        throw new CatalogException("Family '" + family.getId() + "' already exists.");
                    }
                    validateNewAnnotationSets(study.getVariableSets(), family.getAnnotationSets());

                    List<Individual> existingMembers = autoCompleteFamilyMembers(organizationId, study, family, members, userId);
                    validateFamily(family, existingMembers);
                    validatePhenotypes(family, existingMembers);
                    validateDisorders(family, existingMembers);

                    QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
                    family.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FAMILY));
                    entryParam.setUuid(family.getUuid());

                    OpenCGAResult<Family> insert = getFamilyDBAdaptor(organizationId).insert(study.getUid(), family, existingMembers,
                            study.getVariableSets(), finalOptions);
                    if (finalOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                        // Fetch updated family
                        OpenCGAResult<Family> queryResult = getFamily(organizationId, study.getUid(), family.getUuid(), finalOptions);
                        insert.setResults(queryResult.getResults());
                    }
                    return insert;
                });
    }

    public OpenCGAResult<Family> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        return search(studyId, query, options, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();

            fixQueryObject(organizationId, study, finalQuery, userId, token);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
            AnnotationUtils.fixQueryOptionAnnotation(finalOptions);

            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getFamilyDBAdaptor(organizationId).get(study.getUid(), finalQuery, finalOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException {
        return distinct(studyId, fields, query, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();

            fixQueryObject(organizationId, study, finalQuery, userId, token);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getFamilyDBAdaptor(organizationId).distinct(study.getUid(), fields, finalQuery, userId);
        });
    }

    private void fixQueryObject(String organizationId, Study study, Query query, String userId, String token) throws CatalogException {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.FAMILY_INTERNAL_STATUS_PARAM, FamilyDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());
        changeQueryId(query, ParamConstants.FAMILY_STATUS_PARAM, FamilyDBAdaptor.QueryParams.STATUS_ID.key());

        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MEMBERS.key()))
                && StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()))) {
            throw new CatalogException("Cannot look for samples and members at the same time");
        }

        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        // We change the MEMBERS parameters for MEMBER_UID which is what the DBAdaptor understands
        if (query.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
            List<Individual> memberList = catalogManager.getIndividualManager().internalGet(organizationId, study.getUid(),
                    query.getAsStringList(FamilyDBAdaptor.QueryParams.MEMBERS.key()), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId,
                    true).getResults();
            if (ListUtils.isNotEmpty(memberList)) {
                query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), memberList.stream().map(Individual::getUid)
                        .collect(Collectors.toList()));
            } else {
                // Add -1 to query so no results are obtained
                query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), -1);
            }

            query.remove(FamilyDBAdaptor.QueryParams.MEMBERS.key());
        }

        // We look for the individuals containing those samples
        if (query.containsKey(IndividualDBAdaptor.QueryParams.SAMPLES.key())) {
            if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()))) {
                Query newQuery = new Query()
                        .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                                query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()));
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.UID.key());
                OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager()
                        .search(study.getFqn(), newQuery, options, token);

                if (individualResult.getNumResults() == 0) {
                    // Add -1 to query so no results are obtained
                    query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), -1);
                } else {
                    // Look for the individuals containing those samples
                    query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(),
                            individualResult.getResults().stream().map(Individual::getUid).collect(Collectors.toList()));
                }
            }

            query.remove(IndividualDBAdaptor.QueryParams.SAMPLES.key());
        }
    }

    public OpenCGAResult<Family> count(String studyId, Query query, String token) throws CatalogException {
        return count(studyId, query, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
            fixQueryObject(organizationId, study, finalQuery, userId, token);

            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = getFamilyDBAdaptor(organizationId).count(finalQuery, userId);

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        });
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> familyIds, QueryOptions options, String token)
            throws CatalogException {
        return delete(studyStr, familyIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> familyIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, familyIds, params, ignoreException, token, (organizationId, study, userId, entryParam) -> {
            if (StringUtils.isEmpty(entryParam.getId())) {
                throw new CatalogException("Internal error: Missing family id. This family id should have been provided internally.");
            }
            String familyId = entryParam.getId();

            Query query = new Query();
            authorizationManager.buildAclCheckQuery(userId, IndividualPermissions.DELETE.name(), query);
            OpenCGAResult<Family> internalResult = internalGet(organizationId, study.getUid(), familyId, INCLUDE_FAMILY_IDS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Family '" + familyId + "' not found");
            }

            Family family = internalResult.first();
            // We set the proper values for entry param object
            entryParam.setId(family.getId());
            entryParam.setUuid(family.getUuid());

            // Check family can be deleted
            checkCanBeDeleted(organizationId, study, family);

            // Delete the family
            return getFamilyDBAdaptor(organizationId).delete(family);
        });
    }

    private void checkCanBeDeleted(String organizationId, Study study, Family family) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(), family.getUid());
        OpenCGAResult<ClinicalAnalysis> result = getClinicalAnalysisDBAdaptor(organizationId).get(query,
                ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS);
        if (result.getNumResults() > 0) {
            String clinicalIds = result.getResults().stream().map(ClinicalAnalysis::getId).collect(Collectors.joining(", "));
            throw new CatalogException("Could not delete family '" + family.getId() + "'. Family is in use in Clinical Analyses: '"
                    + clinicalIds + "'");
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, query, options, ignoreException, token, StudyManager.INCLUDE_VARIABLE_SET,
                (organizationId, study, userId) -> {
                    Query finalQuery = query != null ? new Query(query) : new Query();
                    // Fix query if it contains any annotation
                    AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
                    fixQueryObject(organizationId, study, finalQuery, userId, token);
                    finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                    authorizationManager.buildAclCheckQuery(userId, FamilyPermissions.DELETE.name(), finalQuery);
                    return getFamilyDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_FAMILY_IDS, userId);
                }, (organizationId, study, userId, family) -> {
                    // Check family can be deleted
                    checkCanBeDeleted(organizationId, study, family);
                    // Delete the family
                    return getFamilyDBAdaptor(organizationId).delete(family);
                });
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        return groupBy(studyStr, query, fields, options, token, (organizationId, study, userId) -> {
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_FAMILIES);

            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();

            if (fields == null || fields.size() == 0) {
                throw new CatalogException("Empty fields parameter.");
            }

            fixQueryObject(organizationId, study, finalQuery, userId, token);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, userId, finalQuery, authorizationManager);
            AnnotationUtils.fixQueryOptionAnnotation(finalOptions);

            // Add study id to the query
            finalQuery.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getFamilyDBAdaptor(organizationId).groupBy(finalQuery, fields, finalOptions, userId);
        });
    }

    public OpenCGAResult<Family> updateAnnotationSet(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                     ParamUtils.BasicUpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        FamilyUpdateParams updateParams = new FamilyUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, familyStr, updateParams, options, token);
    }

    public OpenCGAResult<Family> addAnnotationSet(String studyStr, String familyStr, AnnotationSet annotationSet, QueryOptions options,
                                                  String token) throws CatalogException {
        return addAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Family> addAnnotationSets(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.BasicUpdateAction.ADD, options,
                token);
    }

    public OpenCGAResult<Family> setAnnotationSet(String studyStr, String familyStr, AnnotationSet annotationSet,
                                                  QueryOptions options, String token) throws CatalogException {
        return setAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Family> setAnnotationSets(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.BasicUpdateAction.SET, options,
                token);
    }

    public OpenCGAResult<Family> removeAnnotationSet(String studyStr, String familyStr, String annotationSetId,
                                                     QueryOptions options, String token) throws CatalogException {
        return removeAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSetId), options, token);
    }

    public OpenCGAResult<Family> removeAnnotationSets(String studyStr, String familyStr,
                                                      List<String> annotationSetIdList, QueryOptions options, String token)
            throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.BasicUpdateAction.REMOVE, options,
                token);
    }

    public OpenCGAResult<Family> updateAnnotations(String studyStr, String familyStr, String annotationSetId,
                                                   Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                   QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            throw new CatalogException("Missing array of annotations.");
        }
        FamilyUpdateParams updateParams = new FamilyUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, "", annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, familyStr, updateParams, options, token);
    }

    public OpenCGAResult<Family> removeAnnotations(String studyStr, String familyStr, String annotationSetId, List<String> annotations,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, familyStr, annotationSetId,
                new ObjectMap("remove", StringUtils.join(annotations, ",")), ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Family> resetAnnotations(String studyStr, String familyStr, String annotationSetId, List<String> annotations,
                                                  QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, familyStr, annotationSetId,
                new ObjectMap("reset", StringUtils.join(annotations, ",")), ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    public OpenCGAResult<Family> update(String studyStr, Query query, FamilyUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Family> update(String studyStr, Query query, FamilyUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        return updateMany(studyStr, query, updateParams, ignoreException, options, token, StudyManager.INCLUDE_VARIABLE_SET,
                (organizationId, study, userId) -> {
                    Query finalQuery = query != null ? new Query(query) : new Query();
                    fixQueryObject(organizationId, study, finalQuery, userId, token);
                    // Fix query if it contains any annotation
                    AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
                    finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                    return getFamilyDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, QueryOptions.empty(), userId);
                }, (organizationId, study, userId, family) -> update(organizationId, study, family, updateParams, options, userId),
                "Could not update family");
    }

    public OpenCGAResult<Family> update(String studyStr, String familyId, FamilyUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, familyId, updateParams, options, token, StudyManager.INCLUDE_VARIABLE_SET,
                (organizationId, study, userId, entryParam) -> {
                    entryParam.setId(familyId);
                    OpenCGAResult<Family> internalResult = internalGet(organizationId, study.getUid(), familyId, QueryOptions.empty(),
                            userId);
                    if (internalResult.getNumResults() == 0) {
                        throw new CatalogException("Family '" + familyId + "' not found");
                    }
                    Family family = internalResult.first();

                    // We set the proper values for entry param object
                    entryParam.setId(family.getId());
                    entryParam.setUuid(family.getUuid());

                    return update(organizationId, study, family, updateParams, options, userId);
                });
    }

    /**
     * Update families from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of
     *                    [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param familyIds    List of family ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Family> update(String studyStr, List<String> familyIds, FamilyUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, familyIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Family> update(String studyStr, List<String> familyIds, FamilyUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        return updateMany(studyStr, familyIds, updateParams, ignoreException, options, token, StudyManager.INCLUDE_STUDY_IDS,
                (organizationId, study, userId, entryParam) -> {
                    String familyId = entryParam.getId();
                    OpenCGAResult<Family> internalResult = internalGet(organizationId, study.getUid(), familyId, QueryOptions.empty(),
                            userId);
                    if (internalResult.getNumResults() == 0) {
                        throw new CatalogException("Family '" + familyId + "' not found");
                    }
                    Family family = internalResult.first();

                    // We set the proper values for entry param object
                    entryParam.setId(family.getId());
                    entryParam.setUuid(family.getUuid());

                    return update(organizationId, study, family, updateParams, options, userId);
                });
    }

    private OpenCGAResult<Family> update(String organizationId, Study study, Family family, FamilyUpdateParams updateParams,
                                         QueryOptions options, String userId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse FamilyUpdateParams object: " + e.getMessage(), e);
            }
        }

        if (StringUtils.isNotEmpty(parameters.getString(FamilyDBAdaptor.QueryParams.CREATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(FamilyDBAdaptor.QueryParams.CREATION_DATE.key()),
                    FamilyDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (StringUtils.isNotEmpty(parameters.getString(FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key()),
                    FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

        // If there is nothing to update, we fail
        if (parameters.isEmpty() && !options.getBoolean(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, false)
                && !options.getBoolean(ParamConstants.FAMILY_UPDATE_PEDIGREEE_GRAPH_PARAM, false)) {
            ParamUtils.checkUpdateParametersMap(parameters);
        }

        if (parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
            if (!actionMap.containsKey(AnnotationSetManager.ANNOTATION_SETS)
                    && !actionMap.containsKey(AnnotationSetManager.ANNOTATIONS)) {
                logger.warn("Assuming the user wants to add the list of annotation sets provided");
                actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.BasicUpdateAction.ADD);
                options.put(Constants.ACTIONS, actionMap);
            }
        }

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (updateParams != null && updateParams.getAnnotationSets() != null) {
            authorizationManager.checkFamilyPermission(organizationId, study.getUid(), family.getUid(), userId,
                    FamilyPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkFamilyPermission(organizationId, study.getUid(), family.getUid(), userId,
                    FamilyPermissions.WRITE);
        }

        if (updateParams != null && updateParams.getId() != null) {
            ParamUtils.checkIdentifier(updateParams.getId(), FamilyDBAdaptor.QueryParams.ID.key());
        }

        boolean updateRoles = options.getBoolean(ParamConstants.FAMILY_UPDATE_ROLES_PARAM);
        if (updateRoles || (updateParams != null && CollectionUtils.isNotEmpty(updateParams.getMembers()))) {
            Family tmpFamily = new Family();
            if (updateParams != null && CollectionUtils.isNotEmpty(updateParams.getMembers())) {
                // We obtain the members from catalog
                List<String> memberIds = new ArrayList<>(updateParams.getMembers().size());
                for (IndividualReferenceParam member : updateParams.getMembers()) {
                    if (StringUtils.isNotEmpty(member.getId())) {
                        memberIds.add(member.getId());
                    } else if (StringUtils.isNotEmpty(member.getUuid())) {
                        memberIds.add(member.getUuid());
                    } else {
                        throw new CatalogException("Found members without any id.");
                    }
                }
                List<Individual> updatedMembers = autoCompleteFamilyMembers(organizationId, study, tmpFamily, memberIds, userId);
                tmpFamily.setMembers(updatedMembers);
            } else {
                // We use the list of members from the stored family
                tmpFamily.setMembers(family.getMembers());
            }

            validateFamily(tmpFamily, null);
            validatePhenotypes(tmpFamily, null);
            validateDisorders(tmpFamily, null);

            ObjectMap tmpParams;
            try {
                ObjectMapper objectMapper = getDefaultObjectMapper();
                tmpParams = new ObjectMap(objectMapper.writeValueAsString(tmpFamily));
            } catch (JsonProcessingException e) {
                logger.error("{}", e.getMessage(), e);
                throw new CatalogException(e);
            }

            if (parameters.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
                parameters.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));
            }
            if (parameters.containsKey(FamilyDBAdaptor.QueryParams.PHENOTYPES.key())) {
                parameters.put(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(),
                        tmpParams.get(FamilyDBAdaptor.QueryParams.PHENOTYPES.key()));
            }
            if (parameters.containsKey(FamilyDBAdaptor.QueryParams.DISORDERS.key())) {
                parameters.put(FamilyDBAdaptor.QueryParams.DISORDERS.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.DISORDERS.key()));
            }
        }

        checkUpdateAnnotations(organizationId, study, family, parameters, options, VariableSet.AnnotableDataModels.FAMILY,
                getFamilyDBAdaptor(organizationId), userId);

        OpenCGAResult<Family> update = getFamilyDBAdaptor(organizationId).update(family.getUid(), parameters, study.getVariableSets(),
                options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated family
            OpenCGAResult<Family> result = getFamilyDBAdaptor(organizationId).get(study.getUid(),
                    new Query(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid()), options, userId);
            update.setResults(result.getResults());
        }
        return update;
    }

    public OpenCGAResult<Map<String, List<String>>> calculateFamilyGenotypes(String studyStr, String clinicalAnalysisId, String familyId,
                                                                             ClinicalProperty.ModeOfInheritance moi, String disorderId,
                                                                             Penetrance penetrance, String token) throws CatalogException {
        ObjectMap methodParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("familyId", familyId)
                .append("moi", moi)
                .append("disorderId", disorderId)
                .append("penetrance", penetrance)
                .append("token", token);

        return runForSingleEntry(methodParams, Enums.Action.CALCULATE_FAMILY_GENOTYPES, studyStr, token, StudyManager.INCLUDE_STUDY_IDS,
                (organizationId, study, userId, entryParam) -> {
                    entryParam.setId(familyId);

                    Pedigree pedigree;
                    Disorder disorder = null;

                    if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
                        OpenCGAResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager()
                                .get(studyStr, clinicalAnalysisId, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                        ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(),
                                        ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
                                        ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key())), token);
                        if (clinicalAnalysisDataResult.getNumResults() == 0) {
                            throw new CatalogException("Clinical analysis " + clinicalAnalysisId + " not found");
                        }

                        disorder = clinicalAnalysisDataResult.first().getDisorder();
                        pedigree = getPedigreeFromFamily(clinicalAnalysisDataResult.first().getFamily(),
                                clinicalAnalysisDataResult.first().getProband().getId());

                        entryParam.setId(clinicalAnalysisDataResult.first().getFamily().getId());
                        entryParam.setUuid(clinicalAnalysisDataResult.first().getFamily().getUuid());
                    } else if (StringUtils.isNotEmpty(familyId) && StringUtils.isNotEmpty(disorderId)) {
                        OpenCGAResult<Family> familyDataResult = get(studyStr, familyId, QueryOptions.empty(), token);

                        if (familyDataResult.getNumResults() == 0) {
                            throw new CatalogException("Family " + familyId + " not found");
                        }
                        entryParam.setId(familyDataResult.first().getId());
                        entryParam.setUuid(familyDataResult.first().getUuid());
                        for (Disorder tmpDisorder : familyDataResult.first().getDisorders()) {
                            if (tmpDisorder.getId().equals(disorderId)) {
                                disorder = tmpDisorder;
                                break;
                            }
                        }
                        if (disorder == null) {
                            throw new CatalogException("Disorder " + disorderId + " not found in any member of the family");
                        }

                        pedigree = getPedigreeFromFamily(familyDataResult.first(), null);
                    } else {
                        throw new CatalogException("Missing 'clinicalAnalysis' or ('family' and 'disorderId') parameters");
                    }

                    Map<String, List<String>> result;
                    switch (moi) {
                        case AUTOSOMAL_DOMINANT:
                            result = ModeOfInheritance.dominant(pedigree, disorder, penetrance);
                            break;
                        case AUTOSOMAL_RECESSIVE:
                            result = ModeOfInheritance.recessive(pedigree, disorder, penetrance);
                            break;
                        case X_LINKED_RECESSIVE:
                            result = ModeOfInheritance.xLinked(pedigree, disorder, false, penetrance);
                            break;
                        case X_LINKED_DOMINANT:
                            result = ModeOfInheritance.xLinked(pedigree, disorder, true, penetrance);
                            break;
                        case Y_LINKED:
                            result = ModeOfInheritance.yLinked(pedigree, disorder, penetrance);
                            break;
                        case MITOCHONDRIAL:
                            result = ModeOfInheritance.mitochondrial(pedigree, disorder, penetrance);
                            break;
                        case DE_NOVO:
                            result = ModeOfInheritance.deNovo(pedigree);
                            break;
                        case COMPOUND_HETEROZYGOUS:
                            result = ModeOfInheritance.compoundHeterozygous(pedigree);
                            break;
                        default:
                            throw new CatalogException("Unsupported or unknown mode of inheritance " + moi);
                    }

                    return new OpenCGAResult<>(0, Collections.singletonList(result));
                });
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<FamilyPermissions>> getAcls(String studyId, List<String> familyList, String member,
                                                                  boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, familyList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(),
                ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<FamilyPermissions>> getAcls(String studyId, List<String> familyList, List<String> members,
                                                                  boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, familyList, members, ignoreException, token, (organizationId, study, userId, entryParamList) -> {
            OpenCGAResult<AclEntryList<FamilyPermissions>> familyAcls;
            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();

            for (String familyId : familyList) {
                entryParamList.add(new EntryParam(familyId, null));
            }
            InternalGetDataResult<Family> queryResult = internalGet(organizationId, study.getUid(), familyList, INCLUDE_FAMILY_IDS, userId,
                    ignoreException);
            entryParamList.clear();
            for (Family family : queryResult.getResults()) {
                entryParamList.add(new EntryParam(family.getId(), family.getUuid()));
            }
            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> familyUids = queryResult.getResults().stream().map(Family::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                familyAcls = authorizationManager.getAcl(organizationId, study.getUid(), familyUids, members, Enums.Resource.FAMILY,
                        FamilyPermissions.class, userId);
            } else {
                familyAcls = authorizationManager.getAcl(organizationId, study.getUid(), familyUids, Enums.Resource.FAMILY,
                        FamilyPermissions.class, userId);
            }

            // Include non-existing samples to the result list
            List<AclEntryList<FamilyPermissions>> resultList = new ArrayList<>(familyList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String familyId : familyList) {
                if (!missingMap.containsKey(familyId)) {
                    resultList.add(familyAcls.getResults().get(counter));
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, familyId, missingMap.get(familyId).getErrorMsg()));
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                familyAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            familyAcls.setResults(resultList);
            familyAcls.setEvents(eventList);

            return familyAcls;
        });
    }

    public OpenCGAResult<AclEntryList<FamilyPermissions>> updateAcl(String studyId, FamilyAclParams aclParams, String memberList,
                                                                    ParamUtils.AclAction action, String token) throws CatalogException {
        return updateAcls(studyId, null, memberList, aclParams, action, token, (organizationId, study, userId, entryParamList) -> {
            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

            int count = 0;
            count += aclParams.getFamily() != null && !aclParams.getFamily().isEmpty() ? 1 : 0;
            count += aclParams.getIndividual() != null && !aclParams.getIndividual().isEmpty() ? 1 : 0;
            count += aclParams.getSample() != null && !aclParams.getSample().isEmpty() ? 1 : 0;

            if (count > 1) {
                throw new CatalogException("Update ACL: Only one of these parameters are allowed: family, individual or sample per query.");
            } else if (count == 0) {
                throw new CatalogException("Update ACL: At least one of these parameters should be provided: family, individual or sample");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, FamilyPermissions::valueOf);
            }

            String individualStr = aclParams.getIndividual();
            if (StringUtils.isNotEmpty(aclParams.getSample())) {
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyId, aclParams.getSample(),
                        new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()), token);
                individualStr = sampleResult.getResults().stream().map(Sample::getIndividualId).collect(Collectors.joining(","));
            }

            List<Family> familyList = null;
            if (StringUtils.isNotEmpty(individualStr)) {
                familyList = catalogManager.getFamilyManager().search(studyId,
                                new Query(FamilyDBAdaptor.QueryParams.MEMBERS.key(), individualStr), FamilyManager.INCLUDE_FAMILY_MEMBERS,
                                token)
                        .getResults();
            } else if (StringUtils.isNotEmpty(aclParams.getFamily())) {
                familyList = catalogManager.getFamilyManager().get(studyId, Arrays.asList(aclParams.getFamily().split(",")),
                        FamilyManager.INCLUDE_FAMILY_MEMBERS, token).getResults();
            }
            if (familyList == null || familyList.isEmpty()) {
                throw new CatalogException("No families found to set permissions");
            }
            for (Family family : familyList) {
                entryParamList.add(new EntryParam(family.getId(), family.getUuid()));
            }

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(organizationId, study.getUid(), members);
            if (study.getInternal().isFederated()) {
                try {
                    checkIsNotAFederatedUser(organizationId, members);
                } catch (CatalogException e) {
                    throw new CatalogException("Cannot provide access to federated users to a federated study.", e);
                }
            }

            List<AuthorizationManager.CatalogAclParams> aclParamsList = new LinkedList<>();
            List<Long> familyUids = familyList.stream().map(Family::getUid).collect(Collectors.toList());
            aclParamsList.add(new AuthorizationManager.CatalogAclParams(familyUids, permissions, Enums.Resource.FAMILY));

            if (aclParams.getPropagate() == FamilyAclParams.Propagate.YES
                    || aclParams.getPropagate() == FamilyAclParams.Propagate.YES_AND_VARIANT_VIEW) {
                // Obtain the list of members and samples
                Set<Long> memberUids = new HashSet<>();
                Set<Long> sampleUids = new HashSet<>();
                for (Family family : familyList) {
                    if (family.getMembers() != null) {
                        for (Individual member : family.getMembers()) {
                            memberUids.add(member.getUid());
                            if (member.getSamples() != null) {
                                for (Sample sample : member.getSamples()) {
                                    sampleUids.add(sample.getUid());
                                }
                            }
                        }
                    }
                }
                if (!memberUids.isEmpty()) {
                    // Add permissions to those individuals
                    aclParamsList.add(new AuthorizationManager.CatalogAclParams(new ArrayList<>(memberUids), permissions,
                            Enums.Resource.INDIVIDUAL));
                }

                if (!sampleUids.isEmpty()) {
                    // Add permissions to those samples
                    List<String> samplePermissions = new ArrayList<>(permissions);
                    if (aclParams.getPropagate() == FamilyAclParams.Propagate.YES_AND_VARIANT_VIEW) {
                        samplePermissions.add(SamplePermissions.VIEW_VARIANTS.name());
                    }
                    aclParamsList.add(new AuthorizationManager.CatalogAclParams(new ArrayList<>(sampleUids), samplePermissions,
                            Enums.Resource.SAMPLE));
                }
            }

            switch (action) {
                case SET:
                    authorizationManager.setAcls(organizationId, study.getUid(), members, aclParamsList);
                    break;
                case ADD:
                    authorizationManager.addAcls(organizationId, study.getUid(), members, aclParamsList);
                    break;
                case REMOVE:
                    authorizationManager.removeAcls(organizationId, members, aclParamsList);
                    break;
                case RESET:
                    for (AuthorizationManager.CatalogAclParams auxAclParams : aclParamsList) {
                        auxAclParams.setPermissions(null);
                    }
                    authorizationManager.removeAcls(organizationId, members, aclParamsList);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            OpenCGAResult<AclEntryList<FamilyPermissions>> remainingAcls = authorizationManager.getAcls(organizationId, study.getUid(),
                    familyUids, members, Enums.Resource.FAMILY, FamilyPermissions.class);
            for (int i = 0; i < remainingAcls.getResults().size(); i++) {
                remainingAcls.getResults().get(i).setId(familyList.get(i).getId());
            }
            return remainingAcls;
        });
    }

    /**
     * Validate the list of members provided in the members list already exists (and retrieves their information).
     * It also makes sure the members provided inside the family object are valid and can be successfully created.
     * Returns the list of already existing members that will be associated to the family.
     *
     * @param organizationId Organization id.
     * @param study          study.
     * @param family         family object.
     * @param members        Already existing members.
     * @param userId         user id.
     * @return list of already existing members that will be associated to the family.
     * @throws CatalogException if there is any kind of error.
     */
    private List<Individual> autoCompleteFamilyMembers(String organizationId, Study study, Family family, List<String> members,
                                                       String userId) throws CatalogException {
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            List<Individual> memberList = new ArrayList<>();
            // Check the user can create new individuals
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId,
                    StudyPermissions.Permissions.WRITE_INDIVIDUALS);

            // Validate the individuals can be created and are valid
            for (Individual individual : family.getMembers()) {
                catalogManager.getIndividualManager().validateNewIndividual(organizationId, study, individual, null, userId, false);
                memberList.add(individual);
            }

            family.setMembers(memberList);
        }

        if (CollectionUtils.isNotEmpty(members)) {
            // We check for possible duplicates
            Set<String> memberSet = new HashSet<>();
            for (String member : members) {
                boolean unique = memberSet.add(member);
                if (!unique) {
                    throw new CatalogException("Duplicated member '" + member + "' passed.");
                }
            }

            InternalGetDataResult<Individual> individualDataResult = catalogManager.getIndividualManager().internalGet(organizationId,
                    study.getUid(), members, IndividualManager.INCLUDE_INDIVIDUAL_DISORDERS_PHENOTYPES, userId, false);

            return individualDataResult.getResults();
        }

        return Collections.emptyList();
    }

    private void validateFamily(Family family, List<Individual> existingMembers) throws CatalogException {
        List<Individual> members = new LinkedList<>();
        if (family.getMembers() != null) {
            members.addAll(family.getMembers());
        }
        if (existingMembers != null) {
            members.addAll(existingMembers);
        }
        if (members.isEmpty()) {
            return;
        }

        Map<String, Individual> membersMap = new HashMap<>();       // individualName|individualId: Individual
        Map<String, List<Individual>> parentsMap = new HashMap<>(); // motherName||F---fatherName||M: List<children>
        Set<String> noParentsSet = new HashSet<>();                 // Set with individuals without parents

        // 1. Fill in the objects initialised above
        for (Individual individual : members) {
            membersMap.put(individual.getId(), individual);
            if (individual.getUid() > 0) {
                membersMap.put(String.valueOf(individual.getUid()), individual);
            }

            String parentsKey = "";
            if (individual.getMother() != null) {
                if (!StringUtils.isEmpty(individual.getMother().getId())) {
                    parentsKey += individual.getMother().getId() + "__" + individual.getMother().getUid() + "||F";
                }
            }
            if (individual.getFather() != null) {
                if (StringUtils.isNotEmpty(parentsKey)) {
                    parentsKey += "---";
                }
                if (!StringUtils.isEmpty(individual.getFather().getId())) {
                    parentsKey += individual.getFather().getId() + "__" + individual.getFather().getUid() + "||M";
                }
            }
            if (StringUtils.isEmpty(parentsKey)) {
                noParentsSet.add(individual.getId());
            } else {
                if (!parentsMap.containsKey(parentsKey)) {
                    parentsMap.put(parentsKey, new ArrayList<>());
                }
                parentsMap.get(parentsKey).add(individual);
            }
        }

        // 2. Loop over the parentsMap object. We will be emptying the noParentsSet as soon as we find a parent in the set. Once,
        // everything finishes, that set should be empty. Otherwise, it will mean that parent is not in use
        for (Map.Entry<String, List<Individual>> parentListEntry : parentsMap.entrySet()) {
            String[] split = parentListEntry.getKey().split("---");
            for (String parentName : split) {
                String[] splitNameSex = parentName.split("\\|\\|");
                String[] splitIdUid = splitNameSex[0].split("__");
                String parentId = splitIdUid[0];
                String parentUid = splitIdUid[1];
                SexOntologyTermAnnotation sexTerm = splitNameSex[1].equals("F")
                        ? SexOntologyTermAnnotation.initFemale()
                        : SexOntologyTermAnnotation.initMale();
                IndividualProperty.Sex sex = sexTerm.getSex();

                if (membersMap.containsKey(parentUid)) {
                    // Check if the sex is correct
                    IndividualProperty.Sex sex1 = membersMap.get(parentUid).getSex() != null
                            ? membersMap.get(parentUid).getSex().getSex()
                            : IndividualProperty.Sex.UNKNOWN;
                    if (sex1 != null && sex1 != sex && sex1 != IndividualProperty.Sex.UNKNOWN) {
                        throw new CatalogException("Sex of parent '" + parentId + "' is incorrect or the relationship is incorrect. In "
                                + "principle, it should be " + sexTerm);
                    }
                    membersMap.get(parentUid).setSex(sexTerm);

                    // We attempt to remove the individual from the noParentsSet
                    noParentsSet.remove(membersMap.get(parentUid).getId());
                }
            }
        }

        // FIXME Pedro: this is a quick fix to allow create families without the parents, this needs to be reviewed.
        if (noParentsSet.size() > 0) {
//            throw new CatalogException("Some members that are not related to any other have been found: "
//                    + noParentsSet.stream().map(Individual::getName).collect(Collectors.joining(", ")));
            logger.warn("Some members that are not related to any other have been found: {}", StringUtils.join(noParentsSet, ", "));
        }
    }

    private void validatePhenotypes(Family family, List<Individual> existingMembers) throws CatalogException {
        List<Individual> members = new LinkedList<>();
        if (family.getMembers() != null) {
            members.addAll(family.getMembers());
        }
        if (existingMembers != null) {
            members.addAll(existingMembers);
        }
        if (family.getPhenotypes() == null || family.getPhenotypes().isEmpty()) {
            if (CollectionUtils.isNotEmpty(members)) {
                Map<String, Phenotype> phenotypeMap = new HashMap<>();

                for (Individual member : members) {
                    if (CollectionUtils.isNotEmpty(member.getPhenotypes())) {
                        for (Phenotype phenotype : member.getPhenotypes()) {
                            phenotypeMap.put(phenotype.getId(), phenotype);
                        }
                    }
                }

                // Set the new phenotype list
                List<Phenotype> phenotypeList = new ArrayList<>(phenotypeMap.values());
                family.setPhenotypes(phenotypeList);
            }
        } else {
            // We need to validate the phenotypes are actually correct
            if (members.isEmpty()) {
                throw new CatalogException("Missing family members");
            }

            // Validate all the phenotypes are contained in at least one individual
            Set<String> memberPhenotypes = new HashSet<>();
            for (Individual individual : members) {
                if (individual.getPhenotypes() != null && !individual.getPhenotypes().isEmpty()) {
                    memberPhenotypes.addAll(individual.getPhenotypes().stream().map(Phenotype::getId).collect(Collectors.toSet()));
                }
            }
            Set<String> familyPhenotypes = family.getPhenotypes().stream().map(Phenotype::getId).collect(Collectors.toSet());
            if (!familyPhenotypes.containsAll(memberPhenotypes)) {
                throw new CatalogException("Some of the phenotypes are not present in any member of the family");
            }
        }
    }

    private void validateDisorders(Family family, List<Individual> existingMembers) throws CatalogException {
        List<Individual> members = new LinkedList<>();
        if (family.getMembers() != null) {
            members.addAll(family.getMembers());
        }
        if (existingMembers != null) {
            members.addAll(existingMembers);
        }
        if (CollectionUtils.isEmpty(family.getDisorders())) {
            if (CollectionUtils.isNotEmpty(members)) {
                // Obtain the union of all disorders
                Map<String, Disorder> disorderMap = new HashMap<>();
                Map<String, Map<String, Phenotype>> disorderPhenotypeMap = new HashMap<>();

                for (Individual member : members) {
                    if (CollectionUtils.isNotEmpty(member.getDisorders())) {
                        for (Disorder disorder : member.getDisorders()) {
                            disorderMap.put(disorder.getId(), disorder);

                            if (CollectionUtils.isNotEmpty(disorder.getEvidences())) {
                                if (!disorderPhenotypeMap.containsKey(disorder.getId())) {
                                    disorderPhenotypeMap.put(disorder.getId(), new HashMap<>());
                                }

                                for (Phenotype evidence : disorder.getEvidences()) {
                                    disorderPhenotypeMap.get(disorder.getId()).put(evidence.getId(), evidence);
                                }
                            }
                        }
                    }
                }

                // Set the new disorder list
                List<Disorder> disorderList = new ArrayList<>(disorderMap.size());
                for (Disorder disorder : disorderMap.values()) {
                    List<Phenotype> phenotypeList = null;
                    if (disorderPhenotypeMap.get(disorder.getId()) != null) {
                        phenotypeList = new ArrayList<>(disorderPhenotypeMap.get(disorder.getId()).values());
                    }
                    disorder.setEvidences(phenotypeList);
                    disorderList.add(disorder);
                }

                family.setDisorders(disorderList);
            }
        } else {
            // We need to validate the disorders are actually correct
            if (members.isEmpty()) {
                throw new CatalogException("Missing family members");
            }

            // Validate all the disorders are contained in at least one individual
            Set<String> memberDisorders = new HashSet<>();
            for (Individual individual : members) {
                if (CollectionUtils.isNotEmpty(individual.getDisorders())) {
                    memberDisorders.addAll(individual.getDisorders().stream().map(Disorder::getId).collect(Collectors.toSet()));
                }
            }
            Set<String> familyDisorders = family.getDisorders().stream().map(Disorder::getId).collect(Collectors.toSet());
            if (!familyDisorders.containsAll(memberDisorders)) {
                throw new CatalogException("Some of the disorders are not present in any member of the family");
            }
        }
    }

}
