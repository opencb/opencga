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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.cellbase.CellBaseValidator;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.cohort.CohortPermissions;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.FamilyPermissions;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FilePermissions;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.job.JobPermissions;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.summaries.StudySummary;
import org.opencb.opencga.core.models.summaries.VariableSetSummary;
import org.opencb.opencga.core.models.summaries.VariableSummary;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyManager extends AbstractManager {

    private final CatalogIOManager catalogIOManager;
    private final IOManagerFactory ioManagerFactory;

    public static final String MEMBERS = ParamConstants.MEMBERS_GROUP;
    public static final String ADMINS = ParamConstants.ADMINS_GROUP;
    //[A-Za-z]([-_.]?[A-Za-z0-9]
    private static final String USER_PATTERN = "[A-Za-z][[-_.]?[A-Za-z0-9]?]*";
    private static final String PROJECT_PATTERN = "[A-Za-z0-9][[-_.]?[A-Za-z0-9]?]*";
    private static final String STUDY_PATTERN = "[A-Za-z0-9\\-_.]+|\\*";

    private static final Pattern ORGANIZATION_PROJECT_STUDY_PATTERN = CatalogFqn.ORGANIZATION_PROJECT_STUDY_PATTERN;
    private static final Pattern PROJECT_STUDY_PATTERN = CatalogFqn.PROJECT_STUDY_PATTERN;

    static final QueryOptions INCLUDE_STUDY_UID = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UID.key());
    public static final QueryOptions INCLUDE_STUDY_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            StudyDBAdaptor.QueryParams.UID.key(), StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.UUID.key(),
            StudyDBAdaptor.QueryParams.FQN.key()));
    static final QueryOptions INCLUDE_VARIABLE_SET = keepFieldInQueryOptions(INCLUDE_STUDY_IDS,
            StudyDBAdaptor.QueryParams.VARIABLE_SET.key());
    static final QueryOptions INCLUDE_CONFIGURATION = keepFieldInQueryOptions(INCLUDE_STUDY_IDS,
            StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION.key());
    static final QueryOptions INCLUDE_VARIABLE_SET_AND_CONFIGURATION = keepFieldsInQueryOptions(INCLUDE_STUDY_IDS,
            Arrays.asList(StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION.key(), StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

    protected Logger logger;

    StudyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                 DBAdaptorFactory catalogDBAdaptorFactory, IOManagerFactory ioManagerFactory, CatalogIOManager catalogIOManager,
                 Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.catalogIOManager = catalogIOManager;
        this.ioManagerFactory = ioManagerFactory;
        logger = LoggerFactory.getLogger(StudyManager.class);
    }

//    public String getProjectId(long studyId) throws CatalogException {
//        return studyDBAdaptor.getProjectIdByStudyUid(studyId);
//    }


    List<Study> resolveIds(List<String> studyList, String userId, String organizationId) throws CatalogException {
        if (studyList == null || studyList.isEmpty() || (studyList.size() == 1 && studyList.get(0).endsWith("*"))) {
            String studyStr = "*";
            if (studyList != null && !studyList.isEmpty()) {
                studyStr = studyList.get(0);
            }

            return smartResolutor(studyStr, INCLUDE_STUDY_IDS, userId, organizationId).getResults();
        }

        List<Study> returnList = new ArrayList<>(studyList.size());
        for (String study : studyList) {
            returnList.add(resolveId(study, INCLUDE_STUDY_IDS, userId, organizationId));
        }
        return returnList;
    }

    Study resolveId(String studyStr, String userId, String organizationId) throws CatalogException {
        return resolveId(studyStr, INCLUDE_STUDY_IDS, userId, organizationId);
    }

    Study resolveId(String studyStr, QueryOptions options, String userId, String organizationId) throws CatalogException {
        OpenCGAResult<Study> studyDataResult = smartResolutor(studyStr, options, userId, organizationId);

        if (studyDataResult.getNumResults() > 1) {
            String studyMessage = "";
            if (StringUtils.isNotEmpty(studyStr)) {
                studyMessage = " given '" + studyStr + "'";
            }
            throw new CatalogException("More than one study found" + studyMessage + ". Please, be more specific."
                    + " The accepted pattern is [" + CatalogFqn.STUDY_FQN_FORMAT + "]");
        }

        return studyDataResult.first();
    }

    Study resolveId(CatalogFqn catalogFqn, JwtPayload payload) throws CatalogException {
        return resolveId(catalogFqn, QueryOptions.empty(), payload);
    }

    Study resolveId(CatalogFqn catalogFqn, QueryOptions options, JwtPayload payload) throws CatalogException {
        OpenCGAResult<Study> studyDataResult = smartResolutor(catalogFqn, options, payload);

        if (studyDataResult.getNumResults() > 1) {
            String studyMessage = "";
            if (StringUtils.isNotEmpty(catalogFqn.getProvidedId())) {
                studyMessage = " given '" + catalogFqn.getProvidedId() + "'";
            }
            throw new CatalogException("More than one study found" + studyMessage + ". Please, be more specific."
                    + " The accepted pattern is [" + CatalogFqn.STUDY_FQN_FORMAT + "]");
        }

        return studyDataResult.first();
    }

    /**
     * Resolve the study the user wants to access.
     *
     * @param catalogFqn     CatalogFqn object.
     * @param options        Options to include/exclude Study fields.
     * @param payload        Token payload containing the user requesting the operation.
     * @return An OpenCGAResult with the appropriate study.
     * @throws CatalogException if the user cannot access the study, or it is unclear which study they want to access.
     */
    private OpenCGAResult<Study> smartResolutor(CatalogFqn catalogFqn, QueryOptions options, JwtPayload payload) throws CatalogException {
        String userId = payload.getUserId(catalogFqn.getOrganizationId());

        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(catalogFqn.getOrganizationId(), "organizationId");

        Query query = new Query();
        QueryOptions queryOptions;
        if (options == null) {
            queryOptions = new QueryOptions();
        } else {
            queryOptions = new QueryOptions(options);
        }

        String studyStr;
        if (StringUtils.isNotEmpty(catalogFqn.getStudyUuid())) {
            query.putIfNotEmpty(StudyDBAdaptor.QueryParams.UUID.key(), catalogFqn.getStudyUuid());
            studyStr = catalogFqn.getStudyUuid();
        } else {
            studyStr = catalogFqn.getStudyId();
            if (!"*".equals(catalogFqn.getStudyId())) {
                query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ID.key(), catalogFqn.getStudyId());
            }
        }
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), catalogFqn.getProjectId());

        if (!queryOptions.isEmpty()) {
            // Ensure at least the ids are included
            fixQueryOptions(queryOptions, INCLUDE_STUDY_IDS.getAsStringList(QueryOptions.INCLUDE));
        }

        OpenCGAResult<Study> studyDataResult;
        if (!payload.getOrganization().equals(catalogFqn.getOrganizationId())) {
            // If it is the administrator, we allow it without checking the user anymore
            authorizationManager.checkIsOpencgaAdministrator(payload);
            studyDataResult = getStudyDBAdaptor(catalogFqn.getOrganizationId()).get(query, queryOptions);
        } else {
            studyDataResult = getStudyDBAdaptor(catalogFqn.getOrganizationId()).get(query, queryOptions, userId);
            if (studyDataResult.getNumResults() == 0) {
                studyDataResult = getStudyDBAdaptor(catalogFqn.getOrganizationId()).get(query, queryOptions);
                if (studyDataResult.getNumResults() != 0) {
                    throw CatalogAuthorizationException.denyAny(userId, "view", "study");
                }
            }
        }

        if (studyDataResult.getNumResults() == 0) {
            String studyMessage = "";
            if (StringUtils.isNotEmpty(studyStr)) {
                studyMessage = " given '" + studyStr + "'";
            }
            throw new CatalogException("No study found" + studyMessage + ".");
        } else {
            return studyDataResult;
        }
    }

    /**
     * Resolve the study the user wants to access.
     *
     * @param studyStr       Study id or fqn.
     * @param options        Options to include/exclude Study fields.
     * @param userId         User requesting the operation.
     * @param organizationId Organization to which the user belongs.
     * @return An OpenCGAResult with the appropriate study.
     * @throws CatalogException if the user cannot access the study, or it is unclear which study they want to access.
     */
    private OpenCGAResult<Study> smartResolutor(String studyStr, QueryOptions options, String userId, String organizationId)
            throws CatalogException {
        if (StringUtils.isEmpty(userId)) {
            throw new CatalogException("Missing internal mandatory field 'userId'");
        }
        if (StringUtils.isEmpty(organizationId)) {
            throw new CatalogException("Missing internal mandatory field 'organization'");
        }

        String organizationFqn = null;
        String project = null;

        Query query = new Query();
        QueryOptions queryOptions;
        if (options == null) {
            queryOptions = new QueryOptions();
        } else {
            queryOptions = new QueryOptions(options);
        }

        if (StringUtils.isNotEmpty(studyStr)) {
            if (UuidUtils.isOpenCgaUuid(studyStr)) {
                query.putIfNotEmpty(StudyDBAdaptor.QueryParams.UUID.key(), studyStr);
            } else {
                String study;

                Matcher matcher = ORGANIZATION_PROJECT_STUDY_PATTERN.matcher(studyStr);
                if (matcher.find()) {
                    // studyStr contains the full path (org@project:study)
                    organizationFqn = matcher.group(1);
                    project = matcher.group(2);
                    study = matcher.group(3);
                } else {
                    matcher = PROJECT_STUDY_PATTERN.matcher(studyStr);
                    if (matcher.find()) {
                        // studyStr contains the path (project:study)
                        project = matcher.group(1);
                        study = matcher.group(2);
                    } else {
                        // studyStr only contains the study information
                        study = studyStr;
                    }
                }

                if (study.equals("*")) {
                    // If the user is asking for all the studies...
                    study = null;
                }

                query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ID.key(), study);
            }
        }

        if (organizationFqn != null && !organizationId.equals(organizationFqn)
                && !ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
            logger.error("User '{}' belonging to organization '{}' requested access to organization '{}'", userId, organizationId,
                    organizationFqn);
            throw new CatalogAuthorizationException("Cannot access data from a different organization.");
        } else {
            // If organization is not part of the FQN, assign it with the organization the user belongs to.
            organizationFqn = organizationId;
        }

        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), project);

        if (!queryOptions.isEmpty()) {
            // Ensure at least the ids are included
            fixQueryOptions(queryOptions, INCLUDE_STUDY_IDS.getAsStringList(QueryOptions.INCLUDE));
        }

        OpenCGAResult<Study> studyDataResult = getStudyDBAdaptor(organizationFqn).get(query, queryOptions, userId);

        if (studyDataResult.getNumResults() == 0) {
            studyDataResult = getStudyDBAdaptor(organizationFqn).get(query, queryOptions);
            if (studyDataResult.getNumResults() == 0) {
                String studyMessage = "";
                if (StringUtils.isNotEmpty(studyStr)) {
                    studyMessage = " given '" + studyStr + "'";
                }
                throw new CatalogException("No study found" + studyMessage + ".");
            } else {
                throw CatalogAuthorizationException.denyAny(userId, "view", "study");
            }
        }

        return studyDataResult;
    }

    private void fixQueryOptions(QueryOptions queryOptions, List<String> includeFieldsList) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            Set<String> includeList = new HashSet<>(queryOptions.getAsStringList(QueryOptions.INCLUDE));
            includeList.addAll(Arrays.asList(
                    StudyDBAdaptor.QueryParams.UUID.key(), StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.UID.key(),
                    StudyDBAdaptor.QueryParams.ALIAS.key(), StudyDBAdaptor.QueryParams.CREATION_DATE.key(),
                    StudyDBAdaptor.QueryParams.NOTIFICATION.key(), StudyDBAdaptor.QueryParams.FQN.key(),
                    StudyDBAdaptor.QueryParams.URI.key()));
            // We create a new object in case there was an exclude or any other field. We only want to include fields in this case
            queryOptions.put(QueryOptions.INCLUDE, new ArrayList<>(includeList));
        } else if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            // We will make sure that the user does not exclude the minimum required fields
            Set<String> excludeList = new HashSet<>(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
            for (String field : includeFieldsList) {
                excludeList.remove(field);
            }
            if (!excludeList.isEmpty()) {
                queryOptions.put(QueryOptions.EXCLUDE, new ArrayList<>(excludeList));
            } else {
                queryOptions.remove(QueryOptions.EXCLUDE);
            }
        }
    }

    private OpenCGAResult<Study> getStudy(String organizationId, long projectUid, String studyUuid, QueryOptions options)
            throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), projectUid)
                .append(StudyDBAdaptor.QueryParams.UUID.key(), studyUuid);
        return getStudyDBAdaptor(organizationId).get(query, options);
    }

    @Deprecated
    public OpenCGAResult<Study> create(String projectStr, String id, String alias, String name, String description,
                                       StudyNotification notification, StudyInternal internal, Status status,
                                       Map<String, Object> attributes, QueryOptions options, String token) throws CatalogException {
        Study study = new Study()
                .setId(id)
                .setAlias(alias)
                .setName(name)
                .setDescription(description)
                .setNotification(notification)
                .setInternal(internal)
                .setStatus(status)
                .setAttributes(attributes);
        return create(projectStr, study, options, token);
    }

    public OpenCGAResult<Study> create(String projectStr, Study study, QueryOptions options, String token) throws CatalogException {
        ParamUtils.checkObj(study, "study");
        ParamUtils.checkIdentifier(study.getId(), "id");

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromProject(projectStr, tokenPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Project project = catalogManager.getProjectManager().resolveId(catalogFqn, null, tokenPayload).first();
        ObjectMap auditParams = new ObjectMap()
                .append("projectId", projectStr)
                .append("study", study)
                .append("options", options)
                .append("token", token);

        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);

            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            String cellbaseVersion;
            if (project.getCellbase() == null || StringUtils.isEmpty(project.getCellbase().getUrl())) {
                CellBaseValidator cellBaseValidator = new CellBaseValidator(
                        project.getCellbase(),
                        project.getOrganism().getScientificName(),
                        project.getOrganism().getAssembly());
                cellbaseVersion = cellBaseValidator.getVersionFromServer();
            } else {
                cellbaseVersion = null;
            }
            long projectUid = project.getUid();

            // Initialise fields
            study.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.STUDY));
            study.setName(ParamUtils.defaultString(study.getName(), study.getId()));
            study.setAlias(ParamUtils.defaultString(study.getAlias(), study.getId()));
            study.setType(ParamUtils.defaultObject(study.getType(), StudyType::init));
            study.setSources(ParamUtils.defaultObject(study.getSources(), Collections::emptyList));
            study.setDescription(ParamUtils.defaultString(study.getDescription(), ""));
            study.setInternal(StudyInternal.init(cellbaseVersion));
            study.setStatus(ParamUtils.defaultObject(study.getStatus(), Status::new));
            study.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(study.getCreationDate(),
                    StudyDBAdaptor.QueryParams.CREATION_DATE.key()));
            study.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(study.getModificationDate(),
                    StudyDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
            study.setRelease(project.getCurrentRelease());
            study.setNotification(ParamUtils.defaultObject(study.getNotification(), new StudyNotification()));
            study.setPermissionRules(ParamUtils.defaultObject(study.getPermissionRules(), HashMap::new));
            study.setAdditionalInfo(ParamUtils.defaultObject(study.getAdditionalInfo(), Collections::emptyList));
            study.setAttributes(ParamUtils.defaultObject(study.getAttributes(), HashMap::new));

            // Scan and add default VariableSets
            List<VariableSet> variableSetList = scanDefaultVariableSets(study.getRelease());
            study.setVariableSets(variableSetList);

            // Get organization owner and admins
            Organization organization = catalogManager.getOrganizationManager().get(organizationId,
                    OrganizationManager.INCLUDE_ORGANIZATION_ADMINS, token).first();
            Set<String> users = new HashSet<>();
            users.add(organization.getOwner());
            users.addAll(organization.getAdmins());

            List<Group> groups = Arrays.asList(
                    new Group(MEMBERS, new ArrayList<>(users)),
                    new Group(ADMINS, new ArrayList<>(users))
            );
            study.setGroups(groups);

            /* CreateStudy */
            OpenCGAResult<Study> result = getStudyDBAdaptor(organizationId).insert(project, study, options);

            auditManager.auditCreate(organizationId, userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                OpenCGAResult<Study> tmpResult = getStudy(organizationId, projectUid, study.getUuid(), options);
                result.setResults(tmpResult.getResults());
            }
            return result;
        } catch (Exception e) {
            auditManager.auditError(organizationId, userId, Enums.Action.CREATE, Enums.Resource.STUDY, study.getId(),
                    "", study.getId(), "", auditParams, e);
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException("Error creating study '" + study.getId() + "'", e);
            }
        }
    }

    public List<VariableSet> scanDefaultVariableSets(int release) throws CatalogException {
        Set<String> variablesets = new Reflections(new ResourcesScanner(), "variablesets/").getResources(Pattern.compile(".*\\.json"));
        List<VariableSet> variableSetList = new ArrayList<>(variablesets.size());
        for (String variableSetFile : variablesets) {
            VariableSet vs;
            try {
                vs = JacksonUtils.getDefaultNonNullObjectMapper().readValue(
                        getClass().getClassLoader().getResourceAsStream(variableSetFile), VariableSet.class);
            } catch (IOException e) {
                throw new CatalogException("Could not parse default variable set '" + variableSetFile + "'.", e);
            }
            if (vs != null) {
                if (vs.getAttributes() == null) {
                    vs.setAttributes(new HashMap<>());
                }
                vs.getAttributes().put("resource", variableSetFile);
                validateNewVariableSet(vs, release);
                variableSetList.add(vs);
            } else {
                throw new CatalogException("Default VariableSet object is null after parsing file '" + variableSetFile + "'");
            }
        }
        return variableSetList;
    }

    public void createDefaultVariableSets(String studyStr, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = catalogFqn.getOrganizationId();
        Study study = resolveId(catalogFqn, null, tokenPayload);
        createDefaultVariableSets(organizationId, study, token);
    }

    private void createDefaultVariableSets(String organizationId, Study study, String token) throws CatalogException {
        List<VariableSet> variableSetList = scanDefaultVariableSets(study.getRelease());

        for (VariableSet vs : variableSetList) {
            if (study.getVariableSets().stream().anyMatch(tvs -> tvs.getId().equals(vs.getId()))) {
                logger.debug("Skip already existing variable set " + vs.getId());
            } else {
                createVariableSet(organizationId, study, vs, token);
            }
        }
    }

    int getCurrentRelease(Study study) throws CatalogException {
        String[] split = StringUtils.split(study.getFqn(), ":");
        String[] split2 = StringUtils.split(split[0], "@");
        String organization = split2[0];
        String projectId = split2[1];

        Query query = new Query(ProjectDBAdaptor.QueryParams.ID.key(), projectId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key());
        return getProjectDBAdaptor(organization).get(query, options).first().getCurrentRelease();
    }

    /**
     * Fetch a study from Catalog given a study id or alias.
     *
     * @param studyStr Study id or alias.
     * @param options  Read options
     * @param token    sessionId
     * @return The specified object
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Study> get(String studyStr, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("options", options)
                .append("token", token);
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            Study study = catalogManager.getStudyManager().resolveId(studyStr, options, userId, organizationId);
            auditManager.auditInfo(organizationId, userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            OpenCGAResult<Study> studyDataResult = new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), null, 1,
                    Collections.singletonList(study), 1);
            return filterResults(studyDataResult);
        } catch (CatalogException e) {
            auditManager.auditInfo(organizationId, userId, Enums.Resource.STUDY, studyStr, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Study> get(List<String> studyList, QueryOptions queryOptions, boolean ignoreException, String token)
            throws CatalogException {
        OpenCGAResult<Study> result = OpenCGAResult.empty(Study.class);
        for (String study : studyList) {
            try {
                OpenCGAResult<Study> studyObj = get(study, queryOptions, token);
                result.append(studyObj);
            } catch (CatalogException e) {
                String warning = "Missing " + study + ": " + e.getMessage();
                Event event = new Event(Event.Type.ERROR, study, e.getMessage());
                if (ignoreException) {
                    logger.error(warning, e);
                    result.getEvents().add(event);
                } else {
                    logger.error(warning);
                    throw e;
                }
            }
        }
        return filterResults(result);
    }

    /**
     * Fetch all the study objects matching the query.
     *
     * @param projectStr     Project id, uuid or fqn.
     * @param query          Query to catalog.
     * @param options        Query options, like "include", "exclude", "limit" and "skip"
     * @param token          token
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Study> search(String projectStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        QueryOptions qOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn projectFqn = CatalogFqn.extractFqnFromProject(projectStr, tokenPayload);
        String organizationId = projectFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("projectStr", projectStr)
                .append("query", query)
                .append("options", options)
                .append("token", token);
        try {
            query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectFqn.getProjectId());
            query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_UUID.key(), projectFqn.getProjectUuid());
            fixQueryObject(query);

            OpenCGAResult<Study> studyDataResult = getStudyDBAdaptor(organizationId).get(query, qOptions, userId);
            auditManager.auditSearch(organizationId, userId, Enums.Resource.STUDY, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return filterResults(studyDataResult);
        } catch (CatalogException e) {
            auditManager.auditSearch(organizationId, userId, Enums.Resource.STUDY, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Fetch all the study objects matching the query.
     *
     * @param organizationId Organization id..
     * @param query          Query to catalog.
     * @param options        Query options, like "include", "exclude", "limit" and "skip"
     * @param token          token
     * @return All matching elements.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Study> searchInOrganization(String organizationId, Query query, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        QueryOptions qOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("query", query)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        organizationId = StringUtils.isNotEmpty(organizationId) ? organizationId : tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId(organizationId);

        try {
            fixQueryObject(query);

            OpenCGAResult<Study> studyDataResult = getStudyDBAdaptor(organizationId).get(query, qOptions, userId);
            auditManager.auditSearch(organizationId, userId, Enums.Resource.STUDY, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS),
                    new ObjectMap("numMatches", studyDataResult.getNumMatches()));
            return filterResults(studyDataResult);
        } catch (CatalogException e) {
            auditManager.auditSearch(organizationId, userId, Enums.Resource.STUDY, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private OpenCGAResult<Study> filterResults(OpenCGAResult<Study> result) {
        if (CollectionUtils.isEmpty(result.getResults())) {
            return result;
        }
        for (Study studyResult : result.getResults()) {
            // Filter out internal variable sets
            if (CollectionUtils.isNotEmpty(studyResult.getVariableSets())) {
                studyResult.getVariableSets().removeIf(VariableSet::isInternal);
            }
        }
        return result;
    }

    /**
     * Update an existing catalog study.
     *
     * @param studyId    Study id or alias.
     * @param parameters Parameters to change.
     * @param options    options
     * @param token      sessionId
     * @return The modified entry.
     * @throws CatalogException CatalogException
     */
    public OpenCGAResult<Study> update(String studyId, StudyUpdateParams parameters, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("studyId", studyId)
                .append("updateParams", parameters)
                .append("options", options)
                .append("token", token);

        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            ParamUtils.checkObj(parameters, "Parameters");

            authorizationManager.checkCanEditStudy(organizationId, study.getUid(), userId);

            if (StringUtils.isNotEmpty(parameters.getAlias())) {
                ParamUtils.checkIdentifier(parameters.getAlias(), "alias");
            }

            if (StringUtils.isNotEmpty(parameters.getCreationDate())) {
                ParamUtils.checkDateFormat(parameters.getCreationDate(), StudyDBAdaptor.QueryParams.CREATION_DATE.key());
            }
            if (StringUtils.isNotEmpty(parameters.getModificationDate())) {
                ParamUtils.checkDateFormat(parameters.getModificationDate(), StudyDBAdaptor.QueryParams.MODIFICATION_DATE.key());
            }

            ObjectMap update;
            try {
                update = new ObjectMap(getUpdateObjectMapper().writeValueAsString(parameters));
            } catch (JsonProcessingException e) {
                throw new CatalogException("Jackson casting error: " + e.getMessage(), e);
            }

            OpenCGAResult<Study> updateResult = getStudyDBAdaptor(organizationId).update(study.getUid(), update, options);
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated study
                OpenCGAResult<Study> result = getStudyDBAdaptor(organizationId).get(study.getUid(), options);
                updateResult.setResults(result.getResults());
            }

            return updateResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<PermissionRule> createPermissionRule(String studyId, Enums.Entity entry, PermissionRule permissionRule,
                                                              String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, INCLUDE_STUDY_IDS, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entry", entry)
                .append("permissionRule", permissionRule)
                .append("token", token);
        try {
            ParamUtils.checkObj(entry, "entry");
            ParamUtils.checkObj(permissionRule, "permission rule");

            authorizationManager.checkCanUpdatePermissionRules(organizationId, study.getUid(), userId);
            validatePermissionRules(organizationId, study.getUid(), entry, permissionRule);

            OpenCGAResult<PermissionRule> result = getStudyDBAdaptor(organizationId).createPermissionRule(study.getUid(), entry,
                    permissionRule);

            auditManager.audit(organizationId, userId, Enums.Action.ADD_STUDY_PERMISSION_RULE, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(result.getTime(), result.getEvents(), 1, Collections.singletonList(permissionRule), 1,
                    result.getNumInserted(), result.getNumUpdated(), result.getNumDeleted(), result.getNumErrors(), new ObjectMap());
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.ADD_STUDY_PERMISSION_RULE, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public void markDeletedPermissionRule(String studyId, Enums.Entity entry, String permissionRuleId,
                                          PermissionRule.DeleteAction deleteAction, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entry", entry)
                .append("permissionRuleId", permissionRuleId)
                .append("deleteAction", deleteAction)
                .append("token", token);
        try {
            ParamUtils.checkObj(entry, "entry");
            ParamUtils.checkObj(deleteAction, "Delete action");
            ParamUtils.checkObj(permissionRuleId, "permission rule id");

            authorizationManager.checkCanUpdatePermissionRules(organizationId, study.getUid(), userId);
            getStudyDBAdaptor(organizationId).markDeletedPermissionRule(study.getUid(), entry, permissionRuleId, deleteAction);

            auditManager.audit(organizationId, userId, Enums.Action.REMOVE_STUDY_PERMISSION_RULE, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.REMOVE_STUDY_PERMISSION_RULE, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<PermissionRule> getPermissionRules(String studyId, Enums.Entity entry, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entry", entry)
                .append("token", token);
        try {
            authorizationManager.checkCanViewStudy(organizationId, study.getUid(), userId);
            OpenCGAResult<PermissionRule> result = getStudyDBAdaptor(organizationId).getPermissionRules(study.getUid(), entry);

            auditManager.audit(organizationId, userId, Enums.Action.FETCH_STUDY_PERMISSION_RULES, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.FETCH_STUDY_PERMISSION_RULES, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult rank(String organizationId, long projectId, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(projectId, "projectId");

        JwtPayload payload = catalogManager.getUserManager().validateToken(token);
        String userId = payload.getUserId(organizationId);
        authorizationManager.checkCanViewProject(organizationId, projectId, userId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = getStudyDBAdaptor(organizationId).rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public OpenCGAResult groupBy(String organizationId, long projectId, Query query, String field, QueryOptions options, String token)
            throws CatalogException {
        return groupBy(organizationId, projectId, query, Collections.singletonList(field), options, token);
    }

    public OpenCGAResult groupBy(String organizationId, long projectId, Query query, List<String> fields, QueryOptions options,
                                 String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(projectId, "projectId");

        JwtPayload payload = catalogManager.getUserManager().validateToken(token);
        String userId = payload.getUserId(organizationId);
        authorizationManager.checkCanViewProject(organizationId, projectId, userId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = getStudyDBAdaptor(organizationId).groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public OpenCGAResult<StudySummary> getSummary(String studyStr, QueryOptions queryOptions, String token) throws CatalogException {
        long startTime = System.currentTimeMillis();

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        Study study = resolveId(studyFqn, queryOptions, tokenPayload);

        StudySummary studySummary = new StudySummary()
                .setAlias(study.getId())
                .setAttributes(study.getAttributes())
                .setCreationDate(study.getCreationDate())
                .setDescription(study.getDescription())
                .setSize(study.getSize())
                .setGroups(study.getGroups())
                .setName(study.getName())
                .setInternal(study.getInternal())
                .setVariableSets(study.getVariableSets());

        Long nFiles = getFileDBAdaptor(organizationId).count(
                        new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                                .append(FileDBAdaptor.QueryParams.TYPE.key(), File.Type.FILE)
                                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), "!=" + FileStatus.TRASHED + ";!="
                                        + FileStatus.DELETED))
                .getNumMatches();
        studySummary.setFiles(nFiles);

        Long nSamples = getSampleDBAdaptor(organizationId).count(new Query(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()))
                .getNumMatches();
        studySummary.setSamples(nSamples);

        Long nJobs = getJobDBAdaptor(organizationId).count(new Query(JobDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()))
                .getNumMatches();
        studySummary.setJobs(nJobs);

        Long nCohorts = getCohortDBAdaptor(organizationId).count(new Query(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()))
                .getNumMatches();
        studySummary.setCohorts(nCohorts);

        Long nIndividuals = getIndividualDBAdaptor(organizationId)
                .count(new Query(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()))
                .getNumMatches();
        studySummary.setIndividuals(nIndividuals);

        return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), 1,
                Collections.singletonList(studySummary), 1);
    }

    public List<OpenCGAResult<StudySummary>> getSummary(List<String> studyList, QueryOptions queryOptions, boolean ignoreException,
                                                        String token) throws CatalogException {
        List<OpenCGAResult<StudySummary>> results = new ArrayList<>(studyList.size());
        for (String study : studyList) {
            try {
                OpenCGAResult<StudySummary> summaryObj = getSummary(study, queryOptions, token);
                results.add(summaryObj);
            } catch (CatalogException e) {
                if (ignoreException) {
                    Event event = new Event(Event.Type.ERROR, study, e.getMessage());
                    results.add(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.emptyList(), 0));
                } else {
                    throw e;
                }
            }
        }
        return results;
    }

    public OpenCGAResult<Group> createGroup(String studyStr, String groupId, List<String> users, String token) throws CatalogException {
        ParamUtils.checkParameter(groupId, "group id");
        return createGroup(studyStr, new Group(groupId, users, null), token);
    }

    public OpenCGAResult<Group> createGroup(String studyId, Group group, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("group", group)
                .append("token", token);
        try {
            ParamUtils.checkObj(group, "group");
            ParamUtils.checkGroupId(group.getId());
            group.setUserIds(ParamUtils.defaultObject(group.getUserIds(), Collections::emptyList));

            if (group.getSyncedFrom() != null) {
                ParamUtils.checkParameter(group.getSyncedFrom().getAuthOrigin(), "Authentication origin");
                ParamUtils.checkParameter(group.getSyncedFrom().getRemoteGroup(), "Remote group id");
            }

            // Fix the group id
            if (!group.getId().startsWith("@")) {
                group.setId("@" + group.getId());
            }

            authorizationManager.checkCreateDeleteGroupPermissions(organizationId, study.getUid(), userId, group.getId());

            // Check group exists
            if (existsGroup(organizationId, study.getUid(), group.getId())) {
                throw new CatalogException("The group " + group.getId() + " already exists.");
            }

            List<String> users = group.getUserIds();
            if (CollectionUtils.isNotEmpty(users)) {
                // We remove possible duplicates
                users = users.stream().collect(Collectors.toSet()).stream().collect(Collectors.toList());
                getUserDBAdaptor(organizationId).checkIds(users);
            } else {
                users = Collections.emptyList();
            }
            group.setUserIds(users);

            // Add those users to the members group
            if (CollectionUtils.isNotEmpty(users)) {
                getStudyDBAdaptor(organizationId).addUsersToGroup(study.getUid(), MEMBERS, users);
            }

            // Create the group
            OpenCGAResult result = getStudyDBAdaptor(organizationId).createGroup(study.getUid(), group);

            OpenCGAResult<Group> queryResult = getStudyDBAdaptor(organizationId).getGroup(study.getUid(), group.getId(), null);
            queryResult.setTime(queryResult.getTime() + result.getTime());

            auditManager.audit(organizationId, userId, Enums.Action.ADD_STUDY_GROUP, Enums.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.ADD_STUDY_GROUP, Enums.Resource.STUDY, study.getId(), study.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Group> getGroup(String studyId, String groupId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("groupId", groupId)
                .append("token", token);
        try {
            authorizationManager.checkCanViewStudy(organizationId, study.getUid(), userId);

            // Fix the groupId
            if (groupId != null && !groupId.startsWith("@")) {
                groupId = "@" + groupId;
            }

            OpenCGAResult<Group> result = getStudyDBAdaptor(organizationId).getGroup(study.getUid(), groupId, Collections.emptyList());
            auditManager.audit(organizationId, userId, Enums.Action.FETCH_STUDY_GROUPS, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.FETCH_STUDY_GROUPS, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<CustomGroup> getCustomGroups(String studyId, String groupId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("groupId", groupId)
                .append("token", token);
        try {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);

            // Fix the groupId
            if (groupId != null && !groupId.startsWith("@")) {
                groupId = "@" + groupId;
            }

            OpenCGAResult<Group> result = getStudyDBAdaptor(organizationId).getGroup(study.getUid(), groupId, Collections.emptyList());

            // Extract all users from all groups
            Set<String> userIds = new HashSet<>();
            for (Group group : result.getResults()) {
                userIds.addAll(group.getUserIds());
            }

            Query userQuery = new Query(UserDBAdaptor.QueryParams.ID.key(), new ArrayList<>(userIds));
            QueryOptions userOptions = new QueryOptions();
            OpenCGAResult<User> userResult = getUserDBAdaptor(organizationId).get(userQuery, userOptions);
            Map<String, User> userMap = new HashMap<>();
            for (User user : userResult.getResults()) {
                userMap.put(user.getId(), user);
            }

            // Generate groups with list of full users
            List<CustomGroup> customGroupList = new ArrayList<>(result.getNumResults());
            for (Group group : result.getResults()) {
                List<User> userList = new ArrayList<>(group.getUserIds().size());
                for (String tmpUserId : group.getUserIds()) {
                    if (userMap.containsKey(tmpUserId)) {
                        userList.add(userMap.get(tmpUserId));
                    }
                }
                customGroupList.add(new CustomGroup(group.getId(), userList, group.getSyncedFrom()));
            }

            OpenCGAResult<CustomGroup> finalResult = new OpenCGAResult<>(result.getTime(), result.getEvents(), result.getNumResults(),
                    customGroupList, result.getNumMatches(), result.getNumInserted(), result.getNumUpdated(), result.getNumDeleted(),
                    result.getNumErrors(), result.getAttributes(), result.getFederationNode());

            auditManager.audit(organizationId, userId, Enums.Action.FETCH_STUDY_GROUPS, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            stopWatch.stop();
            finalResult.setTime((int) stopWatch.getTime(TimeUnit.MILLISECONDS));

            return finalResult;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.FETCH_STUDY_GROUPS, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Group> updateGroup(String studyId, String groupId, ParamUtils.BasicUpdateAction action,
                                            GroupUpdateParams updateParams, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("groupId", groupId)
                .append("action", action)
                .append("updateParams", updateParams)
                .append("token", token);
        try {
            ParamUtils.checkObj(updateParams, "Group parameters");
            ParamUtils.checkParameter(groupId, "Group name");
            ParamUtils.checkObj(action, "Action");

            // Fix the group name
            if (!groupId.startsWith("@")) {
                groupId = "@" + groupId;
            }

            authorizationManager.checkUpdateGroupPermissions(organizationId, study.getUid(), userId, groupId, action);

            if (CollectionUtils.isNotEmpty(updateParams.getUsers())) {
                List<String> tmpUsers = updateParams.getUsers();
                if (groupId.equals(MEMBERS) || groupId.equals(ADMINS)) {
                    // Remove anonymous user if present for the checks.
                    // Anonymous user is only allowed in MEMBERS group, otherwise we keep it as if it is present it should fail.
                    tmpUsers = updateParams.getUsers().stream()
                            .filter(user -> !user.equals(ParamConstants.ANONYMOUS_USER_ID))
                            .filter(user -> !user.equals(ParamConstants.REGISTERED_USERS))
                            .collect(Collectors.toList());
                }
                if (tmpUsers.size() > 0) {
                    getUserDBAdaptor(organizationId).checkIds(tmpUsers);
                }
            } else {
                updateParams.setUsers(Collections.emptyList());
            }

            switch (action) {
                case SET:
                    if (MEMBERS.equals(groupId)) {
                        throw new CatalogException("Operation not valid. Valid actions over the '@members' group are ADD or REMOVE.");
                    } else if (ADMINS.equals(groupId)) {
                        Set<String> admins = catalogManager.getOrganizationManager().getOrganizationOwnerAndAdmins(organizationId);
                        if (!updateParams.getUsers().containsAll(admins)) {
                            throw new CatalogException("Can't set the users provided to the " + ADMINS + " group. Organization owner "
                                    + "and admins cannot be removed from that group. Please ensure these users are also added: "
                                    + String.join(", ", admins));
                        }
                    }
                    getStudyDBAdaptor(organizationId).setUsersToGroup(study.getUid(), groupId, updateParams.getUsers());
                    getStudyDBAdaptor(organizationId).addUsersToGroup(study.getUid(), MEMBERS, updateParams.getUsers());
                    break;
                case ADD:
                    getStudyDBAdaptor(organizationId).addUsersToGroup(study.getUid(), groupId, updateParams.getUsers());
                    if (!MEMBERS.equals(groupId)) {
                        getStudyDBAdaptor(organizationId).addUsersToGroup(study.getUid(), MEMBERS, updateParams.getUsers());
                    }
                    break;
                case REMOVE:
                    // Check is not one of the organization admins
                    if (MEMBERS.equals(groupId) || ADMINS.equals(groupId)) {
                        Set<String> admins = catalogManager.getOrganizationManager().getOrganizationOwnerAndAdmins(organizationId);
                        for (String user : updateParams.getUsers()) {
                            if (admins.contains(user)) {
                                throw new CatalogException("Cannot remove user '" + user + "'. The user is either the organization owner "
                                        + "or one of the organization administrators.");
                            }
                        }
                    }

                    if (MEMBERS.equals(groupId)) {
                        // We remove the users from all the groups and acls
                        authorizationManager.resetPermissionsFromAllEntities(organizationId, study.getUid(), updateParams.getUsers());
                        getStudyDBAdaptor(organizationId).removeUsersFromAllGroups(study.getUid(), updateParams.getUsers());
                    } else {
                        getStudyDBAdaptor(organizationId).removeUsersFromGroup(study.getUid(), groupId, updateParams.getUsers());
                    }
                    break;
                default:
                    throw new CatalogException("Unknown action " + action + " found.");
            }

            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return getStudyDBAdaptor(organizationId).getGroup(study.getUid(), groupId, Collections.emptyList());
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<?> updateSummaryIndex(String studyStr, RecessiveGeneSummaryIndex summaryIndex, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("recessiveGeneSummaryIndex", summaryIndex)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);

            ParamUtils.checkObj(summaryIndex, "RecessiveGeneSummaryIndex");
            summaryIndex.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(summaryIndex.getModificationDate(), "creationDate"));

            ObjectMap update;
            try {
                update = new ObjectMap(StudyDBAdaptor.QueryParams.INTERNAL_INDEX_RECESSIVE_GENE.key(),
                        new ObjectMap(getUpdateObjectMapper().writeValueAsString(summaryIndex)));
            } catch (JsonProcessingException e) {
                throw new CatalogException("Jackson casting error: " + e.getMessage(), e);
            }

            OpenCGAResult<Study> result = getStudyDBAdaptor(organizationId).update(study.getUid(), update, QueryOptions.empty());

            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_INTERNAL, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_INTERNAL, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

    }

//    public OpenCGAResult<Group> syncGroupWith(String studyStr, String groupId, Group.Sync syncedFrom, String sessionId)
//            throws CatalogException {
//        ParamUtils.checkObj(syncedFrom, "sync");
//
//        String userId = catalogManager.getUserManager().getUserId(sessionId);
//        Study study = resolveId(studyStr, userId);
//
//        if (StringUtils.isEmpty(groupId)) {
//            throw new CatalogException("Missing group name parameter");
//        }
//
//        // Fix the groupId
//        if (!groupId.startsWith("@")) {
//            groupId = "@" + groupId;
//        }
//
//        authorizationManager.checkSyncGroupPermissions(study.getUid(), userId, groupId);
//
//        OpenCGAResult<Group> group = studyDBAdaptor.getGroup(study.getUid(), groupId, Collections.emptyList());
//        if (group.first().getSyncedFrom() != null && StringUtils.isNotEmpty(group.first().getSyncedFrom().getAuthOrigin())
//                && StringUtils.isNotEmpty(group.first().getSyncedFrom().getRemoteGroup())) {
//            throw new CatalogException("Cannot modify already existing sync information.");
//        }
//
//        // Check the group exists
//        Query query = new Query()
//                .append(StudyDBAdaptor.QueryParams.UID.key(), study.getUid())
//                .append(StudyDBAdaptor.QueryParams.GROUP_ID.key(), groupId);
//        if (studyDBAdaptor.count(query).getNumMatches() == 0) {
//            throw new CatalogException("The group " + groupId + " does not exist.");
//        }
//
//        studyDBAdaptor.syncGroup(study.getUid(), groupId, syncedFrom);
//
//        return studyDBAdaptor.getGroup(study.getUid(), groupId, Collections.emptyList());
//    }

    public OpenCGAResult<Group> deleteGroup(String studyId, String groupId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("groupId", groupId)
                .append("token", token);
        try {
            if (StringUtils.isEmpty(groupId)) {
                throw new CatalogException("Missing group id");
            }

            // Fix the groupId
            if (!groupId.startsWith("@")) {
                groupId = "@" + groupId;
            }

            authorizationManager.checkCreateDeleteGroupPermissions(organizationId, study.getUid(), userId, groupId);

            OpenCGAResult<Group> group = getStudyDBAdaptor(organizationId).getGroup(study.getUid(), groupId, Collections.emptyList());

            // Remove the permissions the group might have had
            StudyAclParams aclParams = new StudyAclParams(null, null);
            updateAcl(studyId, groupId, aclParams, ParamUtils.AclAction.RESET, token);

            getStudyDBAdaptor(organizationId).deleteGroup(study.getUid(), groupId);

            auditManager.audit(organizationId, userId, Enums.Action.REMOVE_STUDY_GROUP, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return group;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.REMOVE_STUDY_GROUP, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<VariableSetSummary> getVariableSetSummary(String studyStr, String variableSetStr, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        VariableSet variableSet = extractVariableSet(organizationId, study, variableSetStr, userId);

        int dbTime = 0;
        VariableSetSummary variableSetSummary = new VariableSetSummary(variableSet.getUid(), variableSet.getId());

        OpenCGAResult<VariableSummary> annotationSummary = getSampleDBAdaptor(organizationId).getAnnotationSummary(study.getUid(),
                variableSet.getUid());
        dbTime += annotationSummary.getTime();
        variableSetSummary.setSamples(annotationSummary.getResults());

        annotationSummary = getCohortDBAdaptor(organizationId).getAnnotationSummary(study.getUid(), variableSet.getUid());
        dbTime += annotationSummary.getTime();
        variableSetSummary.setCohorts(annotationSummary.getResults());

        annotationSummary = getIndividualDBAdaptor(organizationId).getAnnotationSummary(study.getUid(), variableSet.getUid());
        dbTime += annotationSummary.getTime();
        variableSetSummary.setIndividuals(annotationSummary.getResults());

        annotationSummary = getFamilyDBAdaptor(organizationId).getAnnotationSummary(study.getUid(), variableSet.getUid());
        dbTime += annotationSummary.getTime();
        variableSetSummary.setFamilies(annotationSummary.getResults());

        return new OpenCGAResult<>(dbTime, Collections.emptyList(), 1, Collections.singletonList(variableSetSummary), 1);
    }

    private void validateNewVariableSet(VariableSet variableSet, int release) throws CatalogException {
        ParamUtils.checkParameter(variableSet.getId(), "id");
        ParamUtils.checkObj(variableSet.getVariables(), "Variables from VariableSet");

        variableSet.setDescription(ParamUtils.defaultString(variableSet.getDescription(), ""));
        variableSet.setAttributes(ParamUtils.defaultObject(variableSet.getAttributes(), new HashMap<>()));
        variableSet.setEntities(ParamUtils.defaultObject(variableSet.getEntities(), Collections.emptyList()));
        variableSet.setName(ParamUtils.defaultString(variableSet.getName(), variableSet.getId()));
        variableSet.setRelease(release);

        for (Variable variable : variableSet.getVariables()) {
            ParamUtils.checkParameter(variable.getId(), "variable ID");
            ParamUtils.checkObj(variable.getType(), "variable Type");
            variable.setAllowedValues(ParamUtils.defaultObject(variable.getAllowedValues(), Collections.emptyList()));
            variable.setAttributes(ParamUtils.defaultObject(variable.getAttributes(), Collections.emptyMap()));
            variable.setCategory(ParamUtils.defaultString(variable.getCategory(), ""));
            variable.setDependsOn(ParamUtils.defaultString(variable.getDependsOn(), ""));
            variable.setDescription(ParamUtils.defaultString(variable.getDescription(), ""));
            variable.setName(ParamUtils.defaultString(variable.getName(), variable.getId()));
//            variable.setRank(defaultString(variable.getDescription(), ""));
        }
    }

    /*
     * Variables Methods
     */
    private OpenCGAResult<VariableSet> createVariableSet(String organizationId, Study study, VariableSet variableSet, String token)
            throws CatalogException {
        ParamUtils.checkParameter(variableSet.getId(), "id");
        ParamUtils.checkObj(variableSet.getVariables(), "Variables from VariableSet");
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        String userId = jwtPayload.getUserId(organizationId);
        authorizationManager.checkCanCreateUpdateDeleteVariableSets(organizationId, study.getUid(), userId);

        variableSet.setDescription(ParamUtils.defaultString(variableSet.getDescription(), ""));
        variableSet.setAttributes(ParamUtils.defaultObject(variableSet.getAttributes(), new HashMap<>()));
        variableSet.setEntities(ParamUtils.defaultObject(variableSet.getEntities(), Collections.emptyList()));
        variableSet.setName(ParamUtils.defaultString(variableSet.getName(), variableSet.getId()));

        for (Variable variable : variableSet.getVariables()) {
            ParamUtils.checkParameter(variable.getId(), "variable ID");
            ParamUtils.checkObj(variable.getType(), "variable Type");
            variable.setAllowedValues(ParamUtils.defaultObject(variable.getAllowedValues(), Collections.emptyList()));
            variable.setAttributes(ParamUtils.defaultObject(variable.getAttributes(), Collections.emptyMap()));
            variable.setCategory(ParamUtils.defaultString(variable.getCategory(), ""));
            variable.setDependsOn(ParamUtils.defaultString(variable.getDependsOn(), ""));
            variable.setDescription(ParamUtils.defaultString(variable.getDescription(), ""));
            variable.setName(ParamUtils.defaultString(variable.getName(), variable.getId()));
//            variable.setRank(defaultString(variable.getDescription(), ""));
        }

        variableSet.setRelease(getCurrentRelease(study));
        AnnotationUtils.checkVariableSet(variableSet);

        OpenCGAResult<VariableSet> result = getStudyDBAdaptor(organizationId).createVariableSet(study.getUid(), variableSet);
        OpenCGAResult<VariableSet> queryResult = getStudyDBAdaptor(organizationId).getVariableSet(study.getUid(), variableSet.getId(),
                QueryOptions.empty());

        return OpenCGAResult.merge(Arrays.asList(result, queryResult));
    }

    @Deprecated
    public OpenCGAResult<VariableSet> createVariableSet(String studyId, String id, String name, Boolean unique, Boolean confidential,
                                                        String description, Map<String, Object> attributes, List<Variable> variables,
                                                        List<VariableSet.AnnotableDataModels> entities, String token)
            throws CatalogException {
        return createVariableSet(studyId, new VariableSet(id, name, unique != null ? unique : true,
                confidential != null ? confidential : false, false, description, new HashSet<>(variables), entities, 1, attributes), token);
    }

    public OpenCGAResult<VariableSet> createVariableSet(String studyId, VariableSet variableSet, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("variableSet", variableSet)
                .append("token", token);
        try {
            OpenCGAResult<VariableSet> queryResult = createVariableSet(organizationId, study, variableSet, token);
            auditManager.audit(organizationId, userId, Enums.Action.ADD_VARIABLE_SET, Enums.Resource.STUDY, queryResult.first().getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.ADD_VARIABLE_SET, Enums.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<VariableSet> getVariableSet(String studyId, String variableSetId, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("variableSetId", variableSetId)
                .append("options", options)
                .append("token", token);
        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            VariableSet variableSet = extractVariableSet(organizationId, study, variableSetId, userId);
            OpenCGAResult<VariableSet> result = getStudyDBAdaptor(organizationId).getVariableSet(variableSet.getUid(), options, userId);

            auditManager.audit(organizationId, userId, Enums.Action.FETCH_VARIABLE_SET, Enums.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.FETCH_VARIABLE_SET, Enums.Resource.STUDY, variableSetId, "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<VariableSet> deleteVariableSet(String studyId, String variableSetId, boolean force, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);
        VariableSet variableSet = extractVariableSet(organizationId, study, variableSetId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("variableSetId", variableSetId)
                .append("force", force)
                .append("token", token);
        try {
            authorizationManager.checkCanCreateUpdateDeleteVariableSets(organizationId, study.getUid(), userId);
            OpenCGAResult writeResult = getStudyDBAdaptor(organizationId).deleteVariableSet(study.getUid(), variableSet, force);
            auditManager.audit(organizationId, userId, Enums.Action.DELETE_VARIABLE_SET, Enums.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return writeResult;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.DELETE_VARIABLE_SET, Enums.Resource.STUDY, variableSet.getId(), "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<VariableSet> addFieldToVariableSet(String studyId, String variableSetId, Variable variable, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);
        VariableSet variableSet = extractVariableSet(organizationId, study, variableSetId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("variableSetId", variableSetId)
                .append("variable", variable)
                .append("token", token);
        try {
            if (StringUtils.isEmpty(variable.getId())) {
                if (StringUtils.isEmpty(variable.getName())) {
                    throw new CatalogException("Missing variable id");
                }
                variable.setId(variable.getName());
            }

            authorizationManager.checkCanCreateUpdateDeleteVariableSets(organizationId, study.getUid(), userId);
            OpenCGAResult<VariableSet> result = getStudyDBAdaptor(organizationId).addFieldToVariableSet(study.getUid(),
                    variableSet.getUid(), variable, userId);
            auditManager.audit(organizationId, userId, Enums.Action.ADD_VARIABLE_TO_VARIABLE_SET, Enums.Resource.STUDY, variableSet.getId(),
                    "", study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            OpenCGAResult<VariableSet> queryResult = getStudyDBAdaptor(organizationId).getVariableSet(variableSet.getUid(),
                    QueryOptions.empty());
            queryResult.setTime(queryResult.getTime() + result.getTime());
            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.ADD_VARIABLE_TO_VARIABLE_SET, Enums.Resource.STUDY, variableSet.getId(),
                    "", study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<VariableSet> removeFieldFromVariableSet(String studyId, String variableSetId, String variableId, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);
        VariableSet variableSet = extractVariableSet(organizationId, study, variableSetId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("study", studyId)
                .append("variableSetId", variableSetId)
                .append("variableId", variableId)
                .append("token", token);

        try {
            authorizationManager.checkCanCreateUpdateDeleteVariableSets(organizationId, study.getUid(), userId);
            OpenCGAResult<VariableSet> result = getStudyDBAdaptor(organizationId).removeFieldFromVariableSet(study.getUid(),
                    variableSet.getUid(), variableId, userId);
            auditManager.audit(organizationId, userId, Enums.Action.REMOVE_VARIABLE_FROM_VARIABLE_SET, Enums.Resource.STUDY,
                    variableSet.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            OpenCGAResult<VariableSet> queryResult = getStudyDBAdaptor(organizationId).getVariableSet(variableSet.getUid(),
                    QueryOptions.empty());
            queryResult.setTime(queryResult.getTime() + result.getTime());
            return queryResult;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.REMOVE_VARIABLE_FROM_VARIABLE_SET, Enums.Resource.STUDY,
                    variableSet.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private VariableSet extractVariableSet(String organizationId, Study study, String variableSetId, String userId)
            throws CatalogException {
        if (study == null || study.getVariableSets() == null || study.getVariableSets().isEmpty()) {
            throw new CatalogException(variableSetId + " not found.");
        }
        if (StringUtils.isEmpty(variableSetId)) {
            throw new CatalogException("Missing 'variableSetId' variable");
        }

        Query query = new Query(StudyDBAdaptor.VariableSetParams.STUDY_UID.key(), study.getUid())
                .append(StudyDBAdaptor.VariableSetParams.ID.key(), variableSetId);

        // We query again because we only want to return the variable set if the user can access it
        OpenCGAResult<VariableSet> queryResult = getStudyDBAdaptor(organizationId).getVariableSets(query, new QueryOptions(), userId);
        if (queryResult.getNumResults() == 0) {
            throw new CatalogException(variableSetId + " not found.");
        }
        return queryResult.first();
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getAcls(List<String> studyIdList, String member,
                                                                             boolean ignoreException, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        ParamUtils.checkObj(studyIdList, "studyIdList");
        List<CatalogFqn> fqnList = new ArrayList<>(studyIdList.size());
        for (String studyStr : studyIdList) {
            fqnList.add(CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload));
        }
        // Validate all studies belong to the same organization
        List<String> organizationIds = fqnList.stream().map(CatalogFqn::getOrganizationId).distinct().collect(Collectors.toList());
        if (organizationIds.size() > 1) {
            throw new CatalogException("More than organization found for the studies provided: " + StringUtils.join(organizationIds, ", "));
        }
        String organizationId = organizationIds.get(0);
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("studyIdList", studyIdList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> studyAclList = OpenCGAResult.empty();

        for (CatalogFqn studyFqn : fqnList) {
            Study study = resolveId(studyFqn, null, tokenPayload);
            long studyId = study.getUid();
            try {
                OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> allStudyAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allStudyAcls = authorizationManager.getStudyAcl(organizationId, studyId, member, userId);
                } else {
                    allStudyAcls = authorizationManager.getAllStudyAcls(organizationId, studyId, userId);
                }
                studyAclList.append(allStudyAcls);

                auditManager.audit(organizationId, operationUuid, userId, Enums.Action.FETCH_ACLS, Enums.Resource.STUDY, study.getId(),
                        study.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            } catch (CatalogException e) {
                auditManager.audit(organizationId, operationUuid, userId, Enums.Action.FETCH_ACLS, Enums.Resource.STUDY, study.getId(),
                        study.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                if (ignoreException) {
                    Event event = new Event(Event.Type.ERROR, study.getFqn(), e.getMessage());
                    studyAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.singletonList(null), 0));
                } else {
                    throw e;
                }
            }
        }
        return studyAclList;
    }

    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> updateAcl(String studyStr, String memberIds, StudyAclParams aclParams,
                                                                               ParamUtils.AclAction action, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("memberIds", memberIds)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        try {
            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, StudyPermissions.Permissions::valueOf);
            }

            if (StringUtils.isNotEmpty(aclParams.getTemplate())) {
                EnumSet<StudyPermissions.Permissions> studyPermissions;
                switch (aclParams.getTemplate()) {
                    case AuthorizationManager.ROLE_ADMIN:
                        studyPermissions = AuthorizationManager.getAdminAcls();
                        break;
                    case AuthorizationManager.ROLE_ANALYST:
                        studyPermissions = AuthorizationManager.getAnalystAcls();
                        break;
                    case AuthorizationManager.ROLE_VIEW_ONLY:
                        studyPermissions = AuthorizationManager.getViewOnlyAcls();
                        break;
                    case AuthorizationManager.ROLE_LOCKED:
                        studyPermissions = AuthorizationManager.getLockedAcls();
                        break;
                    default:
                        throw new CatalogException("Admissible template ids are: " + AuthorizationManager.ROLE_ADMIN + ", "
                                + AuthorizationManager.ROLE_ANALYST + ", " + AuthorizationManager.ROLE_VIEW_ONLY + ", "
                                + AuthorizationManager.ROLE_LOCKED);
                }

                if (studyPermissions != null) {
                    // Merge permissions from the template with the ones written
                    Set<String> uniquePermissions = new HashSet<>(permissions);

                    for (StudyPermissions.Permissions studyPermission : studyPermissions) {
                        uniquePermissions.add(studyPermission.toString());
                    }

                    permissions = new ArrayList<>(uniquePermissions);
                }
            }

            // Check the user has the permissions needed to change permissions
            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);


            // Validate that the members are actually valid members
            List<String> members;
            if (memberIds != null && !memberIds.isEmpty()) {
                members = Arrays.asList(memberIds.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(organizationId, study.getUid(), members);

            switch (action) {
                case SET:
                    authorizationManager.setStudyAcls(organizationId, Collections.singletonList(study.getUid()), members, permissions);
                    break;
                case ADD:
                    authorizationManager.addStudyAcls(organizationId, Collections.singletonList(study.getUid()), members, permissions);
                    break;
                case REMOVE:
                    authorizationManager.removeStudyAcls(organizationId, Collections.singletonList(study.getUid()), members, permissions);
                    break;
                case RESET:
                    authorizationManager.removeStudyAcls(organizationId, Collections.singletonList(study.getUid()), members, null);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> remainingAcls = authorizationManager.getStudyAcl(organizationId,
                    study.getUid(), members);

            auditManager.audit(organizationId, operationUuid, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());

            return remainingAcls;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, operationUuid, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.STUDY, study.getId(),
                    study.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());

            throw e;
        }
    }

    // **************************   Protected internal methods  ******************************** //

    public void setVariantEngineConfigurationOptions(String studyStr, ObjectMap options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn,
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                        StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_VARIANT_ENGINE.key())),
                tokenPayload);

        authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);
        StudyVariantEngineConfiguration configuration = study.getInternal().getConfiguration().getVariantEngine();
        if (configuration == null) {
            configuration = new StudyVariantEngineConfiguration();
        }
        configuration.setOptions(options);

        ObjectMap parameters = new ObjectMap(StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_VARIANT_ENGINE.key(), configuration);
        getStudyDBAdaptor(organizationId).update(study.getUid(), parameters, QueryOptions.empty());
    }

    public void setVariantEngineSetupOptions(String studyStr, VariantSetupResult variantSetupResult, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn,
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                        StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_VARIANT_ENGINE.key())),
                tokenPayload);

        authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);
        StudyVariantEngineConfiguration configuration = study.getInternal().getConfiguration().getVariantEngine();
        if (configuration == null) {
            configuration = new StudyVariantEngineConfiguration();
        }
        if (configuration.getOptions() == null) {
            configuration.setOptions(new ObjectMap());
        }
        VariantSetupResult prevSetupValue = configuration.getSetup();
        if (prevSetupValue != null && prevSetupValue.getOptions() != null) {
            // Variant setup was already executed.
            // Remove the options from the previous execution before adding the new ones
            // Check that both key/value matches, to avoid removing options that might have been modified manually
            for (Map.Entry<String, Object> entry : prevSetupValue.getOptions().entrySet()) {
                configuration.getOptions().remove(entry.getKey(), entry.getValue());
            }
        }
        configuration.getOptions().putAll(variantSetupResult.getOptions());
        configuration.setSetup(variantSetupResult);

        ObjectMap parameters = new ObjectMap(StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_VARIANT_ENGINE.key(), configuration);
        getStudyDBAdaptor(organizationId).update(study.getUid(), parameters, QueryOptions.empty());
    }

    public void setVariantEngineConfigurationSampleIndex(String studyStr, SampleIndexConfiguration sampleIndexConfiguration, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn,
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                        StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_VARIANT_ENGINE.key())),
                tokenPayload);

        authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);
        StudyVariantEngineConfiguration configuration = study.getInternal().getConfiguration().getVariantEngine();
        if (configuration == null) {
            configuration = new StudyVariantEngineConfiguration();
        }
        configuration.setSampleIndex(sampleIndexConfiguration);

        ObjectMap parameters = new ObjectMap(StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_VARIANT_ENGINE.key(), configuration);
        getStudyDBAdaptor(organizationId).update(study.getUid(), parameters, QueryOptions.empty());
    }

    // **************************   Private methods  ******************************** //

    private boolean existsGroup(String organizationId, long studyId, String groupId) throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.UID.key(), studyId)
                .append(StudyDBAdaptor.QueryParams.GROUP_ID.key(), groupId);
        return getStudyDBAdaptor(organizationId).count(query).getNumMatches() > 0;
    }

    private void validatePermissionRules(String organizationId, long studyId, Enums.Entity entry, PermissionRule permissionRule)
            throws CatalogException {
        ParamUtils.checkIdentifier(permissionRule.getId(), "PermissionRules");

        if (permissionRule.getPermissions() == null || permissionRule.getPermissions().isEmpty()) {
            throw new CatalogException("Missing permissions for the Permissions Rule object");
        }

        switch (entry) {
            case SAMPLES:
                validatePermissions(permissionRule.getPermissions(), SamplePermissions::valueOf);
                break;
            case FILES:
                validatePermissions(permissionRule.getPermissions(), FilePermissions::valueOf);
                break;
            case COHORTS:
                validatePermissions(permissionRule.getPermissions(), CohortPermissions::valueOf);
                break;
            case INDIVIDUALS:
                validatePermissions(permissionRule.getPermissions(), IndividualPermissions::valueOf);
                break;
            case FAMILIES:
                validatePermissions(permissionRule.getPermissions(), FamilyPermissions::valueOf);
                break;
            case JOBS:
                validatePermissions(permissionRule.getPermissions(), JobPermissions::valueOf);
                break;
            case CLINICAL_ANALYSES:
                validatePermissions(permissionRule.getPermissions(), ClinicalAnalysisPermissions::valueOf);
                break;
            default:
                throw new CatalogException("Unexpected entry found");
        }

        checkMembers(organizationId, studyId, permissionRule.getMembers());
    }

    private void validatePermissions(List<String> permissions, Function<String, Object> valueOf) throws CatalogException {
        for (String permission : permissions) {
            try {
                valueOf.apply(permission);
            } catch (IllegalArgumentException e) {
                logger.error("Detected unsupported " + permission + " permission: {}", e.getMessage(), e);
                throw new CatalogException("Detected unsupported " + permission + " permission.");
            }
        }
    }

    public String getProjectFqn(String studyFqn) throws CatalogException {
        return CatalogFqn.extractFqnFromStudyFqn(studyFqn).toProjectFqn();
    }

    /*
    ====================== TEMPLATES ======================
     */

    /**
     * Upload a template file in Catalog.
     *
     * @param studyStr    study where the file will be uploaded.
     * @param filename    File name.
     * @param inputStream Input stream of the file to be uploaded (must be a .zip or .tar.gz file).
     * @param token       Token of the user performing the upload.
     * @return an empty OpenCGAResult if it successfully uploaded.
     * @throws CatalogException if there is any issue with the upload.
     */
    public OpenCGAResult<String> uploadTemplate(String studyStr, String filename, InputStream inputStream, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        String templateId = "template." + TimeUtils.getTime() + "." + RandomStringUtils.random(6, true, false);

        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("filename", filename)
                .append("token", token);
        try {
            StopWatch stopWatch = StopWatch.createStarted();

            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);

            ParamUtils.checkParameter(filename, "File name");
            if (!filename.endsWith(".zip") && !filename.endsWith(".tar.gz")) {
                throw new CatalogException("Expected zip or tar.gz file");
            }

            // We obtain the basic studyPath where we will upload the file temporarily
            java.nio.file.Path studyPath = Paths.get(study.getUri());
            Path path = studyPath.resolve("OPENCGA").resolve("TEMPLATE").resolve(templateId);

            IOManager ioManager;
            try {
                ioManager = ioManagerFactory.get(study.getUri());
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(study.getUri(), e);
            }
            if (ioManager.exists(path.toUri())) {
                throw new CatalogException("Template '" + templateId + "' already exists");
            }

            Path filePath = path.resolve(filename);
            URI fileUri = path.resolve(filename).toUri();
            try {
                logger.debug("Creating folder '{}' to write the template file", path);
                ioManager.createDirectory(path.toUri(), true);

                // Start uploading the file to the directory
                ioManager.copy(inputStream, fileUri);
            } catch (Exception e) {
                logger.error("Error uploading file '{}'. Trying to clean directory '{}'", filename, path, e);

                // Clean temporal directory
                ioManager.deleteDirectory(path.toUri());

                throw new CatalogException("Error uploading file " + filename, e);
            }

            // Decompress file
            try {
                ioManager.decompress(filePath, path);
            } catch (CatalogIOException e) {
                logger.error("Error decompressing file '{}'. Trying to clean directory '{}'", filePath, path, e);

                // Clean temporal directory
                ioManager.deleteDirectory(path.toUri());

                throw new CatalogException("Error decompressing file: '" + filePath + "'", e);
            }

            return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(),
                    1, Collections.singletonList(templateId), 1);
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Action.UPLOAD_TEMPLATE, Enums.Resource.STUDY, templateId, "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, userId, Enums.Action.UPLOAD_TEMPLATE, Enums.Resource.STUDY, templateId, "",
             study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                            new Error(-1, "template upload", e.getMessage())));
            throw e;
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error("Could not close InputStream. Already closed?");
            }
        }
    }

    /**
     * Delete a template from Catalog.
     *
     * @param studyStr   study where the template belongs to.
     * @param templateId Template id.
     * @param token      Token of the user performing the upload.
     * @return an empty OpenCGAResult if it successfully uploaded.
     * @throws CatalogException if there is any issue with the upload.
     */
    public OpenCGAResult<Boolean> deleteTemplate(String studyStr, String templateId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = resolveId(studyFqn, QueryOptions.empty(), tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("templateId", templateId)
                .append("token", token);
        try {
            StopWatch stopWatch = StopWatch.createStarted();

            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);

            // We obtain the basic studyPath where we will upload the file temporarily
            java.nio.file.Path studyPath = Paths.get(study.getUri());
            Path path = studyPath.resolve("OPENCGA").resolve("TEMPLATE").resolve(templateId);

            IOManager ioManager;
            try {
                ioManager = ioManagerFactory.get(study.getUri());
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(study.getUri(), e);
            }
            if (!ioManager.exists(path.toUri())) {
                throw new CatalogException("Template '" + templateId + "' doesn't exist");
            }

            ioManager.deleteDirectory(path.toUri());

            return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), null, 1, Collections.singletonList(true), 1);
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Action.DELETE_TEMPLATE, Enums.Resource.STUDY, templateId, "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, userId, Enums.Action.DELETE_TEMPLATE, Enums.Resource.STUDY, templateId, "",
             study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                            new Error(-1, "template delete", e.getMessage())));
            throw e;
        }
    }

}
