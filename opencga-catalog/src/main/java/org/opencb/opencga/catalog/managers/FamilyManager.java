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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.OntologyTerm;
import org.opencb.opencga.core.models.VariableSet;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FamilyAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * Created by pfurio on 02/05/17.
 */
public class FamilyManager extends AnnotationSetManager<Family> {

    protected static Logger logger = LoggerFactory.getLogger(FamilyManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    FamilyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param familyStr Family id in string format. Could be either the id or name.
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one family id is found.
     */
    public MyResourceId getId(String familyStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(familyStr)) {
            throw new CatalogException("Missing family parameter");
        }

        String userId;
        long studyId;
        long familyId;

        if (StringUtils.isNumeric(familyStr) && Long.parseLong(familyStr) > configuration.getCatalog().getOffset()) {
            familyId = Long.parseLong(familyStr);
            familyDBAdaptor.exists(familyId);
            studyId = familyDBAdaptor.getStudyId(familyId);
            userId = catalogManager.getUserManager().getUserId(sessionId);
        } else {
            if (familyStr.contains(",")) {
                throw new CatalogException("More than one family found");
            }

            userId = catalogManager.getUserManager().getUserId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FamilyDBAdaptor.QueryParams.NAME.key(), familyStr);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.ID.key());
            QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, queryOptions);
            if (familyQueryResult.getNumResults() == 1) {
                familyId = familyQueryResult.first().getId();
            } else {
                if (familyQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Family " + familyStr + " not found in study " + studyStr);
                } else {
                    throw new CatalogException("More than one family found under " + familyStr + " in study " + studyStr);
                }
            }
        }

        return new MyResourceId(userId, studyId, familyId);
    }

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param familyList List of family id in string format. Could be either the id or name.
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId  Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    @Override
    MyResourceIds getIds(List<String> familyList, @Nullable String studyStr, boolean silent, String sessionId) throws CatalogException {
        if (familyList == null || familyList.isEmpty()) {
            throw new CatalogException("Missing family parameter");
        }

        String userId;
        long studyId;
        List<Long> familyIds = new ArrayList<>();

        if (familyList.size() == 1 && StringUtils.isNumeric(familyList.get(0))
                && Long.parseLong(familyList.get(0)) > configuration.getCatalog().getOffset()) {
            familyIds.add(Long.parseLong(familyList.get(0)));
            familyDBAdaptor.checkId(familyIds.get(0));
            studyId = familyDBAdaptor.getStudyId(familyIds.get(0));
            userId = userManager.getUserId(sessionId);
        } else {
            userId = userManager.getUserId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Map<String, Long> myIds = new HashMap<>();
            for (String familiestrAux : familyList) {
                if (StringUtils.isNumeric(familiestrAux)) {
                    long familyId = getFamilyId(silent, familiestrAux);
                    myIds.put(familiestrAux, familyId);
                }
            }

            if (myIds.size() < familyList.size()) {
                Query query = new Query()
                        .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FamilyDBAdaptor.QueryParams.NAME.key(), familyList);

                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.NAME.key()));
                QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, queryOptions);

                if (familyQueryResult.getNumResults() > 0) {
                    myIds.putAll(familyQueryResult.getResult().stream().collect(Collectors.toMap(Family::getName, Family::getId)));
                }
            }
            if (myIds.size() < familyList.size() && !silent) {
                throw new CatalogException("Found only " + myIds.size() + " out of the " + familyList.size()
                        + " families looked for in study " + studyStr);
            }
            for (String familiestrAux : familyList) {
                familyIds.add(myIds.getOrDefault(familiestrAux, -1L));
            }
        }

        return new MyResourceIds(userId, studyId, familyIds);
    }

    private long getFamilyId(boolean silent, String familyStrAux) throws CatalogException {
        long familyId = Long.parseLong(familyStrAux);
        try {
            familyDBAdaptor.checkId(familyId);
        } catch (CatalogException e) {
            if (silent) {
                return -1L;
            } else {
                throw e;
            }
        }
        return familyId;
    }

    @Override
    public Long getStudyId(long entryId) throws CatalogException {
        return familyDBAdaptor.getStudyId(entryId);
    }

    @Override
    public DBIterator<Family> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    public QueryResult<Family> create(String studyStr, Family family, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_FAMILIES);

        ParamUtils.checkObj(family, "family");
        ParamUtils.checkAlias(family.getName(), "name", configuration.getCatalog().getOffset());
        family.setMembers(ParamUtils.defaultObject(family.getMembers(), Collections.emptyList()));
        family.setPhenotypes(ParamUtils.defaultObject(family.getPhenotypes(), Collections.emptyList()));
        family.setCreationDate(TimeUtils.getTime());
        family.setDescription(ParamUtils.defaultString(family.getDescription(), ""));
        family.setStatus(new Family.FamilyStatus());
        family.setAnnotationSets(ParamUtils.defaultObject(family.getAnnotationSets(), Collections.emptyList()));
        family.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));
        family.setVersion(1);
        family.setAttributes(ParamUtils.defaultObject(family.getAttributes(), Collections.emptyMap()));

        List<VariableSet> variableSetList = validateNewAnnotationSetsAndExtractVariableSets(studyId, family.getAnnotationSets());

        autoCompleteFamilyMembers(family, studyId, sessionId);
        validateFamily(family);
        validateMultiples(family);
        validatePhenotypes(family);
        createMissingMembers(family, studyId, sessionId);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        QueryResult<Family> queryResult = familyDBAdaptor.insert(studyId, family, variableSetList, options);
        auditManager.recordCreation(AuditRecord.Resource.family, queryResult.first().getId(), userId, queryResult.first(), null, null);

        addMemberInformation(queryResult, studyId, sessionId);
        return queryResult;
    }

    @Override
    public QueryResult<Family> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, query);
        fixQueryOptionAnnotation(options);

        query.append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, options, userId);
        addMemberInformation(familyQueryResult, studyId, sessionId);

        if (familyQueryResult.getNumResults() == 0 && query.containsKey("id")) {
            List<Long> idList = query.getAsLongList("id");
            for (Long myId : idList) {
                authorizationManager.checkFamilyPermission(studyId, myId, userId, FamilyAclEntry.FamilyPermissions.VIEW);
            }
        }

        return familyQueryResult;
    }

    public QueryResult<Family> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, query);
        fixQueryOptionAnnotation(options);

        fixQueryObject(query, studyId, sessionId);

        query.append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<Family> queryResult = familyDBAdaptor.get(query, options, userId);
        addMemberInformation(queryResult, studyId, sessionId);

        return queryResult;
    }

    private void fixQueryObject(Query query, long studyId, String sessionId) throws CatalogException {
        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        // We change the FATHER, MOTHER and MEMBER parameters for FATHER_ID, MOTHER_ID and MEMBER_ID which is what the DBAdaptor
        // understands
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.FATHER.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getAsStringList(FamilyDBAdaptor.QueryParams.FATHER.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.FATHER_ID.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.FATHER.key());
        }
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MOTHER.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getAsStringList(FamilyDBAdaptor.QueryParams.MOTHER.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.MOTHER_ID.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.MOTHER.key());
        }
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MEMBER.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getAsStringList(FamilyDBAdaptor.QueryParams.MEMBER.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.MEMBER_ID.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.MEMBER.key());
        }
    }

    public QueryResult<Family> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, query);

        fixQueryObject(query, studyId, sessionId);

        query.append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = familyDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_FAMILIES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public List<QueryResult<Family>> delete(@Nullable String studyStr, String entries, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
        return null;
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId) throws
            CatalogException {
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

        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        fixQueryObject(query, studyId, sessionId);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, userId, query, true);
        fixQueryOptionAnnotation(options);

        // Add study id to the query
        query.put(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult queryResult = familyDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult<Family> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Missing parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceId resource = getId(entryStr, studyStr, sessionId);
        long familyId = resource.getResourceId();

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (parameters.containsKey(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            authorizationManager.checkFamilyPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                    FamilyAclEntry.FamilyPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkFamilyPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                    FamilyAclEntry.FamilyPermissions.UPDATE);
        }

        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(familyId, new QueryOptions());
        addMemberInformation(familyQueryResult, resource.getStudyId(), sessionId);
        if (familyQueryResult.getNumResults() == 0) {
            throw new CatalogException("Family " + familyId + " not found");
        }

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> FamilyDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        // In case the user is updating members or phenotype list, we will create the family variable. If it is != null, it will mean that
        // all or some of those parameters have been passed to be updated, and we will need to call the private validator to check if the
        // fields are valid.
        Family family = null;

        if (parameters.containsKey(FamilyDBAdaptor.QueryParams.NAME.key())) {
            ParamUtils.checkAlias(parameters.getString(FamilyDBAdaptor.QueryParams.NAME.key()), "name",
                    configuration.getCatalog().getOffset());
        }
        if (parameters.containsKey(FamilyDBAdaptor.QueryParams.PHENOTYPES.key())
                || parameters.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
            // We parse the parameters to a family object
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

                family = objectMapper.readValue(objectMapper.writeValueAsString(parameters), Family.class);
            } catch (IOException e) {
                logger.error("{}", e.getMessage(), e);
                throw new CatalogException(e);
            }
        }

        if (family != null) {
            // MEMBERS or PHENOTYPES have been passed. We will complete the family object with the stored parameters that are not expected
            // to be updated
            if (family.getMembers() == null || family.getMembers().isEmpty()) {
                family.setMembers(familyQueryResult.first().getMembers());
            } else {
                // We will need to complete the individual information provided
                autoCompleteFamilyMembers(family, resource.getStudyId(), sessionId);
            }
            if (family.getPhenotypes() == null || family.getMembers().isEmpty()) {
                family.setPhenotypes(familyQueryResult.first().getPhenotypes());
            }

            validateFamily(family);
            validateMultiples(family);
            validatePhenotypes(family);

            ObjectMap tmpParams;
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                tmpParams = new ObjectMap(objectMapper.writeValueAsString(family));
            } catch (JsonProcessingException e) {
                logger.error("{}", e.getMessage(), e);
                throw new CatalogException(e);
            }

            if (parameters.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
                parameters.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));
            }
            if (parameters.containsKey(FamilyDBAdaptor.QueryParams.PHENOTYPES.key())) {
                parameters.put(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.PHENOTYPES.key()));
            }
        }

        List<VariableSet> variableSetList = checkUpdateAnnotationsAndExtractVariableSets(resource, parameters, familyDBAdaptor);

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(resource.getStudyId()));
        }

        QueryResult<Family> queryResult = familyDBAdaptor.update(familyId, parameters, variableSetList, options);
        auditManager.recordUpdate(AuditRecord.Resource.family, familyId, resource.getUser(), parameters, null, null);

        addMemberInformation(queryResult, resource.getStudyId(), sessionId);
        return queryResult;
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<FamilyAclEntry>> getAcls(String studyStr, List<String> familyList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        MyResourceIds resource = getIds(familyList, studyStr, silent, sessionId);

        List<QueryResult<FamilyAclEntry>> familyAclList = new ArrayList<>(resource.getResourceIds().size());
        List<Long> resourceIds = resource.getResourceIds();
        for (int i = 0; i < resourceIds.size(); i++) {
            Long familyId = resourceIds.get(i);
            try {
                QueryResult<FamilyAclEntry> allFamilyAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allFamilyAcls = authorizationManager.getFamilyAcl(resource.getStudyId(), familyId, resource.getUser(), member);
                } else {
                    allFamilyAcls = authorizationManager.getAllFamilyAcls(resource.getStudyId(), familyId, resource.getUser());
                }
                allFamilyAcls.setId(String.valueOf(familyId));
                familyAclList.add(allFamilyAcls);
            } catch (CatalogException e) {
                if (silent) {
                    familyAclList.add(new QueryResult<>(familyList.get(i), 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return familyAclList;
    }

    public List<QueryResult<FamilyAclEntry>> updateAcl(String studyStr, List<String> familyList, String memberIds,
                                                       AclParams familyAclParams, String sessionId) throws CatalogException {
        if (familyList == null || familyList.isEmpty()) {
            throw new CatalogException("Update ACL: Missing family parameter");
        }

        if (familyAclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(familyAclParams.getPermissions())) {
            permissions = Arrays.asList(familyAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, FamilyAclEntry.FamilyPermissions::valueOf);
        }

        MyResourceIds resourceIds = getIds(familyList, studyStr, sessionId);
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
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        switch (familyAclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.FAMILY);
            case ADD:
                return authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.FAMILY);
            case REMOVE:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, Entity.FAMILY);
            case RESET:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, Entity.FAMILY);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

    /**
     * Looks for all the members in the database. If they exist, the data will be overriden. It also fetches the parents individuals if they
     * haven't been provided.
     *
     * @param family    family object.
     * @param studyId   study id.
     * @param sessionId session id.
     * @throws CatalogException if there is any kind of error.
     */
    private void autoCompleteFamilyMembers(Family family, long studyId, String sessionId) throws CatalogException {
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            return;
        }

        Map<String, Individual> memberMap = new HashMap<>();
        Set<String> individualNames = new HashSet<>();
        for (Individual individual : family.getMembers()) {
            memberMap.put(individual.getName(), individual);
            individualNames.add(individual.getName());

            if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getName())) {
                individualNames.add(individual.getFather().getName());
            }
            if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getName())) {
                individualNames.add(individual.getMother().getName());
            }
        }

        Query query = new Query(IndividualDBAdaptor.QueryParams.NAME.key(), individualNames);
        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(String.valueOf(studyId), query,
                new QueryOptions(), sessionId);
        for (Individual individual : individualQueryResult.getResult()) {
            // We override the individuals from the map
            memberMap.put(individual.getName(), individual);
        }

        family.setMembers(memberMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList()));
    }

    private void validateFamily(Family family) throws CatalogException {
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            return;
        }

        Map<String, Individual> membersMap = new HashMap<>();       // individualName|individualId: Individual
        Map<String, List<Individual>> parentsMap = new HashMap<>(); // motherName||F---fatherName||M: List<children>
        Set<Individual> noParentsSet = new HashSet<>();             // Set with individuals without parents

        // 1. Fill in the objects initialised above
        for (Individual individual : family.getMembers()) {
            membersMap.put(individual.getName(), individual);
            if (individual.getId() > 0) {
                membersMap.put(String.valueOf(individual.getId()), individual);
            }

            String parentsKey = null;
            if (individual.getMother() != null) {
                if (individual.getMother().getId() > 0) {
                    individual.getMother().setName(String.valueOf(individual.getMother().getId()));
                }
                if (!StringUtils.isEmpty(individual.getMother().getName())) {
                    parentsKey = individual.getMother().getName() + "||F";
                }
            }
            if (individual.getFather() != null) {
                if (parentsKey != null) {
                    parentsKey += "---";
                }
                if (individual.getFather().getId() > 0) {
                    individual.getFather().setName(String.valueOf(individual.getFather().getId()));
                }
                if (!StringUtils.isEmpty(individual.getFather().getName())) {
                    if (parentsKey != null) {
                        parentsKey += individual.getFather().getName() + "||M";
                    } else {
                        parentsKey = individual.getFather().getName() + "||M";
                    }
                }
            }
            if (parentsKey == null) {
                noParentsSet.add(individual);
            } else {
                if (!parentsMap.containsKey(parentsKey)) {
                    parentsMap.put(parentsKey, new ArrayList<>());
                }
                parentsMap.get(parentsKey).add(individual);
            }
        }

        // 2. Loop over the parentsMap object. We will be emptying the noParentsSet as soon as we find a parent in the set. Once,
        // everything finishes, that set should be empty. Otherwise, it will mean that parent is not in use
        // On the other hand, all the parents should exist in the membersMap, otherwise it will mean that is missing in the family
        for (Map.Entry<String, List<Individual>> parentListEntry : parentsMap.entrySet()) {
            String[] split = parentListEntry.getKey().split("---");
            for (String parentName : split) {
                String[] splitNameSex = parentName.split("\\|\\|");
                String name = splitNameSex[0];
                Individual.Sex sex = splitNameSex[1].equals("F") ? Individual.Sex.FEMALE : Individual.Sex.MALE;

                if (!membersMap.containsKey(name)) {
                    throw new CatalogException("The parent " + name + " is not present in the members list");
                } else {
                    // Check if the sex is correct
                    Individual.Sex sex1 = membersMap.get(name).getSex();
                    if (sex1 != null && sex1 != sex && sex1 != Individual.Sex.UNKNOWN) {
                        throw new CatalogException("Sex of parent " + name + " is incorrect or the relationship is incorrect. In "
                                + "principle, it should be " + sex);
                    }
                    membersMap.get(name).setSex(sex);

                    // We attempt to remove the individual from the noParentsSet
                    noParentsSet.remove(membersMap.get(name));
                }
            }
        }

        if (noParentsSet.size() > 0) {
            throw new CatalogException("Some members that are not related to any other have been found: "
                    + noParentsSet.stream().map(Individual::getName).collect(Collectors.joining(", ")));
        }
    }

    private void validateMultiples(Family family) throws CatalogException {
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            return;
        }

        Map<String, List<String>> multiples = new HashMap<>();
        // Look for all the multiples
        for (Individual individual : family.getMembers()) {
            if (individual.getMultiples() != null && individual.getMultiples().getSiblings() != null
                    && !individual.getMultiples().getSiblings().isEmpty()) {
                multiples.put(individual.getName(), individual.getMultiples().getSiblings());
            }
        }

        if (multiples.size() > 0) {
            // Check if they are all cross-referenced
            for (Map.Entry<String, List<String>> entry : multiples.entrySet()) {
                for (String sibling : entry.getValue()) {
                    if (!multiples.containsKey(sibling)) {
                        throw new CatalogException("Missing sibling " + sibling + " of member " + entry.getKey());
                    }
                    if (!multiples.get(sibling).contains(entry.getKey())) {
                        throw new CatalogException("Incomplete sibling information. Sibling " + sibling + " does not contain "
                                + entry.getKey() + " as its sibling");
                    }
                }
            }
        }
    }

    private void validatePhenotypes(Family family) throws CatalogException {
        if (family.getPhenotypes() == null || family.getPhenotypes().isEmpty()) {
            return;
        }

        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            throw new CatalogException("Missing family members");
        }

        Set<String> memberPhenotypes = new HashSet<>();
        for (Individual individual : family.getMembers()) {
            if (individual.getPhenotypes() != null && !individual.getPhenotypes().isEmpty()) {
                memberPhenotypes.addAll(individual.getPhenotypes().stream().map(OntologyTerm::getId).collect(Collectors.toSet()));
            }
        }
        Set<String> familyPhenotypes = family.getPhenotypes().stream().map(OntologyTerm::getId).collect(Collectors.toSet());
        if (!familyPhenotypes.containsAll(memberPhenotypes)) {
            throw new CatalogException("Some of the phenotypes are not present in any member of the family");
        }
    }

    private void createMissingMembers(Family family, long studyId, String sessionId) throws CatalogException {
        if (family.getMembers() == null) {
            return;
        }

        // First, we will need to fix all the relationships. This means, that all children will be pointing to the latest parent individual
        // information available before it is created ! On the other hand, individuals will be created from the top to the bottom of the
        // family. Otherwise, references to parents might be lost.

        // We will assume that before calling to this method, the autoCompleteFamilyMembers method would have been called.
        // In that case, only individuals with ids <= 0 will have to be created

        // We initialize the individual map containing all the individuals
        Map<String, Individual> individualMap = new HashMap<>();
        List<Individual> individualsToCreate = new ArrayList<>();
        for (Individual individual : family.getMembers()) {
            individualMap.put(individual.getName(), individual);
            if (individual.getId() <= 0) {
                individualsToCreate.add(individual);
            }
        }

        // We link father and mother to individual objects
        for (Map.Entry<String, Individual> entry : individualMap.entrySet()) {
            if (entry.getValue().getFather() != null && StringUtils.isNotEmpty(entry.getValue().getFather().getName())) {
                entry.getValue().setFather(individualMap.get(entry.getValue().getFather().getName()));
            }
            if (entry.getValue().getMother() != null && StringUtils.isNotEmpty(entry.getValue().getMother().getName())) {
                entry.getValue().setMother(individualMap.get(entry.getValue().getMother().getName()));
            }
        }

        // We start creating missing individuals
        for (Individual individual : individualsToCreate) {
            createMissingIndividual(individual, individualMap, String.valueOf(studyId), sessionId);
        }
    }

    private void createMissingIndividual(Individual individual, Map<String, Individual> individualMap, String studyId, String sessionId)
            throws CatalogException {
        if (individual == null || individual.getId() > 0) {
            return;
        }
        if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getName())) {
            createMissingIndividual(individual.getFather(), individualMap, studyId, sessionId);
            individual.setFather(individualMap.get(individual.getFather().getName()));
        }
        if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getName())) {
            createMissingIndividual(individual.getMother(), individualMap, studyId, sessionId);
            individual.setMother(individualMap.get(individual.getMother().getName()));
        }
        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().create(String.valueOf(studyId), individual,
                QueryOptions.empty(), sessionId);
        if (individualQueryResult.getNumResults() == 0) {
            throw new CatalogException("Unexpected error when trying to create individual " + individual.getName());
        }
        individualMap.put(individual.getName(), individualQueryResult.first());
    }

//    /**
//     * Auxiliar method to get either the id of an individual or the name to be used as a unique identifier of the individual.
//     *
//     * @param individual individual.
//     * @return the id or name.
//     */
//    private String getIndividualIdOrName(Individual individual) {
//        return individual.getId() > 0 ? String.valueOf(individual.getId()) : individual.getName();
//    }

    private void addMemberInformation(QueryResult<Family> queryResult, long studyId, String sessionId) {
        if (queryResult.getNumResults() == 0) {
            return;
        }

        List<String> errorMessages = new ArrayList<>();
        for (Family family : queryResult.getResult()) {
            if (family.getMembers() == null || family.getMembers().isEmpty()) {
                continue;
            }

            List<Individual> memberList = new ArrayList<>();
            for (Individual member : family.getMembers()) {
                Query query = new Query()
                        .append(IndividualDBAdaptor.QueryParams.ID.key(), member.getId())
                        .append(IndividualDBAdaptor.QueryParams.VERSION.key(), member.getVersion());
                try {
                    QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(String.valueOf(studyId),
                            query, QueryOptions.empty(), sessionId);
                    if (individualQueryResult.getNumResults() == 0) {
                        throw new CatalogException("Could not get information from member " + member.getId());
                    } else {
                        memberList.add(individualQueryResult.first());
                    }
                } catch (CatalogException e) {
                    logger.warn("Could not retrieve member information to complete family {}, {}", family.getName(), e.getMessage(), e);
                    errorMessages.add("Could not retrieve member information to complete family " + family.getName() + ", "
                            + e.getMessage());
                }
            }
            family.setMembers(memberList);
        }

        if (errorMessages.size() > 0) {
            queryResult.setWarningMsg(StringUtils.join(errorMessages, "\n"));
        }
    }

}
