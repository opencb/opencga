package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AdminManager extends AbstractManager {

    private final CatalogIOManager catalogIOManager;
    protected static Logger logger = LoggerFactory.getLogger(AdminManager.class);

    AdminManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager, Configuration configuration)
            throws CatalogException {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.catalogIOManager = catalogIOManager;
    }

    public OpenCGAResult<User> userSearch(String organizationId, Query query, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("query", query)
                .append("options", options)
                .append("token", token);
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        try {
            authorizationManager.checkIsOpencgaAdministrator(jwtPayload);
        } catch (CatalogException e) {
            auditManager.auditSearch(organizationId, jwtPayload.getUserId(organizationId), Enums.Resource.USER, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        // Fix query object
        if (query.containsKey(ParamConstants.USER)) {
            query.put(UserDBAdaptor.QueryParams.ID.key(), query.get(ParamConstants.USER));
            query.remove(ParamConstants.USER);
        }
        if (query.containsKey(ParamConstants.USER_CREATION_DATE)) {
            query.put(UserDBAdaptor.QueryParams.INTERNAL_ACCOUNT_CREATION_DATE.key(), query.get(ParamConstants.USER_CREATION_DATE));
            query.remove(ParamConstants.USER_CREATION_DATE);
        }

        return catalogManager.getUserManager().search(organizationId, query, options, token);
    }

    public OpenCGAResult<Group> updateGroups(String organizationId, String userId, List<String> studyIds, List<String> groupIds,
                                             ParamUtils.AddRemoveAction action, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("userId", userId)
                .append("studyIds", studyIds)
                .append("groupIds", groupIds)
                .append("action", action)
                .append("token", token);
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        try {
            authorizationManager.checkIsOpencgaAdministrator(jwtPayload);

            // Check userId exists
            Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userId);
            OpenCGAResult<Long> count = getUserDBAdaptor(organizationId).count(query);
            if (count.getNumMatches() == 0) {
                throw new CatalogException("User '" + userId + "' not found.");
            }

            boolean membersPassed = groupIds.stream().anyMatch(g -> g.startsWith("@")
                    ? ParamConstants.MEMBERS_GROUP.equals(g)
                    : ParamConstants.MEMBERS_GROUP.equals("@" + g));
            if (action.equals(ParamUtils.AddRemoveAction.REMOVE) && membersPassed) {
                // TODO: If @members is added for REMOVAL, we should also automatically remove all permissions.
                throw new CatalogException("Operation not supported for '@members' group.");
            }

            // Check studyIds exist
            List<Study> studies = catalogManager.getStudyManager().resolveIds(studyIds, jwtPayload.getUserId(organizationId),
                    organizationId);
            List<Long> studyUids = new ArrayList<>(studies.size());
            for (Study study : studies) {
                if (ParamConstants.ADMIN_STUDY_FQN.equals(study.getFqn())) {
                    if (CollectionUtils.isNotEmpty(studyIds)) {
                        // Only fail if the user is passing the list of study ids
                        throw new CatalogException("Cannot perform this operation on administration study '" + study.getFqn() + "'.");
                    }
                } else {
                    studyUids.add(study.getUid());
                }
            }

            OpenCGAResult<Group> result = getStudyDBAdaptor(organizationId).updateUserFromGroups(userId, studyUids, groupIds, action);

            auditManager.audit(organizationId, jwtPayload.getUserId(organizationId), Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP,
                    Enums.Resource.STUDY, "", "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, jwtPayload.getUserId(organizationId), Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP,
                    Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public List<String> getOrganizationIds(String token) throws CatalogException {
        JwtPayload payload = catalogManager.getUserManager().validateToken(token);
        try {
            authorizationManager.checkIsOpencgaAdministrator(payload);
            List<String> organizationIds = getCatalogDBAdaptorFactory().getOrganizationIds();

            auditManager.audit(ParamConstants.ADMIN_ORGANIZATION, payload.getUserId(), Enums.Action.FETCH_ORGANIZATION_IDS,
                    Enums.Resource.STUDY, "", "", "", "", new ObjectMap(), new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return organizationIds;
        } catch (CatalogException e) {
            auditManager.audit(ParamConstants.ADMIN_ORGANIZATION, payload.getUserId(), Enums.Action.FETCH_ORGANIZATION_IDS,
                    Enums.Resource.STUDY, "", "", "", "", new ObjectMap(),
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

    }

    /**
     * Add a user to all the remote groups he/she may belong for a particular authentication origin.
     * Also remove the user from other groups he/she may have been associated in the past for the same authentication origin.
     *
     * @param organizationId         Organization id.
     * @param userId                 User id.
     * @param remoteGroupIds         List of group ids the user must be associated with.
     * @param authenticationOriginId The authentication origin the groups must be associated to.
     * @param token                  Administrator token.
     * @return An empty OpenCGAResult.
     * @throws CatalogException If the token is invalid or belongs to a user other thant the Administrator and if userId does not exist.
     */
    public OpenCGAResult<Group> syncRemoteGroups(String organizationId, String userId, List<String> remoteGroupIds,
                                                 String authenticationOriginId, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("userId", userId)
                .append("remoteGroupIds", remoteGroupIds)
                .append("authenticationOriginId", authenticationOriginId)
                .append("token", token);
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        try {
            authorizationManager.checkIsOpencgaAdministrator(jwtPayload);

            // Check userId exists
            Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userId);
            OpenCGAResult<Long> count = getUserDBAdaptor(organizationId).count(query);
            if (count.getNumMatches() == 0) {
                throw new CatalogException("User '" + userId + "' not found.");
            }
            OpenCGAResult<Group> result = getStudyDBAdaptor(organizationId).resyncUserWithSyncedGroups(userId, remoteGroupIds,
                    authenticationOriginId);

            auditManager.audit(organizationId, jwtPayload.getUserId(organizationId), Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP,
                    Enums.Resource.STUDY, "", "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, jwtPayload.getUserId(organizationId), Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP,
                    Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Acl> getEffectivePermissions(String studyStr, List<String> entryIdList,  String category, String token)
            throws CatalogException {
        return getEffectivePermissions(studyStr, entryIdList, Collections.emptyList(), category, token);
    }

    public OpenCGAResult<Acl> getEffectivePermissions(String studyStr, List<String> entryIdList, List<String> permissionList,
                                                      String category, String token) throws CatalogException {
        StopWatch stopWatch = StopWatch.createStarted();
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("entryIdList", entryIdList)
                .append("permissionList", permissionList)
                .append("category", category)
                .append("token", token);
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, jwtPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, jwtPayload);
            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);

            ParamUtils.checkParameter(category, "category");
            ParamUtils.checkNotEmptyArray(entryIdList, "entry id list");

            Enums.Resource resource;
            try {
                resource = Enums.Resource.valueOf(category.toUpperCase());
            } catch (IllegalArgumentException e) {
                String allowedResources = Arrays.stream(Enums.Resource.values()).map(Enum::name).collect(Collectors.joining(", "));
                throw new CatalogParameterException("Unexpected category '" + category + "' passed. Allowed categories are: "
                        + allowedResources);
            }
            List<Acl> effectivePermissions = authorizationManager.getEffectivePermissions(organizationId, study.getUid(), entryIdList,
                    permissionList, resource);
            auditManager.audit(organizationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), effectivePermissions);
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }
}
