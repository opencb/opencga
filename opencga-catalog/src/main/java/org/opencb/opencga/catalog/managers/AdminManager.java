package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AdminManager extends AbstractManager {

    private final CatalogIOManager catalogIOManager;
    protected static Logger logger = LoggerFactory.getLogger(AdminManager.class);

    AdminManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager, Configuration configuration)
            throws CatalogException {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.catalogIOManager = catalogIOManager;
    }

    public OpenCGAResult<User> userSearch(Query query, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("options", options)
                .append("token", token);
        String userId = catalogManager.getUserManager().getUserId(token);
        try {
            authorizationManager.checkIsInstallationAdministrator(userId);

            // Fix query object
            if (query.containsKey(ParamConstants.USER)) {
                query.put(UserDBAdaptor.QueryParams.ID.key(), query.get(ParamConstants.USER));
                query.remove(ParamConstants.USER);
            }
            if (query.containsKey(ParamConstants.USER_ACCOUNT_TYPE)) {
                query.put(UserDBAdaptor.QueryParams.ACCOUNT_TYPE.key(), query.get(ParamConstants.USER_ACCOUNT_TYPE));
                query.remove(ParamConstants.USER_ACCOUNT_TYPE);
            }
            if (query.containsKey(ParamConstants.USER_AUTHENTICATION_ORIGIN)) {
                query.put(UserDBAdaptor.QueryParams.ACCOUNT_AUTHENTICATION_ID.key(), query.get(ParamConstants.USER_AUTHENTICATION_ORIGIN));
                query.remove(ParamConstants.USER_AUTHENTICATION_ORIGIN);
            }
            if (query.containsKey(ParamConstants.USER_CREATION_DATE)) {
                query.put(UserDBAdaptor.QueryParams.ACCOUNT_CREATION_DATE.key(), query.get(ParamConstants.USER_CREATION_DATE));
                query.remove(ParamConstants.USER_CREATION_DATE);
            }

            OpenCGAResult<User> userDataResult = userDBAdaptor.get(query, options);
            auditManager.auditSearch(userId, Enums.Resource.USER, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return userDataResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.USER, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Group> updateGroups(String userId, List<String> studyIds, List<String> groupIds,
                                             ParamUtils.AddRemoveAction action, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("studyIds", studyIds)
                .append("groupIds", groupIds)
                .append("action", action)
                .append("token", token);
        String authenticatedUser = catalogManager.getUserManager().getUserId(token);
        try {
            authorizationManager.checkIsInstallationAdministrator(authenticatedUser);

            // Check userId exists
            Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userId);
            OpenCGAResult<Long> count = userDBAdaptor.count(query);
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
            List<Study> studies = catalogManager.getStudyManager().resolveIds(studyIds, authenticatedUser);
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

            OpenCGAResult<Group> result = studyDBAdaptor.updateUserFromGroups(userId, studyUids, groupIds, action);

            auditManager.audit(userId, Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP, Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP, Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Add a user to all the remote groups he/she may belong for a particular authentication origin.
     * Also remove the user from other groups he/she may have been associated in the past for the same authentication origin.
     *
     * @param userId User id.
     * @param remoteGroupIds List of group ids the user must be associated with.
     * @param authenticationOriginId The authentication origin the groups must be associated to.
     * @param token Administrator token.
     * @return An empty OpenCGAResult.
     * @throws CatalogException If the token is invalid or belongs to a user other thant the Administrator and if userId does not exist.
     */
    public OpenCGAResult<Group> syncRemoteGroups(String userId, List<String> remoteGroupIds,
                                             String authenticationOriginId, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("userId", userId)
                .append("remoteGroupIds", remoteGroupIds)
                .append("authenticationOriginId", authenticationOriginId)
                .append("token", token);
        String authenticatedUser = catalogManager.getUserManager().getUserId(token);
        try {
            authorizationManager.checkIsInstallationAdministrator(authenticatedUser);

            // Check userId exists
            Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userId);
            OpenCGAResult<Long> count = userDBAdaptor.count(query);
            if (count.getNumMatches() == 0) {
                throw new CatalogException("User '" + userId + "' not found.");
            }
            OpenCGAResult<Group> result = studyDBAdaptor.resyncUserWithSyncedGroups(userId, remoteGroupIds, authenticationOriginId);

            auditManager.audit(userId, Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP, Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.UPDATE_USERS_FROM_STUDY_GROUP, Enums.Resource.STUDY, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

}
