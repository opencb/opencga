package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.NotificationDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.notification.Notification;
import org.opencb.opencga.core.models.notification.NotificationInternal;
import org.opencb.opencga.core.models.notification.NotificationStatus;
import org.opencb.opencga.core.models.notification.NotificationUpdateParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserStatus;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class NotificationManager extends ResourceManager<Notification> {

    public static final String ORGANIZATION_ADMINISTRATORS = "ORGANIZATION_ADMINISTRATORS";
    public static final String PROJECT_ADMINISTRATORS = "PROJECT_ADMINISTRATORS";
    public static final String STUDY_ADMINISTRATORS = "STUDY_ADMINISTRATORS";
    public static final String STUDY_MEMBERS = "STUDY_MEMBERS";
    public static final String ANY_USER = "ANY_USER";

    public static final List<String> NOTIFICATION_GROUPS = Arrays.asList(ORGANIZATION_ADMINISTRATORS, PROJECT_ADMINISTRATORS,
            STUDY_ADMINISTRATORS, STUDY_MEMBERS, ANY_USER);

    NotificationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                        DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.NOTIFICATION;
    }

    @Override
    InternalGetDataResult<Notification> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
                                                    QueryOptions options, String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing notification entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        queryCopy.put(NotificationDBAdaptor.QueryParams.UUID.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, NotificationDBAdaptor.QueryParams.UUID.key());

        OpenCGAResult<Notification> result = getNotificationDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        Function<Notification, String> sampleStringFunction = Notification::getUuid;

        if (ignoreException || result.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, sampleStringFunction, result, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<Notification> resultsNoCheck = getNotificationDBAdaptor(organizationId).get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == result.getNumResults()) {
            throw CatalogException.notFound("notifications", getMissingFields(uniqueList, result.getResults(), sampleStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the"
                    + " notifications.");
        }
    }

    @Override
    public OpenCGAResult<Notification> create(String studyStr, Notification notification, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);

        authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
        validateNotificationParameters(notification, organizationId, study, userId);
        List<Notification> notificationList = generateNotificationInstances(notification, organizationId, study, userId);

        return catalogDBAdaptorFactory.getNotificationDBAdaptor(organizationId).insert(study.getUid(), notificationList, options);
    }

    private List<Notification> generateNotificationInstances(Notification notification, String organizationId, Study study, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Notification> notificationList = new LinkedList<>();

        List<String> userIds = new LinkedList<>();
        boolean validGroup = false;
        for (String notificationGroup : NOTIFICATION_GROUPS) {
            if (notification.getTarget().equalsIgnoreCase(notificationGroup)) {
                notification.setTarget(notificationGroup);
                validGroup = true;
                break;
            }
        }
        if (validGroup) {
            switch (notification.getTarget()) {
                case ANY_USER:
                    // Get all users
                    try (DBIterator<User> iterator = catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId)
                            .iterator(new Query(UserDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), UserStatus.READY),
                                    UserManager.INCLUDE_INTERNAL)) {
                        while (iterator.hasNext()) {
                            userIds.add(iterator.next().getId());
                        }
                    }
                    break;
                case ORGANIZATION_ADMINISTRATORS:
                    // Get all organization administrators
                    Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId)
                            .get(OrganizationManager.INCLUDE_ORGANIZATION_ADMINS).first();
                    userIds.add(organization.getOwner());
                    userIds.addAll(organization.getAdmins());
                case PROJECT_ADMINISTRATORS:
                    // Get all project administrators
                    // TODO: Implement and add break; in ORGANIZATION_ADMINISTRATORS case. Meanwhile, treat as ORG_ADMINS
                    break;
                case STUDY_ADMINISTRATORS:
                    // Get all study administrators
                    Group adminsGroup = getStudyDBAdaptor(organizationId).getGroup(study.getUid(), StudyManager.ADMINS, null).first();
                    userIds.addAll(adminsGroup.getUserIds());
                    break;
                case STUDY_MEMBERS:
                    // Get all study members
                    Group membersGroup = getStudyDBAdaptor(organizationId).getGroup(study.getUid(), StudyManager.MEMBERS, null).first();
                    userIds.addAll(membersGroup.getUserIds());
                    break;
                default:
                    throw new CatalogParameterException("Unexpected notification target group " + notification.getTarget());
            }
        } else {
            if (notification.getTarget().startsWith("@")) {
                // Check group exists
                Group group = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor(organizationId)
                        .getGroup(study.getUid(), notification.getTarget(), null).first();
                userIds.addAll(group.getUserIds());
            } else {
                // Check user exists
                try {
                    catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId).checkId(notification.getTarget());
                    userIds.add(notification.getTarget());
                } catch (CatalogException e) {
                    throw new CatalogParameterException("Target user " + notification.getTarget() + " not found.", e);
                }
            }
        }

        notification.setOperationId(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTIFICATION));
        // Create all notification instances
        for (String targetUserId : userIds) {
            Notification notificationCopy = new Notification(notification);
            notificationCopy.setSubject(StringUtils.isNotEmpty(notification.getSubject())
                    ? notification.getSubject()
                    : notification.getType());
            notificationCopy.setSender(userId);
            notificationCopy.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTIFICATION));
            NotificationInternal notificationInternal = new NotificationInternal(new NotificationStatus(NotificationStatus.PENDING,
                    "Notification yet to be processed."), TimeUtils.getTime(), TimeUtils.getTime(), Collections.emptyList());
            notificationCopy.setInternal(notificationInternal);
            notificationCopy.setReceiver(targetUserId);

            notificationList.add(notificationCopy);
        }

        return notificationList;
    }

    private void validateNotificationParameters(Notification notification, String organizationId, Study study, String userId)
            throws CatalogParameterException, CatalogDBException {
        ParamUtils.checkParameter(notification.getTarget(), "target");
        ParamUtils.checkParameter(notification.getBody(), "body");
        ParamUtils.checkParameter(notification.getType(), "type");

        boolean validGroup = false;
        for (String notificationGroup : NOTIFICATION_GROUPS) {
            if (notification.getTarget().equalsIgnoreCase(notificationGroup)) {
                validGroup = true;
            }
        }
        if (!validGroup) {
            if (notification.getTarget().startsWith("@")) {
                // Check group exists
                OpenCGAResult<Group> group = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor(organizationId)
                        .getGroup(study.getUid(), notification.getTarget(), null);
                if (group.getNumResults() == 0) {
                    throw new CatalogParameterException("Target group " + notification.getTarget() + " not found.");
                }
            } else {
                // Check user exists
                try {
                    catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId).checkId(notification.getTarget());
                } catch (CatalogException e) {
                    throw new CatalogParameterException("Target user " + notification.getTarget() + " not found.", e);
                }
            }
        }
    }

    public OpenCGAResult<Notification> update(String studyStr, String notificationUuid, NotificationUpdateParams updateParams,
                                              QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);

        authorizationManager.checkIsOpencgaAdministrator(tokenPayload, "update notifications");
        ParamUtils.checkParameter(notificationUuid, "notificationUuid");
        ParamUtils.checkObj(updateParams, "updateParams");
        ObjectMap parameters;
        try {
            parameters = new ObjectMap(getUpdateObjectMapper().writeValueAsString(updateParams));
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse NotificationUpdateParams object: " + e.getMessage(), e);
        }

        OpenCGAResult<Notification> update = getNotificationDBAdaptor(organizationId).update(study.getUid(), notificationUuid, parameters,
                options);
        checkUpdateResult(update);
        return update;
    }

    @Override
    public DBIterator<Notification> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Query finalQuery = new Query(query);
        fixQueryObject(finalQuery);
        finalQuery.append(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getNotificationDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);

        fixQueryObject(query);
        query.append(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getNotificationDBAdaptor(organizationId).facet(study.getUid(), query, facet, userId);
    }

    @Override
    public OpenCGAResult<Notification> search(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
        query.append(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        fixQueryObject(query);

        return getNotificationDBAdaptor(organizationId).get(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<?> distinct(String studyStr, List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);
        fixQueryObject(query);
        query.append(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        return getNotificationDBAdaptor(organizationId).distinct(study.getUid(), fields, query, userId);
    }

    @Override
    public OpenCGAResult<Notification> count(String studyStr, Query query, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);
        query = new Query(ParamUtils.defaultObject(query, Query::new));
        fixQueryObject(query);
        query.append(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        OpenCGAResult<Long> queryResultAux = getNotificationDBAdaptor(organizationId).count(query, userId);

        return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                queryResultAux.getNumMatches());
    }

    @Override
    public OpenCGAResult<Notification> delete(String studyStr, List<String> ids, QueryOptions options, String token)
            throws CatalogException {
        throw new NotImplementedException("Delete not implemented for notifications");
    }

    @Override
    public OpenCGAResult<Notification> delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        throw new NotImplementedException("Delete not implemented for notifications");
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(token, "token");

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);

        boolean count = true;
        query.append(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        query.append(NotificationDBAdaptor.QueryParams.RECEIVER.key(), userId);
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = getNotificationDBAdaptor(organizationId).rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, tokenPayload);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryOptionAnnotation(options);

        // Add study id to the query
        query.put(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = getNotificationDBAdaptor(organizationId).groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }
}
