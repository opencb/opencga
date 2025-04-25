package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.NotificationDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.notification.*;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserStatus;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.*;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class NotificationManager extends AbstractManager {

    public static final String ORGANIZATION_MEMBERS = "ORGANIZATION_MEMBERS";
    public static final String ORGANIZATION_ADMINISTRATORS = "ORGANIZATION_ADMINISTRATORS";
    public static final String PROJECT_ADMINISTRATORS = "PROJECT_ADMINISTRATORS";
    public static final String STUDY_ADMINISTRATORS = "STUDY_ADMINISTRATORS";
    public static final String STUDY_MEMBERS = "STUDY_MEMBERS";

    public static final Set<String> NOTIFICATION_GROUPS = new HashSet<>(Arrays.asList(
            ORGANIZATION_MEMBERS,
            ORGANIZATION_ADMINISTRATORS,
            PROJECT_ADMINISTRATORS,
            STUDY_ADMINISTRATORS,
            STUDY_MEMBERS
    ));

    NotificationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                        DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
    }

    Enums.Resource getEntity() {
        return Enums.Resource.NOTIFICATION;
    }

//    InternalGetDataResult<Notification> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
//                                                    QueryOptions options, String user, boolean ignoreException) throws CatalogException {
//        if (ListUtils.isEmpty(entryList)) {
//            throw new CatalogException("Missing notification entries.");
//        }
//        List<String> uniqueList = ListUtils.unique(entryList);
//
//        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
//
//        Query queryCopy = query == null ? new Query() : new Query(query);
//        queryCopy.put(NotificationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
//        queryCopy.put(NotificationDBAdaptor.QueryParams.UUID.key(), uniqueList);
//
//        // Ensure the field by which we are querying for will be kept in the results
//        queryOptions = keepFieldInQueryOptions(queryOptions, NotificationDBAdaptor.QueryParams.UUID.key());
//
//        OpenCGAResult<Notification> result = getNotificationDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);
//
//        Function<Notification, String> sampleStringFunction = Notification::getUuid;
//
//        if (ignoreException || result.getNumResults() >= uniqueList.size()) {
//            return keepOriginalOrder(uniqueList, sampleStringFunction, result, ignoreException, false);
//        }
//        // Query without adding the user check
//        OpenCGAResult<Notification> resultsNoCheck = getNotificationDBAdaptor(organizationId).get(queryCopy, queryOptions);
//
//        if (resultsNoCheck.getNumResults() == result.getNumResults()) {
//            throw CatalogException.notFound("notifications", getMissingFields(uniqueList, result.getResults(), sampleStringFunction));
//        } else {
//            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the"
//                    + " notifications.");
//        }
//    }

    public OpenCGAResult<Notification> create(NotificationCreateParams notification, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);

        ParamUtils.checkObj(notification, "notification");
        ParamUtils.checkObj(notification.getScope(), "scope");
        ParamUtils.checkParameter(notification.getFqn(), "fqn");

        CatalogFqn fqn = CatalogFqn.extractFqnFromGenericFqn(notification.getFqn());
        String organizationId = fqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
        validateAndObtainNotification(notification, organizationId);
        List<Notification> notificationList = generateNotificationInstances(notification, organizationId, fqn, tokenPayload);

        return catalogDBAdaptorFactory.getNotificationDBAdaptor(organizationId).insert(notificationList, options);
    }

    public OpenCGAResult<Notification> get(String organizationIdStr, String notificationUuid, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = StringUtils.isNotEmpty(organizationIdStr) ? organizationIdStr : tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId(organizationId);
        Query query = new Query(NotificationDBAdaptor.QueryParams.UUID.key(), notificationUuid);
        return getNotificationDBAdaptor(organizationId).get(query, options, userId);
    }

    private List<Notification> generateNotificationInstances(NotificationCreateParams notificationCreateInstance, String organizationId,
                                                             CatalogFqn fqn, JwtPayload payload) throws CatalogException {
        List<Notification> notificationList = new LinkedList<>();

        List<String> userIds = new LinkedList<>();
        boolean validGroup = NOTIFICATION_GROUPS.contains(notificationCreateInstance.getTarget().toUpperCase());
        Project project = null;
        Study study = null;
        if (notificationCreateInstance.getScope().equals(NotificationScope.STUDY)) {
            study = catalogManager.getStudyManager().resolveId(fqn, payload);
        } else if (notificationCreateInstance.getScope().equals(NotificationScope.PROJECT)) {
            project = catalogManager.getProjectManager().resolveId(fqn, null, payload).first();
        }
        if (validGroup) {
            switch (notificationCreateInstance.getTarget().toUpperCase()) {
                case ORGANIZATION_MEMBERS:
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
                    break;
                case PROJECT_ADMINISTRATORS:
                    userIds.addAll(catalogManager.getProjectManager().getProjectAdmins(project));
                    break;
                case STUDY_ADMINISTRATORS:
                    ParamUtils.checkObj(study, "study");
                    // Get all study administrators
                    if (CollectionUtils.isEmpty(study.getGroups())) {
                        throw new CatalogParameterException("Internal error: Study " + study.getId() + " does not have any groups.");
                    }
                    for (Group group : study.getGroups()) {
                        if (StudyManager.ADMINS.equals(group.getId())) {
                            userIds.addAll(group.getUserIds());
                            break;
                        }
                    }
                    break;
                case STUDY_MEMBERS:
                    ParamUtils.checkObj(study, "study");
                    // Get all study members
                    if (CollectionUtils.isEmpty(study.getGroups())) {
                        throw new CatalogParameterException("Internal error: Study " + study.getId() + " does not have any groups.");
                    }
                    for (Group group : study.getGroups()) {
                        if (StudyManager.MEMBERS.equals(group.getId())) {
                            userIds.addAll(group.getUserIds());
                            break;
                        }
                    }
                    break;
                default:
                    throw new CatalogParameterException("Unexpected notification target group " + notificationCreateInstance.getTarget());
            }
        } else {
            if (notificationCreateInstance.getTarget().startsWith("@")) {
                // Obtain group
                OpenCGAResult<Group> group = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor(organizationId)
                        .getGroup(study.getUid(), notificationCreateInstance.getTarget(), null);
                if (group.getNumResults() == 0) {
                    throw new CatalogParameterException("Target group " + notificationCreateInstance.getTarget() + " not found.");
                }
                // Check group exists
                userIds.addAll(group.first().getUserIds());
            } else {
                // Check user exists
                try {
                    catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId).checkId(notificationCreateInstance.getTarget());
                    userIds.add(notificationCreateInstance.getTarget());
                } catch (CatalogException e) {
                    throw new CatalogParameterException("Target user " + notificationCreateInstance.getTarget() + " not found.", e);
                }
            }
        }

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTIFICATION);
        // Create all notification instances
        for (String targetUserId : userIds) {
            String uuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTIFICATION);
            NotificationInternal notificationInternal = new NotificationInternal(new NotificationStatus(NotificationStatus.PENDING,
                    "Notification yet to be processed."), TimeUtils.getTime(), TimeUtils.getTime(), Collections.emptyList());
            Notification notificationCopy = new Notification(notificationCreateInstance, uuid, operationId,
                    payload.getUserId(organizationId), targetUserId, notificationInternal);
            notificationList.add(notificationCopy);
        }

        return notificationList;
    }

    private void validateAndObtainNotification(NotificationCreateParams notification, String organizationId)
            throws CatalogParameterException {
        ParamUtils.checkParameter(notification.getTarget(), "target");
        ParamUtils.checkParameter(notification.getSubject(), "subject");
        ParamUtils.checkParameter(notification.getBody(), "body");
        ParamUtils.checkObj(notification.getScope(), "scope");
        ParamUtils.checkParameter(notification.getFqn(), "fqn");
        ParamUtils.checkObj(notification.getLevel(), "type");

        boolean validGroup = NOTIFICATION_GROUPS.contains(notification.getTarget().toUpperCase());
        if (validGroup) {
            switch (notification.getTarget().toUpperCase()) {
                case STUDY_MEMBERS:
                case STUDY_ADMINISTRATORS:
                    if (notification.getScope() != NotificationScope.STUDY) {
                        throw new CatalogParameterException("Target cannot be addressed to " + notification.getTarget()
                                + " if the scope of the notification is not " + NotificationScope.STUDY + ".");
                    }
                    break;
                case PROJECT_ADMINISTRATORS:
                    if (notification.getScope() != NotificationScope.PROJECT) {
                        throw new CatalogParameterException("Target cannot be addressed to " + notification.getTarget()
                                + " if the scope of the notification is not " + NotificationScope.PROJECT + ".");
                    }
                    break;
                default:
                    break;
            }
        } else {
            if (notification.getTarget().startsWith("@")) {
                if (notification.getScope() != NotificationScope.STUDY) {
                    throw new CatalogParameterException("Target cannot be addressed to a group if the scope of the notification is not "
                            + NotificationScope.STUDY + ".");
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
        if (notification.getScope() == NotificationScope.STUDY && !CatalogFqn.isValidStudyFqn(notification.getFqn())) {
            throw new CatalogParameterException("Expected valid study fqn for scope '" + NotificationScope.STUDY + "'. '"
                    + notification.getFqn() + "' is not a valid study fqn.");
        } else if (notification.getScope() == NotificationScope.PROJECT && !CatalogFqn.isValidProjectFqn(notification.getFqn())) {
            throw new CatalogParameterException("Expected valid project fqn for scope '" + NotificationScope.PROJECT + "'. '"
                    + notification.getFqn() + "' is not a valid project fqn.");
        } else if ((notification.getScope() == NotificationScope.ORGANIZATION || notification.getScope() == NotificationScope.GLOBAL)
                && notification.getFqn().contains("@")) {
            // Check if the fqn seems valid (organization id)
            throw new CatalogParameterException("Expected valid organization fqn (organization id) for scope '" + notification.getScope()
                    + "'. '" + notification.getFqn() + "' is not a valid organization fqn.");
        }

    }

    public OpenCGAResult<Notification> update(String organizationIdStr, String notificationUuid, NotificationUpdateParams updateParams,
                                              QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = StringUtils.isNotEmpty(organizationIdStr) ? organizationIdStr : tokenPayload.getOrganization();

        authorizationManager.checkIsOpencgaAdministrator(tokenPayload, "update notifications");
        ParamUtils.checkParameter(notificationUuid, "notificationUuid");
        ParamUtils.checkObj(updateParams, "updateParams");
        ObjectMap parameters;
        try {
            parameters = new ObjectMap(getUpdateObjectMapper().writeValueAsString(updateParams));
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse NotificationUpdateParams object: " + e.getMessage(), e);
        }

        OpenCGAResult<Notification> update = getNotificationDBAdaptor(organizationId).update(notificationUuid, parameters, options);
        checkUpdateResult(update);
        return update;
    }

    private void checkUpdateResult(OpenCGAResult<Notification> result) throws CatalogException {
        if (result.getNumMatches() == 0) {
            throw new CatalogException("Could not update " + getEntity() + ". " + getEntity() + " not found.");
        }
        if (result.getNumUpdated() == 0) {
            result.addEvent(new Event(Event.Type.WARNING, getEntity() + " was already updated."));
        }
    }

    public DBIterator<Notification> iterator(String organizationStr, Query query, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = extractOrganizationId(organizationStr, query, tokenPayload);
        String userId = tokenPayload.getUserId(organizationId);

        Query finalQuery = new Query(query);
        fixQueryObject(query, tokenPayload);

        return getNotificationDBAdaptor(organizationId).iterator(finalQuery, options, userId);
    }

    public OpenCGAResult<FacetField> facet(String organizationIdStr, Query query, String facet, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = extractOrganizationId(organizationIdStr, query, tokenPayload);
        String userId = tokenPayload.getUserId(organizationId);

        fixQueryObject(query, tokenPayload);
        return getNotificationDBAdaptor(organizationId).facet(query, facet, userId);
    }

    public OpenCGAResult<Notification> search(String organizationIdStr, Query query, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = extractOrganizationId(organizationIdStr, query, tokenPayload);
        String userId = tokenPayload.getUserId(organizationId);
        fixQueryObject(query, tokenPayload);
        return getNotificationDBAdaptor(organizationId).get(query, options, userId);
    }

    public OpenCGAResult<?> distinct(String organizationIdStr, List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = extractOrganizationId(organizationIdStr, query, tokenPayload);
        String userId = tokenPayload.getUserId(organizationId);

        fixQueryObject(query, tokenPayload);
        return getNotificationDBAdaptor(organizationId).distinct(fields, query, userId);
    }

    public OpenCGAResult<Notification> count(String organizationIdStr, Query query, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = extractOrganizationId(organizationIdStr, query, tokenPayload);
        String userId = tokenPayload.getUserId(organizationId);

        query = new Query(ParamUtils.defaultObject(query, Query::new));
        fixQueryObject(query, tokenPayload);
        OpenCGAResult<Long> queryResultAux = getNotificationDBAdaptor(organizationId).count(query, userId);

        return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                queryResultAux.getNumMatches());
    }

    public OpenCGAResult rank(String organizationIdStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(token, "token");

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = extractOrganizationId(organizationIdStr, query, tokenPayload);

        boolean count = true;
        fixQueryObject(query, tokenPayload);
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = getNotificationDBAdaptor(organizationId).rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public OpenCGAResult groupBy(String organizationIdStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = extractOrganizationId(organizationIdStr, query, tokenPayload);
        String userId = tokenPayload.getUserId(organizationId);

        fixQueryObject(query, tokenPayload);
        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryOptionAnnotation(options);

        OpenCGAResult queryResult = getNotificationDBAdaptor(organizationId).groupBy(query, fields, options, userId);
        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    private String extractOrganizationId(String organizationStr, Query query, JwtPayload payload) {
        if (StringUtils.isNotEmpty(organizationStr)) {
            return organizationStr;
        } else if (query.containsKey(NotificationDBAdaptor.QueryParams.FQN.key())) {
            String fqn = query.getString(NotificationDBAdaptor.QueryParams.FQN.key());
            return CatalogFqn.extractFqnFromGenericFqn(fqn).getOrganizationId();
        } else {
            return payload.getOrganization();
        }
    }

    protected void fixQueryObject(Query query, JwtPayload tokenPayload) throws CatalogException {
        super.fixQueryObject(query);
        if (!authorizationManager.isOpencgaAdministrator(tokenPayload)) {
            // Only OpenCGA administrators can see any notification
            query.put(NotificationDBAdaptor.QueryParams.RECEIVER.key(), tokenPayload.getUserId());
        }
    }
}
