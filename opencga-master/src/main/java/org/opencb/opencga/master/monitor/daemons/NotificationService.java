package org.opencb.opencga.master.monitor.daemons;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.MailUtils;
import org.opencb.opencga.core.common.WebhookUtils;
import org.opencb.opencga.core.models.notification.*;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserStatus;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class NotificationService extends MonitorParentDaemon implements Closeable {

    public static final String SUCCESS_DESCRIPTION = "Notification successfully processed.";
    public static final String ERROR_DESCRIPTION = "At least, one of the notification mechanisms failed.";

    private final MailUtils mailUtils;

    private Map<String, User> userMap;
    private final QueryOptions userOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            UserDBAdaptor.QueryParams.ID.key(), UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.INTERNAL.key(),
            UserDBAdaptor.QueryParams.NOTIFICATIONS.key()
    ));

    public NotificationService(int interval, String token, CatalogManager catalogManager) {
        super(interval, token, catalogManager);
        this.mailUtils = MailUtils.configure(catalogManager.getConfiguration().getEmail());
    }

    @Override
    public void apply() throws Exception {
        checkNotifications();
    }

    private void checkNotifications() throws CatalogException {
        userMap = new HashMap<>();
        List<String> organizationIds = catalogManager.getOrganizationManager().getOrganizationIds(token);

        for (String organizationId : organizationIds) {
            logger.info("----- NOTIFICATION SERVICE  ----- Checking notifications for organization '{}'", organizationId);
            Query query = new Query(ParamConstants.INTERNAL_STATUS_PARAM, NotificationStatus.PENDING);
            try (DBIterator<Notification> iterator = catalogManager.getNotificationManager()
                    .iterator(organizationId, query, QueryOptions.empty(), token)) {
                while (iterator.hasNext()) {
                    processNotification(organizationId, iterator.next());
                }
            }
        }
    }

    private void processNotification(String organizationId, Notification notification) {
        logger.info("----- NOTIFICATION SERVICE  ----- Processing notification '{}' for user '{}'", notification.getUuid(),
                notification.getTarget());
        User user = getUserConfiguration(organizationId, notification);
        if (user == null) {
            updateNotificationStatus(organizationId, notification.getUuid(), NotificationStatus.ERROR, ERROR_DESCRIPTION, null);
            return;
        }
        if (!user.getInternal().getStatus().getId().equals(UserStatus.READY)) {
            updateNotificationStatus(organizationId, notification.getUuid(), NotificationStatus.DISCARDED,
                    "User account status is " + user.getInternal().getStatus().getId(), null);
            return;
        }
        NotificationConfiguration notificationConfiguration = user.getNotifications();
        if (notificationConfiguration == null) {
            updateNotificationStatus(organizationId, notification.getUuid(), NotificationStatus.DISCARDED,
                    "User doesn't have the notifications configured", null);
            return;
        }

        List<NotificationInternalNotificationResult> notificationInternalList = new ArrayList<>();
        try {
            if (notificationIsExpected(notification, notificationConfiguration.getEmail(), NotificationInternalNotificationResult.EMAIL)) {
                if (StringUtils.isEmpty(user.getEmail())) {
                    throw new CatalogException("User does not have an email configured.");
                } else {
                    try {
                        mailUtils.sendMail(user.getEmail(), notification.getSubject(), notification.getContent());
                        notificationInternalList.add(
                                new NotificationInternalNotificationResult(NotificationInternalNotificationResult.EMAIL,
                                        NotificationInternalNotificationResult.SUCCESS));
                    } catch (Exception e) {
                        logger.error("Could not send email to user '{}': {}", user.getId(), e.getMessage(), e);
                        throw new CatalogException("Could not send email: " + e.getMessage());
                    }
                }
            }
        } catch (CatalogException e) {
            notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.EMAIL,
                    NotificationInternalNotificationResult.ERROR, e.getMessage()));
        }
        try {
            if (notificationIsExpected(notification, notificationConfiguration.getSlack(), NotificationInternalNotificationResult.SLACK)) {
                if (StringUtils.isEmpty(notificationConfiguration.getSlack().getWebhookUrl())) {
                    throw new CatalogException("User does not have a Slack webhook configured.");
                } else {
                    try {
                        WebhookUtils.sendWithRetry(notificationConfiguration.getSlack().getWebhookUrl(), "POST", notification.getSubject(),
                                notification.getContent(), 2);
                        notificationInternalList.add(
                                new NotificationInternalNotificationResult(NotificationInternalNotificationResult.SLACK,
                                        NotificationInternalNotificationResult.SUCCESS));
                    } catch (Exception e) {
                        logger.error("Could not send slack notification to user '{}': {}", user.getId(), e.getMessage(), e);
                        throw new CatalogException("Could not send slack notification: " + e.getMessage());
                    }
                }
            }
        } catch (CatalogException e) {
            notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.SLACK,
                    NotificationInternalNotificationResult.ERROR, e.getMessage()));
        }

        boolean success = notificationInternalList
                .stream()
                .allMatch((r) -> r.getStatus().equals(NotificationInternalNotificationResult.SUCCESS));
        String status = success ? NotificationStatus.SUCCESS : NotificationStatus.ERROR;
        String description = success ? SUCCESS_DESCRIPTION : ERROR_DESCRIPTION;
        updateNotificationStatus(organizationId, notification.getUuid(), status, description, notificationInternalList);
    }

    private boolean notificationIsExpected(Notification notification, AbstractNotificationScopeLevel threshold, String notificator)
            throws CatalogException {
        if (threshold == null || !threshold.isActive()) {
            return false;
        }
        if (CollectionUtils.isEmpty(threshold.getScopes())) {
            throw new CatalogException("The notification scope list is not defined for the " + notificator + " notificator.");
        }
        if (threshold.getMinLevel() == null) {
            throw new CatalogException("The notification min level is not defined for the " + notificator + " notificator.");
        }

        return threshold.getScopes().contains(notification.getScope())
                && notification.getLevel().getValue() >= threshold.getMinLevel().getValue();
    }

    private void updateNotificationStatus(String organizationId, String notificationUuid, String statusId, String description,
                                          List<NotificationInternalNotificationResult> notificationList) {
        NotificationUpdateParams updateParams = new NotificationUpdateParams()
                .setInternal(new NotificationInternalUpdateParams()
                        .setStatus(new NotificationInternalUpdateParams.NotificationInternalStatusUpdateParams(statusId, description)));
        if (CollectionUtils.isNotEmpty(notificationList)) {
            updateParams.getInternal().setNotificatorStatuses(notificationList);
        }
        try {
            catalogManager.getNotificationManager().update(organizationId, notificationUuid, updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e1) {
            // Try again...
            try {
                catalogManager.getNotificationManager().update(organizationId, notificationUuid, updateParams, QueryOptions.empty(), token);
            } catch (CatalogException e2) {
                logger.error("Error updating notification.", e1);
            }
        }
    }

    private User getUserConfiguration(String organizationId, Notification notification) {
        String userId = notification.getTarget();
        User user = userMap.get(userId);
        if (user == null) {
            try {
                user = catalogManager.getUserManager().get(organizationId, userId, userOptions, token).first();
//                user = dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).get(userId, userOptions).first();
            } catch (CatalogException e) {
                logger.error("Could not find user " + userId + " in organization " + organizationId, e);
                return null;
            }
            userMap.put(userId, user);
        }
        return user;
    }

    @Override
    public void close() throws IOException {

    }
}
