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
//    private final DBAdaptorFactory dbAdaptorFactory;

    private Map<String, User> userMap;
    private final QueryOptions userOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            UserDBAdaptor.QueryParams.ID.key(), UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.INTERNAL.key(),
            UserDBAdaptor.QueryParams.NOTIFICATIONS.key()
    ));

    public NotificationService(int interval, String token, CatalogManager catalogManager) {
        super(interval, token, catalogManager);
//        this.dbAdaptorFactory = dbAdaptorFactory;
        this.mailUtils = MailUtils.configure(catalogManager.getConfiguration().getEmail());
    }

    @Override
    public void apply() throws Exception {
        checkNotifications();
    }

    private void checkNotifications() throws CatalogException {
        userMap = new HashMap<>();
        List<String> organizationIds = catalogManager.getOrganizationManager().getOrganizationIds(token);

        boolean idle = false;
        while (!idle) {
            idle = true;
            for (String organizationId : organizationIds) {
                Query query = new Query(ParamConstants.INTERNAL_STATUS_PARAM, NotificationStatus.PENDING);
                try (DBIterator<Notification> iterator = catalogManager.getNotificationManager()
                        .iterator(organizationId, query, QueryOptions.empty(), token)) {
                    while (iterator.hasNext()) {
                        idle = false;
                        processNotification(organizationId, iterator.next());
                    }
                }
            }
        }
    }

    private void processNotification(String organizationId, Notification notification) {
        User user = getUserConfiguration(organizationId, notification);
        if (user == null) {
            updateNotificationStatus(organizationId, notification.getUuid(), NotificationStatus.ERROR, ERROR_DESCRIPTION, null);
        }
        if (!user.getInternal().getStatus().getId().equals(UserStatus.READY)) {
            updateNotificationStatus(organizationId, notification.getUuid(), NotificationStatus.DISCARDED,
                    "User account status is " + user.getInternal().getStatus().getId(), null);
        }
        NotificationConfiguration notificationConfiguration = user.getNotifications();
        if (!notificationConfiguration.isActive()) {
            updateNotificationStatus(organizationId, notification.getUuid(), NotificationStatus.SUCCESS, SUCCESS_DESCRIPTION, null);
        }

        List<NotificationInternalNotificationResult> notificationInternalList = new ArrayList<>();
        if (notificationConfiguration.getEmail().isActive()
                && notificationConfiguration.getEmail().getType().contains(notification.getType())) {
            if (StringUtils.isEmpty(user.getEmail())) {
                notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.EMAIL,
                        NotificationInternalNotificationResult.ERROR, "User does not have an email configured."));
            } else {
                try {
                    mailUtils.sendMail(user.getEmail(), notification.getSubject(), notification.getBody());
                    notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.EMAIL,
                            NotificationInternalNotificationResult.SUCCESS));
                } catch (Exception e) {
                    notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.EMAIL,
                            NotificationInternalNotificationResult.ERROR, "Could not send email: " + e.getMessage()));
                }
            }
        }
        if (notificationConfiguration.getSlack().isActive()
                && notificationConfiguration.getSlack().getType().contains(notification.getType())) {
            if (StringUtils.isEmpty(notificationConfiguration.getSlack().getWebhookUrl())) {
                notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.SLACK,
                        NotificationInternalNotificationResult.ERROR, "User does not have a Slack webhook configured."));
            } else {
                try {
                    // TODO: Send Slack notification
                    notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.SLACK,
                            NotificationInternalNotificationResult.SUCCESS));
                } catch (Exception e) {
                    notificationInternalList.add(new NotificationInternalNotificationResult(NotificationInternalNotificationResult.SLACK,
                            NotificationInternalNotificationResult.ERROR, "Could not send slack notification: " + e.getMessage()));
                }
            }
        }

        boolean success = notificationInternalList
                .stream()
                .allMatch((r) -> r.getStatus().equals(NotificationInternalNotificationResult.SUCCESS));
        String status = success ? NotificationStatus.SUCCESS : NotificationStatus.ERROR;
        String description = success ? SUCCESS_DESCRIPTION : ERROR_DESCRIPTION;
        updateNotificationStatus(status, notification.getUuid(), status, description, notificationInternalList);
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
