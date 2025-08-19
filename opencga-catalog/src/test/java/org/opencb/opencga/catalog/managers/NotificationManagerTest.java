package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.notification.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class NotificationManagerTest extends AbstractManagerTest {

    private NotificationManager notificationManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        notificationManager = catalogManager.getNotificationManager();
    }

    @Test
    public void testOpenCgaUserCanCreateAnyNotification() throws CatalogException {
        NotificationCreateParams globalParams = new NotificationCreateParams()
                .setSubject("Global Notification")
                .setContent("This is a global notification")
                .setScope(NotificationScope.GLOBAL)
                .setLevel(NotificationLevel.INFO)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(noAccessUserId1));

        NotificationCreateParams orgParams = new NotificationCreateParams()
                .setSubject("Organization Notification")
                .setContent("This is an organization notification")
                .setScope(NotificationScope.ORGANIZATION)
                .setLevel(NotificationLevel.INFO)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(noAccessUserId1));

        NotificationCreateParams projParams = new NotificationCreateParams()
                .setSubject("Project Notification")
                .setContent("This is a project notification")
                .setScope(NotificationScope.PROJECT)
                .setLevel(NotificationLevel.INFO)
                .setFqn(projectFqn1)
                .setTargets(Collections.singletonList(normalUserId2));

        NotificationCreateParams studyParams = new NotificationCreateParams()
                .setSubject("Study Notification")
                .setContent("This is a study notification")
                .setScope(NotificationScope.STUDY)
                .setLevel(NotificationLevel.INFO)
                .setFqn(studyFqn)
                .setTargets(Collections.singletonList(normalUserId2));

        // OpenCGA user should be able to create notifications of all scopes
        OpenCGAResult<Notification> globalNotification = notificationManager.create(globalParams, QueryOptions.empty(), opencgaToken);
        assertEquals(1, globalNotification.getNumInserted());

        OpenCGAResult<Notification> orgNotification = notificationManager.create(orgParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, orgNotification.getNumInserted());

        OpenCGAResult<Notification> projNotification = notificationManager.create(projParams, QueryOptions.empty(), orgAdminToken1);
        assertEquals(1, projNotification.getNumInserted());

        OpenCGAResult<Notification> studyNotification = notificationManager.create(studyParams, QueryOptions.empty(), normalToken1);
        assertEquals(1, studyNotification.getNumInserted());
    }

    @Test
    public void testOrganizationRolesCanCreateOrganizationNotifications() throws CatalogException {
        // Organization owner
        NotificationCreateParams orgOwnerParams = new NotificationCreateParams()
                .setSubject("Org Owner Notification")
                .setContent("This is an organization notification from owner")
                .setScope(NotificationScope.ORGANIZATION)
                .setLevel(NotificationLevel.INFO)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(noAccessUserId1));

        OpenCGAResult<Notification> orgOwnerNotif = notificationManager.create(orgOwnerParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, orgOwnerNotif.getNumInserted());

        // Organization admin
        NotificationCreateParams orgAdminParams = new NotificationCreateParams()
                .setSubject("Org Admin Notification")
                .setContent("This is an organization notification from admin")
                .setScope(NotificationScope.ORGANIZATION)
                .setLevel(NotificationLevel.INFO)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(noAccessUserId1));

        OpenCGAResult<Notification> orgAdminNotif = notificationManager.create(orgAdminParams, QueryOptions.empty(), orgAdminToken1);
        assertEquals(1, orgAdminNotif.getNumInserted());
    }

    @Test
    public void testProjectAdminCanCreateProjectNotifications() throws CatalogException {
        NotificationCreateParams projParams = new NotificationCreateParams()
                .setSubject("Project Admin Notification")
                .setContent("This is a project notification from admin")
                .setScope(NotificationScope.PROJECT)
                .setLevel(NotificationLevel.INFO)
                .setFqn(projectFqn1)
                .setTargets(Collections.singletonList(normalUserId3));

        OpenCGAResult<Notification> projNotif = notificationManager.create(projParams, QueryOptions.empty(), studyAdminToken1);
        assertEquals(1, projNotif.getNumInserted());
    }

    @Test
    public void testStudyMemberCanCreateStudyNotifications() throws CatalogException {
        NotificationCreateParams studyParams = new NotificationCreateParams()
                .setSubject("Study Member Notification")
                .setContent("This is a study notification from member")
                .setScope(NotificationScope.STUDY)
                .setLevel(NotificationLevel.INFO)
                .setFqn(studyFqn)
                .setTargets(Collections.singletonList(normalUserId2));

        OpenCGAResult<Notification> studyNotif = notificationManager.create(studyParams, QueryOptions.empty(), normalToken1);
        assertEquals(1, studyNotif.getNumInserted());
    }

    @Test
    public void testUserOnlySeesOwnNotifications() throws CatalogException {
        // Create notifications for different users
        NotificationCreateParams user1Params = new NotificationCreateParams()
                .setSubject("User1 Notification")
                .setContent("This is a notification for user1")
                .setLevel(NotificationLevel.INFO)
                .setScope(NotificationScope.ORGANIZATION)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(orgAdminUserId1));

        NotificationCreateParams user2Params = new NotificationCreateParams()
                .setSubject("User2 Notification")
                .setContent("This is a notification for user2")
                .setScope(NotificationScope.ORGANIZATION)
                .setLevel(NotificationLevel.INFO)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(normalUserId1));

        notificationManager.create(user1Params, QueryOptions.empty(), ownerToken);
        notificationManager.create(user2Params, QueryOptions.empty(), ownerToken);

        // User1 should only see their notifications
        OpenCGAResult<Notification> user1Notifications = notificationManager.search(organizationId, new Query(), QueryOptions.empty(), orgAdminToken1);
        assertEquals(1, user1Notifications.getNumResults());
        for (Notification notification : user1Notifications.getResults()) {
            assertEquals(orgAdminUserId1, notification.getTarget());
        }

        // User2 should only see their notifications
        OpenCGAResult<Notification> user2Notifications = notificationManager.search(organizationId, new Query(), QueryOptions.empty(), normalToken1);
        assertEquals(1, user2Notifications.getNumResults());
        for (Notification notification : user2Notifications.getResults()) {
            assertEquals(normalUserId1, notification.getTarget());
        }
    }

    @Test
    public void testUserCanMarkNotificationAsVisited() throws CatalogException {
        // Create a notification for the user
        NotificationCreateParams params = new NotificationCreateParams()
                .setSubject("Test Notification")
                .setContent("This is a test notification")
                .setScope(NotificationScope.ORGANIZATION)
                .setLevel(NotificationLevel.INFO)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(normalUserId1));

        notificationManager.create(params, QueryOptions.empty(), ownerToken);
        Notification notification = notificationManager.search(organizationId, new Query(), QueryOptions.empty(), normalToken1).first();
        assertNotNull(notification);

        // Verify initial status
        assertFalse(notification.getInternal().isVisited());

        // Mark as visited
        OpenCGAResult<Notification> visit = notificationManager.visit(organizationId, notification.getUuid(), normalToken1);
        assertEquals(1, visit.getNumUpdated());

        // Verify new status
        notification = notificationManager.search(organizationId, new Query(), QueryOptions.empty(), normalToken1).first();
        assertTrue(notification.getInternal().isVisited());
    }

    @Test(expected = CatalogAuthorizationException.class)
    public void testNormalUserCannotCreateGlobalNotification() throws CatalogException {
        NotificationCreateParams params = new NotificationCreateParams()
                .setSubject("Global Notification")
                .setContent("This is a global notification")
                .setFqn(organizationId)
                .setScope(NotificationScope.GLOBAL);

        // This should throw a CatalogException as normal users cannot create global notifications
        notificationManager.create(params, QueryOptions.empty(), normalToken1);
    }

    @Test(expected = CatalogAuthorizationException.class)
    public void testNormalUserCannotCreateOrganizationNotification() throws CatalogException {
        NotificationCreateParams params = new NotificationCreateParams()
                .setSubject("Org Notification")
                .setContent("This is an organization notification")
                .setScope(NotificationScope.ORGANIZATION)
                .setFqn(organizationId)
                .setTargets(Collections.singletonList(noAccessUserId1));

        // This should throw a CatalogException as normal users cannot create organization notifications
        notificationManager.create(params, QueryOptions.empty(), normalToken1);
    }

    @Test(expected = CatalogAuthorizationException.class)
    public void testNormalUserCannotCreateProjectNotification() throws CatalogException {
        NotificationCreateParams params = new NotificationCreateParams()
                .setSubject("Project Notification")
                .setContent("This is a project notification")
                .setScope(NotificationScope.PROJECT)
                .setFqn(projectFqn1)
                .setTargets(Collections.singletonList(noAccessUserId1));;

        // This should throw a CatalogException as normal users cannot create project notifications
        notificationManager.create(params, QueryOptions.empty(), normalToken1);
    }

    @Test
    public void testBatchCreation() throws CatalogException {
        // Create multiple notifications at once
        NotificationCreateParams params = new NotificationCreateParams()
                .setSubject("Batch Notification 1")
                .setContent("This is batch notification 1")
                .setScope(NotificationScope.ORGANIZATION)
                .setLevel(NotificationLevel.INFO)
                .setFqn(organizationId)
                .setTargets(Arrays.asList(normalUserId1, normalUserId2));

        notificationManager.create(params, QueryOptions.empty(), ownerToken);

        // User1 should only see their notifications
        OpenCGAResult<Notification> user1Notifications = notificationManager.search(organizationId, new Query(), QueryOptions.empty(), normalToken1);
        assertEquals(1, user1Notifications.getNumResults());
        String operationUuid = null;
        for (Notification notification : user1Notifications.getResults()) {
            assertEquals(normalUserId1, notification.getTarget());
            operationUuid = notification.getOperationId();
        }

        // User2 should only see their notifications
        OpenCGAResult<Notification> user2Notifications = notificationManager.search(organizationId, new Query(), QueryOptions.empty(), normalToken2);
        assertEquals(1, user2Notifications.getNumResults());
        for (Notification notification : user2Notifications.getResults()) {
            assertEquals(normalUserId2, notification.getTarget());
            assertEquals(operationUuid, notification.getOperationId());
        }
    }
}
