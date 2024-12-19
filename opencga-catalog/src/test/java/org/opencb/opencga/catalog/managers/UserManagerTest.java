package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.organizations.OrganizationConfiguration;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclParams;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import javax.naming.NamingException;
import java.io.IOException;
import java.util.*;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

@Category(MediumTests.class)
public class UserManagerTest extends AbstractManagerTest {

    @Test
    public void createOpencgaUserTest() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("forbidden");
        catalogManager.getUserManager().create(new User().setId(ParamConstants.OPENCGA_USER_ID).setName(orgOwnerUserId)
                .setOrganization(organizationId), TestParamConstants.PASSWORD, opencgaToken);
    }


    @Test
    public void testAdminUserExists() throws Exception {
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        JwtPayload payload = catalogManager.getUserManager().validateToken(token);
        assertEquals(ParamConstants.OPENCGA_USER_ID, payload.getUserId());
        assertEquals(ParamConstants.ADMIN_ORGANIZATION, payload.getOrganization());
    }

    @Test
    public void searchUsersTest() throws CatalogException {
        OpenCGAResult<User> search = catalogManager.getUserManager().search(organizationId, new Query(), QueryOptions.empty(), opencgaToken);
        assertEquals(8, search.getNumResults());
        for (User user : search.getResults()) {
            if (noAccessUserId1.equals(user.getId())) {
                assertEquals(0, user.getProjects().size());
            } else if (user.getId().startsWith("normalUser")) {
                assertEquals(1, user.getProjects().size());
            } else {
                assertEquals(2, user.getProjects().size());
            }
        }

        search = catalogManager.getUserManager().search(null, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(8, search.getNumResults());

        search = catalogManager.getUserManager().search(null, new Query(), QueryOptions.empty(), orgAdminToken2);
        assertEquals(8, search.getNumResults());

        search = catalogManager.getUserManager().search(null, new Query(), QueryOptions.empty(), orgAdminToken1);
        assertEquals(8, search.getNumResults());

        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().search(null, new Query(),
                QueryOptions.empty(), studyAdminToken1));
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().search(null, new Query(),
                QueryOptions.empty(), normalToken1));
    }

    @Test
    public void testGetToken() throws Exception {
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        Map<String, Object> claims = new HashMap<>();
        claims.put("a", "hola");
        claims.put("ab", "byw");
        // Create a token valid for 1 second
        String expiringToken = catalogManager.getUserManager().getToken(ParamConstants.ADMIN_ORGANIZATION, "opencga", claims, 1L, token);
        assertEquals("opencga", catalogManager.getUserManager().validateToken(expiringToken).getUserId());

        String nonExpiringToken = catalogManager.getUserManager().getNonExpiringToken(ParamConstants.ADMIN_ORGANIZATION, "opencga", claims, token);
        assertEquals("opencga", catalogManager.getUserManager().validateToken(nonExpiringToken).getUserId());

        Thread.sleep(1000);
        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage("expired");
        assertEquals("opencga", catalogManager.getUserManager().validateToken(expiringToken).getUserId());
    }

    @Test
    public void loginWithoutOrganizationId() throws CatalogException {
        String token = catalogManager.getUserManager().login(null, ParamConstants.OPENCGA_USER_ID, TestParamConstants.ADMIN_PASSWORD).getToken();
        assertTrue(StringUtils.isNotEmpty(token));
        JwtPayload jwtPayload = new JwtPayload(token);
        assertEquals(ParamConstants.ADMIN_ORGANIZATION, jwtPayload.getOrganization());

        token = catalogManager.getUserManager().login(null, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();
        assertTrue(StringUtils.isNotEmpty(token));
        jwtPayload = new JwtPayload(token);
        assertEquals(organizationId, jwtPayload.getOrganization());

        // Create a third organization
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId("other").setName("Test"), QueryOptions.empty(), opencgaToken);
        token = catalogManager.getUserManager().login(null, ParamConstants.OPENCGA_USER_ID, TestParamConstants.ADMIN_PASSWORD).getToken();
        assertTrue(StringUtils.isNotEmpty(token));
        jwtPayload = new JwtPayload(token);
        assertEquals(ParamConstants.ADMIN_ORGANIZATION, jwtPayload.getOrganization());

        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("organization");
        catalogManager.getUserManager().login(null, orgOwnerUserId, TestParamConstants.PASSWORD);
    }

    @Test
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getUserManager().create(orgOwnerUserId, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId,
                null, opencgaToken);
    }

    @Test
    public void testCreateAnonymousUser() throws Exception {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("reserved"));
        catalogManager.getUserManager().create(ParamConstants.ANONYMOUS_USER_ID, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD,
                organizationId, null, opencgaToken);
    }

    @Test
    public void testCreateRegisteredUser() throws Exception {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("reserved"));
        catalogManager.getUserManager().create(ParamConstants.REGISTERED_USERS, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId, null,
                opencgaToken);
    }

    @Test
    public void testLogin() throws Exception {
        catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD);

        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage(allOf(containsString("Incorrect"), containsString("password")));
        catalogManager.getUserManager().login(organizationId, normalUserId1, "fakePassword");
    }

    @Test
    public void refreshTokenTest() throws Exception {
        String refreshToken = catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD).getRefreshToken();
        AuthenticationResponse authenticationResponse = catalogManager.getUserManager().refreshToken(refreshToken);
        assertNotNull(authenticationResponse);
        assertNotNull(authenticationResponse.getToken());
    }

    @Test
    public void anonymousUserLoginTest() throws CatalogException {
        AuthenticationResponse authResponse = catalogManager.getUserManager().loginAnonymous(organizationId);
        assertNotNull(authResponse.getToken());

        String org2 = "otherOrg";
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(org2), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("userFromOrg2").setName("name").setOrganization(org2), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getOrganizationManager().update(org2, new OrganizationUpdateParams().setOwner("userFromOrg2"), null, opencgaToken);
        String owner2Token = catalogManager.getUserManager().login(org2, "userFromOrg2", TestParamConstants.PASSWORD).getToken();
        Project p = catalogManager.getProjectManager().create(new ProjectCreateParams()
                        .setId("project")
                        .setOrganism(new ProjectOrganism("Homo sapiens", "GRCh38")),
                INCLUDE_RESULT, owner2Token).first();
        Study study = catalogManager.getStudyManager().create(p.getFqn(), new Study().setId("study"), INCLUDE_RESULT, owner2Token).first();

        try {
            catalogManager.getUserManager().loginAnonymous(org2);
            fail("Anonymous user should not get a token for that organization as it has not been granted any kind of access");
        } catch (Exception e) {
            assertEquals(CatalogAuthenticationException.class, e.getClass());
            assertTrue(e.getMessage().contains("not found"));
        }

        catalogManager.getStudyManager().updateGroup(study.getFqn(), ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), owner2Token);
        authResponse = catalogManager.getUserManager().loginAnonymous(org2);
        assertNotNull(authResponse.getToken());


        catalogManager.getStudyManager().updateGroup(study.getFqn(), ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Collections.singletonList("*")), owner2Token);
        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage("not found");
        catalogManager.getUserManager().loginAnonymous(org2);
    }

    @Test
    public void incrementLoginAttemptsTest() throws CatalogException {
        assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getUserManager().login(organizationId, normalUserId1, "incorrect"));
        User user = catalogManager.getUserManager().get(organizationId, normalUserId1, QueryOptions.empty(), ownerToken).first();
        UserInternal userInternal3 = user.getInternal();
        assertEquals(1, userInternal3.getAccount().getFailedAttempts());
        assertEquals(UserStatus.READY, user.getInternal().getStatus().getId());

        for (int i = 2; i < 5; i++) {
            assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getUserManager().login(organizationId, normalUserId1, "incorrect"));
            user = catalogManager.getUserManager().get(organizationId, normalUserId1, QueryOptions.empty(), ownerToken).first();
            UserInternal userInternal = user.getInternal();
            assertEquals(i, userInternal.getAccount().getFailedAttempts());
            assertEquals(UserStatus.READY, user.getInternal().getStatus().getId());
        }

        assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getUserManager().login(organizationId, normalUserId1, "incorrect"));
        user = catalogManager.getUserManager().get(organizationId, normalUserId1, QueryOptions.empty(), ownerToken).first();
        UserInternal userInternal2 = user.getInternal();
        assertEquals(5, userInternal2.getAccount().getFailedAttempts());
        assertEquals(UserStatus.BANNED, user.getInternal().getStatus().getId());

        CatalogAuthenticationException incorrect = assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getUserManager().login(organizationId, normalUserId1, "incorrect"));
        assertTrue(incorrect.getMessage().contains("banned"));
        user = catalogManager.getUserManager().get(organizationId, normalUserId1, QueryOptions.empty(), ownerToken).first();
        UserInternal userInternal1 = user.getInternal();
        assertEquals(5, userInternal1.getAccount().getFailedAttempts());
        assertEquals(UserStatus.BANNED, user.getInternal().getStatus().getId());

        CatalogAuthenticationException authException = assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD));
        assertTrue(authException.getMessage().contains("banned"));

        // Remove ban from user
        catalogManager.getUserManager().changeStatus(organizationId, normalUserId1, UserStatus.READY, QueryOptions.empty(), ownerToken);
        user = catalogManager.getUserManager().get(organizationId, normalUserId1, QueryOptions.empty(), ownerToken).first();
        UserInternal userInternal = user.getInternal();
        assertEquals(0, userInternal.getAccount().getFailedAttempts());
        assertEquals(UserStatus.READY, user.getInternal().getStatus().getId());

        String token = catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD).getToken();
        assertNotNull(token);
    }

    @Test
    public void changeUserStatusTest() throws CatalogException {
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().changeStatus(organizationId, normalUserId1, UserStatus.BANNED, QueryOptions.empty(), normalToken1));
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().changeStatus(organizationId, normalUserId1, UserStatus.BANNED, QueryOptions.empty(), studyAdminToken1));
        assertThrows(CatalogParameterException.class, () -> catalogManager.getUserManager().changeStatus(organizationId, normalUserId1, UserStatus.BANNED, QueryOptions.empty(), ownerToken));
        catalogManager.getUserManager().changeStatus(organizationId, normalUserId1, UserStatus.SUSPENDED, QueryOptions.empty(), ownerToken);
        catalogManager.getUserManager().changeStatus(organizationId, normalUserId1, UserStatus.SUSPENDED, QueryOptions.empty(), orgAdminToken1);
        catalogManager.getUserManager().changeStatus(organizationId, normalUserId1, UserStatus.SUSPENDED, QueryOptions.empty(), opencgaToken);

        catalogManager.getUserManager().changeStatus(organizationId, orgAdminUserId1, UserStatus.SUSPENDED, QueryOptions.empty(), ownerToken);
        CatalogAuthorizationException authException = assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().changeStatus(organizationId, orgOwnerUserId, UserStatus.SUSPENDED, QueryOptions.empty(), ownerToken));
        assertTrue(authException.getMessage().contains("own account"));

        authException = assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().changeStatus(organizationId, orgAdminUserId1, UserStatus.SUSPENDED, QueryOptions.empty(), orgAdminToken2));
        assertTrue(authException.getMessage().contains("suspend administrators"));

        CatalogAuthenticationException incorrect = assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getUserManager().login(organizationId, orgAdminUserId1, TestParamConstants.PASSWORD));
        assertTrue(incorrect.getMessage().contains("suspended"));

        catalogManager.getUserManager().changeStatus(organizationId, orgAdminUserId1, UserStatus.READY, QueryOptions.empty(), orgAdminToken2);
        String token = catalogManager.getUserManager().login(organizationId, orgAdminUserId1, TestParamConstants.PASSWORD).getToken();
        assertNotNull(token);

        CatalogParameterException paramException = assertThrows(CatalogParameterException.class, () -> catalogManager.getUserManager().changeStatus(organizationId, orgAdminUserId1, "NOT_A_STATUS", QueryOptions.empty(), orgAdminToken2));
        assertTrue(paramException.getMessage().contains("Invalid status"));

        CatalogDBException dbException = assertThrows(CatalogDBException.class, () -> catalogManager.getUserManager().changeStatus(organizationId, "notAUser", UserStatus.SUSPENDED, QueryOptions.empty(), orgAdminToken2));
        assertTrue(dbException.getMessage().contains("not exist"));
    }

    @Test
    public void loginExpiredAccountTest() throws CatalogException {
        // Expire account of normalUserId1
        ObjectMap params = new ObjectMap(UserDBAdaptor.QueryParams.INTERNAL_ACCOUNT_EXPIRATION_DATE.key(), TimeUtils.getTime());
        catalogManager.getUserManager().getUserDBAdaptor(organizationId).update(normalUserId1, params);

        CatalogAuthenticationException authException = assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD));
        assertTrue(authException.getMessage().contains("expired"));

        // Ensure it doesn't matter whether opencga account is expired or not
        catalogManager.getUserManager().getUserDBAdaptor(ParamConstants.ADMIN_ORGANIZATION).update(ParamConstants.OPENCGA_USER_ID, params);
        String token = catalogManager.getUserManager().login(ParamConstants.ADMIN_ORGANIZATION, ParamConstants.OPENCGA_USER_ID, TestParamConstants.ADMIN_PASSWORD).getToken();
        assertNotNull(token);
    }

    @Test
    public void updateUserTest() throws JsonProcessingException, CatalogException {
        UserUpdateParams userUpdateParams = new UserUpdateParams()
                .setName("newName")
                .setEmail("mail@mail.com");
        ObjectMap updateParams = new ObjectMap(getUpdateObjectMapper().writeValueAsString(userUpdateParams));
        User user = catalogManager.getUserManager().update(normalUserId1, updateParams, INCLUDE_RESULT, normalToken1).first();
        assertEquals(userUpdateParams.getName(), user.getName());
        assertEquals(userUpdateParams.getEmail(), user.getEmail());

        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().update(normalUserId1, updateParams, INCLUDE_RESULT, normalToken2));
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().update(normalUserId1, updateParams, INCLUDE_RESULT, opencgaToken));
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().update(normalUserId1, updateParams, INCLUDE_RESULT, ownerToken));
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().update(normalUserId1, updateParams, INCLUDE_RESULT, orgAdminToken1));
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getUserManager().update(normalUserId1, updateParams, INCLUDE_RESULT, studyAdminToken1));

        userUpdateParams = new UserUpdateParams()
                .setEmail("notAnEmail");
        ObjectMap updateParams2 = new ObjectMap(getUpdateObjectMapper().writeValueAsString(userUpdateParams));
        assertThrows(CatalogParameterException.class, () -> catalogManager.getUserManager().update(normalUserId1, updateParams2, INCLUDE_RESULT, normalToken1));
    }

    @Test
    public void testGetUserInfo() throws CatalogException {
        // OpenCGA administrator
        DataResult<User> user = catalogManager.getUserManager().get(organizationId,
                Arrays.asList(normalUserId1, normalUserId2, normalUserId3), new QueryOptions(), opencgaToken);
        assertEquals(3, user.getNumResults());
        assertEquals(normalUserId1, user.getResults().get(0).getId());
        assertEquals(normalUserId2, user.getResults().get(1).getId());
        assertEquals(normalUserId3, user.getResults().get(2).getId());

        // Organization owner
        user = catalogManager.getUserManager().get(organizationId, Arrays.asList(normalUserId1, normalUserId2, normalUserId3),
                new QueryOptions(), ownerToken);
        assertEquals(3, user.getNumResults());
        assertEquals(normalUserId1, user.getResults().get(0).getId());
        assertEquals(normalUserId2, user.getResults().get(1).getId());
        assertEquals(normalUserId3, user.getResults().get(2).getId());

        // Organization administrator
        user = catalogManager.getUserManager().get(organizationId, Arrays.asList(normalUserId1, normalUserId2, normalUserId3),
                new QueryOptions(), orgAdminToken1);
        assertEquals(3, user.getNumResults());
        assertEquals(normalUserId1, user.getResults().get(0).getId());
        assertEquals(normalUserId2, user.getResults().get(1).getId());
        assertEquals(normalUserId3, user.getResults().get(2).getId());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("organization");
        catalogManager.getUserManager().get(organizationId, Arrays.asList(normalUserId1, normalUserId2, normalUserId3), new QueryOptions(),
                studyAdminToken1);
    }

    @Test
    public void testGetProjectsFromUserInfo() throws CatalogException {
        String userId = organizationId;
        catalogManager.getUserManager().create(userId, "test", "mail@mail.com", TestParamConstants.PASSWORD, organizationId, null,
                opencgaToken);
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("test")), ownerToken);
        String token = catalogManager.getUserManager().login(organizationId, userId, TestParamConstants.PASSWORD).getToken();

        DataResult<User> user = catalogManager.getUserManager().get(organizationId, userId, new QueryOptions(), token);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, normalUserId3, new QueryOptions(), normalToken3);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, orgOwnerUserId, new QueryOptions(), ownerToken);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, orgAdminUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, studyAdminUserId1, new QueryOptions(), studyAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, normalUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());


        user = catalogManager.getUserManager().get(null, normalUserId1, new QueryOptions(), normalToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, normalUserId3, new QueryOptions(), normalToken3);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, orgOwnerUserId, new QueryOptions(), ownerToken);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, orgAdminUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, studyAdminUserId1, new QueryOptions(), studyAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, normalUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());
    }

    @Test
    public void testModifyUser() throws CatalogException, InterruptedException, IOException {
        ObjectMap params = new ObjectMap();
        String newName = "Changed Name " + RandomStringUtils.randomAlphanumeric(10);
        String newPassword = PasswordUtils.getStrongRandomPassword();
        String newEmail = "new@email.ac.uk";

        params.put("name", newName);

        Thread.sleep(10);

        catalogManager.getUserManager().update(orgOwnerUserId, params, null, ownerToken);
        catalogManager.getUserManager().update(orgOwnerUserId, new ObjectMap("email", newEmail), null, ownerToken);
        catalogManager.getUserManager().changePassword(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD, newPassword);

        List<User> userList = catalogManager.getUserManager().get(organizationId, orgOwnerUserId, new QueryOptions(QueryOptions
                .INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key(),
                UserDBAdaptor.QueryParams.ATTRIBUTES.key())), ownerToken).getResults();
        User userPost = userList.get(0);
        System.out.println("userPost = " + userPost);
        assertEquals(userPost.getName(), newName);
        assertEquals(userPost.getEmail(), newEmail);

        catalogManager.getUserManager().login(organizationId, orgOwnerUserId, newPassword);
        CatalogAuthenticationException exception = assertThrows(CatalogAuthenticationException.class,
                () -> catalogManager.getUserManager().changePassword(organizationId, orgOwnerUserId, newPassword, TestParamConstants.PASSWORD));
        assertTrue(exception.getMessage().contains("The new password has already been used"));

        String anotherPassword = PasswordUtils.getStrongRandomPassword();
        catalogManager.getUserManager().changePassword(organizationId, orgOwnerUserId, newPassword, anotherPassword);
        catalogManager.getUserManager().login(organizationId, orgOwnerUserId, anotherPassword);

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.getUserManager().update(orgOwnerUserId, params, null, ownerToken);
            fail("Expected exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        try {
            catalogManager.getUserManager().update(orgOwnerUserId, params, null, orgAdminToken1);
            fail("Expected exception");
        } catch (CatalogException e) {
            System.out.println(e);
        }
    }

    @Test
    public void automaticPasswordExpirationTest() throws CatalogException {
        // Set 1 day of password expiration
        catalogManager.getConfiguration().getAccount().setPasswordExpirationDays(1);

        String oneDay = TimeUtils.getTime(TimeUtils.addDaysToCurrentDate(1));
        String twoDays = TimeUtils.getTime(TimeUtils.addDaysToCurrentDate(2));

        User user = new User().setId("tempUser");
        String password = PasswordUtils.getStrongRandomPassword();
        User storedUser = catalogManager.getUserManager().create(user, password, ownerToken).first();
        Account account2 = storedUser.getInternal().getAccount();
        assertTrue(Long.parseLong(oneDay) <= Long.parseLong(account2.getPassword().getExpirationDate()));
        Account account1 = storedUser.getInternal().getAccount();
        assertTrue(Long.parseLong(twoDays) > Long.parseLong(account1.getPassword().getExpirationDate()));

        // Set 1 day of password expiration
        catalogManager.getConfiguration().getAccount().setPasswordExpirationDays(-5);
        user = new User().setId("tempUser2");
        storedUser = catalogManager.getUserManager().create(user, password, ownerToken).first();
        Account account = storedUser.getInternal().getAccount();
        assertNull(account.getPassword().getExpirationDate());
    }

    @Test
    public void loginUserPasswordExpiredTest() throws CatalogException {
        try (CatalogManager mockCatalogManager = mockCatalogManager()) {
            UserDBAdaptor userDBAdaptor = mockCatalogManager.getUserManager().getUserDBAdaptor(organizationId);

            OpenCGAResult<User> result = mockCatalogManager.getUserManager().get(organizationId, normalUserId1, new QueryOptions(), normalToken1);

            // Set password expired 2 days ago
            Date date = TimeUtils.addDaysToCurrentDate(-2);
            String beforeYesterday = TimeUtils.getTime(date);
            User user = result.first();
            user.getInternal().getAccount().getPassword().setExpirationDate(beforeYesterday);

            Mockito.doReturn(result).when(userDBAdaptor).get(normalUserId1, UserManager.INCLUDE_INTERNAL);
            CatalogAuthenticationException exception = assertThrows(CatalogAuthenticationException.class,
                    () -> mockCatalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD));
            assertTrue(exception.getMessage().contains("expired on " + beforeYesterday));
        }
    }

    @Test
    public void changePasswordTest() throws CatalogException {
        String newPassword = PasswordUtils.getStrongRandomPassword();
        catalogManager.getUserManager().changePassword(organizationId, normalUserId1, TestParamConstants.PASSWORD, newPassword);
        catalogManager.getUserManager().login(organizationId, normalUserId1, newPassword);

        CatalogAuthenticationException exception = assertThrows(CatalogAuthenticationException.class,
                () -> catalogManager.getUserManager().changePassword(organizationId, normalUserId1, TestParamConstants.PASSWORD, newPassword));
        assertTrue(exception.getMessage().contains("verify that the current password is correct"));

        String anotherPassword = PasswordUtils.getStrongRandomPassword();
        catalogManager.getUserManager().changePassword(organizationId, normalUserId1, newPassword, anotherPassword);
        catalogManager.getUserManager().login(organizationId, normalUserId1, anotherPassword);
    }

    @Test
    public void testUpdateUserConfig() throws CatalogException {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        catalogManager.getUserManager().setConfig(normalUserId1, "a", map, normalToken1);

        Map<String, Object> config = (Map<String, Object>) catalogManager.getUserManager().getConfig(normalUserId1, "a", normalToken1).first();
        assertEquals(2, config.size());
        assertEquals("value1", config.get("key1"));
        assertEquals("value2", config.get("key2"));

        map = new HashMap<>();
        map.put("key2", "value3");
        catalogManager.getUserManager().setConfig(normalUserId1, "a", map, normalToken1);
        config = (Map<String, Object>) catalogManager.getUserManager().getConfig(normalUserId1, "a", normalToken1).first();
        assertEquals(1, config.size());
        assertEquals("value3", config.get("key2"));

        catalogManager.getUserManager().deleteConfig(normalUserId1, "a", normalToken1);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getUserManager().getConfig(normalUserId1, "a", normalToken1);
    }

    private String getAdminToken() throws CatalogException, IOException {
        return catalogManager.getUserManager().loginAsAdmin("admin").getToken();
    }

    @Test
    public void createUserUsingMailAsId() throws CatalogException {
        catalogManager.getUserManager().create(new User().setId("hello.mail@mymail.org").setName("Hello"), TestParamConstants.PASSWORD, ownerToken);
        AuthenticationResponse login = catalogManager.getUserManager().login(organizationId, "hello.mail@mymail.org", TestParamConstants.PASSWORD);
        assertNotNull(login);
        User user = catalogManager.getUserManager().get(organizationId, "hello.mail@mymail.org", new QueryOptions(), login.getToken()).first();
        assertEquals("hello.mail@mymail.org", user.getId());
    }

    @Test
    public void getUserInfoTest() throws CatalogException {
        OpenCGAResult<User> result = catalogManager.getUserManager().get(organizationId, orgOwnerUserId, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(2, result.first().getProjects().size());

        result = catalogManager.getUserManager().get(organizationId, orgAdminUserId1, QueryOptions.empty(), orgAdminToken1);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(2, result.first().getProjects().size());

        result = catalogManager.getUserManager().get(organizationId, studyAdminUserId1, QueryOptions.empty(), studyAdminToken1);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(2, result.first().getProjects().size());

        result = catalogManager.getUserManager().get(organizationId, normalUserId1, QueryOptions.empty(), normalToken1);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(1, result.first().getProjects().size());
    }

    @Ignore
    @Test
    public void importLdapUsers() throws CatalogException, NamingException, IOException {
        // Action only for admins
        catalogManager.getUserManager().importRemoteEntities(organizationId, "ldap", Arrays.asList("pfurio", "imedina"), false, null, null,
                getAdminToken());
        // TODO: Validate the users have been imported
    }

    // To make this test work we will need to add a correct user and password to be able to login
    @Ignore
    @Test
    public void loginNotRegisteredUsers() throws CatalogException {
        // Action only for admins
        Group group = new Group("ldap", Collections.emptyList()).setSyncedFrom(new Group.Sync("ldap", "bio"));
        catalogManager.getStudyManager().createGroup(studyFqn, group, ownerToken);
        catalogManager.getStudyManager().updateAcl(studyFqn, "@ldap", new StudyAclParams("", "view_only"),
                ParamUtils.AclAction.SET, ownerToken);
        String token = catalogManager.getUserManager().login(organizationId, orgOwnerUserId, "password").getToken();

        assertEquals(9, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());

        // We remove the permissions for group ldap
        catalogManager.getStudyManager().updateAcl(studyFqn, "@ldap", new StudyAclParams("", ""),
                ParamUtils.AclAction.RESET, this.ownerToken);

        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());
    }

    @Ignore
    @Test
    public void syncUsers() throws CatalogException {
        // Action only for admins
        String token = catalogManager.getUserManager().loginAsAdmin("admin").getToken();

        catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, "ldap", "bio", "bio", studyFqn, true, token);
        DataResult<Group> bio = catalogManager.getStudyManager().getGroup(studyFqn, "bio", this.ownerToken);

        assertEquals(1, bio.getNumResults());
        assertEquals(0, bio.first().getUserIds().size());

        catalogManager.getUserManager().syncAllUsersOfExternalGroup(organizationId, studyFqn, "ldap", token);
        bio = catalogManager.getStudyManager().getGroup(studyFqn, "bio", this.ownerToken);

        assertEquals(1, bio.getNumResults());
        assertTrue(!bio.first().getUserIds().isEmpty());
    }

    @Ignore
    @Test
    public void importLdapGroups() throws CatalogException, IOException {
        // Action only for admins
        String remoteGroup = "bio";
        String internalGroup = "test";
        catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, "ldap", remoteGroup, internalGroup, studyFqn, true, getAdminToken());

        DataResult<Group> test = catalogManager.getStudyManager().getGroup(studyFqn, "test", ownerToken);
        assertEquals(1, test.getNumResults());
        assertEquals("@test", test.first().getId());
        assertTrue(test.first().getUserIds().size() > 0);

//        internalGroup = "test1";
//        try {
//            catalogManager.getUserManager().importRemoteGroupOfUsers("ldap", remoteGroup, internalGroup, study, getAdminToken());
//            fail("Should not be possible creating another group containing the same users that belong to a different group");
//        } catch (CatalogException e) {
//            System.out.println(e.getMessage());
//        }

        remoteGroup = "bioo";
        internalGroup = "test2";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, "ldap", remoteGroup, internalGroup, studyFqn, true, getAdminToken());
    }

    @Test
    public void syncUsersTest() throws CatalogException {
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, ParamUtils.UpdateAction.ADD);
        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS, actionMap);

        List<AuthenticationOrigin> authenticationOrigins = Collections.singletonList(new AuthenticationOrigin("CAS",
                AuthenticationOrigin.AuthenticationType.SSO, null, null));
        OrganizationConfiguration organizationConfiguration = new OrganizationConfiguration()
                .setAuthenticationOrigins(authenticationOrigins);
        catalogManager.getOrganizationManager().updateConfiguration(organizationId, organizationConfiguration, queryOptions, orgAdminToken1);

        catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, "CAS", "opencb", "opencb", studyFqn, true, opencgaToken);
        OpenCGAResult<Group> opencb = catalogManager.getStudyManager().getGroup(studyFqn, "opencb", studyAdminToken1);
        assertEquals(1, opencb.getNumResults());
        assertEquals("@opencb", opencb.first().getId());
    }

}
