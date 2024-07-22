package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Optimizations;
import org.opencb.opencga.core.models.organizations.*;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.OrganizationUserUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserQuota;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.*;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class OrganizationManagerTest extends AbstractManagerTest {

    @Test
    public void ensureAuthOriginExistsTest() throws CatalogException {
        Organization organization = catalogManager.getOrganizationManager().get(organizationId, null, ownerToken).first();
        assertFalse(organization.getConfiguration().getAuthenticationOrigins().isEmpty());
        assertNotNull(organization.getConfiguration().getAuthenticationOrigins().get(0));
    }

    @Test
    public void updateConfigurationAuthorizationTest() throws CatalogException {
        OrganizationConfiguration configuration = new OrganizationConfiguration()
                .setOptimizations(new Optimizations(true));

        // Owner can update configuration
        catalogManager.getOrganizationManager().updateConfiguration(organizationId, configuration, QueryOptions.empty(), ownerToken);

        // Admin can update configuration
        catalogManager.getOrganizationManager().updateConfiguration(organizationId, configuration, QueryOptions.empty(), orgAdminToken1);

        // Admin can update configuration
        catalogManager.getOrganizationManager().updateConfiguration(organizationId, configuration, QueryOptions.empty(), orgAdminToken2);

        // Study admin cannot update configuration
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getOrganizationManager().updateConfiguration(organizationId,
                configuration, QueryOptions.empty(), studyAdminToken1));

        // Normal user cannot update configuration
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getOrganizationManager().updateConfiguration(organizationId,
                configuration, QueryOptions.empty(), normalToken1));
    }

    @Test
    public void ensureAuthOriginCannotBeRemovedTest() throws CatalogException {
        Organization organization = catalogManager.getOrganizationManager().get(organizationId, null, ownerToken).first();

        OrganizationConfiguration configuration = new OrganizationConfiguration()
                .setAuthenticationOrigins(organization.getConfiguration().getAuthenticationOrigins());
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, ParamUtils.UpdateAction.REMOVE);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        CatalogException catalogException = assertThrows(CatalogException.class, () -> catalogManager.getOrganizationManager()
                .updateConfiguration(organizationId, configuration, options, ownerToken));
        assertTrue(catalogException.getMessage().contains("user account uses"));
    }

    @Test
    public void authOriginActionTest() throws CatalogException {
        OrganizationConfiguration configuration = new OrganizationConfiguration()
                .setAuthenticationOrigins(Collections.singletonList(new AuthenticationOrigin("myId",
                        AuthenticationOrigin.AuthenticationType.SSO, null, null)));
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions()
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true)
                .append(Constants.ACTIONS, actionMap);

        OrganizationConfiguration configurationResult = catalogManager.getOrganizationManager().updateConfiguration(organizationId,
                configuration, options, ownerToken).first();
        assertEquals(2, configurationResult.getAuthenticationOrigins().size());
        for (AuthenticationOrigin authenticationOrigin : configurationResult.getAuthenticationOrigins()) {
            if (authenticationOrigin.getId().equals("myId")) {
                assertEquals(AuthenticationOrigin.AuthenticationType.SSO, authenticationOrigin.getType());
            } else {
                assertEquals(AuthenticationOrigin.AuthenticationType.OPENCGA, authenticationOrigin.getType());
            }
        }

        // Remove authOrigin
        actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, ParamUtils.UpdateAction.REMOVE);
        options.put(Constants.ACTIONS, actionMap);
        configurationResult = catalogManager.getOrganizationManager().updateConfiguration(organizationId, configuration, options,
                ownerToken).first();
        assertEquals(1, configurationResult.getAuthenticationOrigins().size());
        assertEquals(AuthenticationOrigin.AuthenticationType.OPENCGA, configurationResult.getAuthenticationOrigins().get(0).getType());

        // Set authOrigin
        List<AuthenticationOrigin> authenticationOriginList = new ArrayList<>();
        authenticationOriginList.add(new AuthenticationOrigin("myId", AuthenticationOrigin.AuthenticationType.SSO, null, null));
        authenticationOriginList.add(configurationResult.getAuthenticationOrigins().get(0));
        actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, ParamUtils.UpdateAction.SET);
        options.put(Constants.ACTIONS, actionMap);
        configuration.setAuthenticationOrigins(authenticationOriginList);
        configurationResult = catalogManager.getOrganizationManager().updateConfiguration(organizationId, configuration, options,
                ownerToken).first();
        assertEquals(2, configurationResult.getAuthenticationOrigins().size());
        for (AuthenticationOrigin authenticationOrigin : configurationResult.getAuthenticationOrigins()) {
            if (authenticationOrigin.getId().equals("myId")) {
                assertEquals(AuthenticationOrigin.AuthenticationType.SSO, authenticationOrigin.getType());
            } else {
                assertEquals(AuthenticationOrigin.AuthenticationType.OPENCGA, authenticationOrigin.getType());
            }
        }

        // Add existing authOrigin
        actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, ParamUtils.UpdateAction.ADD);
        options.put(Constants.ACTIONS, actionMap);
        CatalogException catalogException = assertThrows(CatalogException.class, () -> catalogManager.getOrganizationManager()
                .updateConfiguration(organizationId, configuration, options, ownerToken));
        assertTrue(catalogException.getMessage().contains("REPLACE"));

        // Replace existing authOrigin
        actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, ParamUtils.UpdateAction.REPLACE);
        options.put(Constants.ACTIONS, actionMap);
        configuration.setAuthenticationOrigins(Collections.singletonList(
                new AuthenticationOrigin(CatalogAuthenticationManager.OPENCGA, AuthenticationOrigin.AuthenticationType.OPENCGA, null, new ObjectMap("key", "value"))));
        configurationResult = catalogManager.getOrganizationManager().updateConfiguration(organizationId, configuration, options, ownerToken).first();
        assertEquals(2, configurationResult.getAuthenticationOrigins().size());
        for (AuthenticationOrigin authenticationOrigin : configurationResult.getAuthenticationOrigins()) {
            if (authenticationOrigin.getId().equals("myId")) {
                assertEquals(AuthenticationOrigin.AuthenticationType.SSO, authenticationOrigin.getType());
            } else {
                assertEquals(AuthenticationOrigin.AuthenticationType.OPENCGA, authenticationOrigin.getType());
                assertTrue(authenticationOrigin.getOptions().containsKey("key"));
                assertEquals("value", authenticationOrigin.getOptions().get("key"));
            }
        }
    }

    @Test
    public void tokenUpdateTest() throws CatalogException {
        TokenConfiguration tokenConfiguration = TokenConfiguration.init();
        OrganizationConfiguration configuration = new OrganizationConfiguration().setToken(tokenConfiguration);
        OrganizationConfiguration configurationResult = catalogManager.getOrganizationManager().updateConfiguration(organizationId,
                configuration, INCLUDE_RESULT, ownerToken).first();
        assertEquals(tokenConfiguration.getSecretKey(), configurationResult.getToken().getSecretKey());

        assertThrows(CatalogAuthenticationException.class, () -> catalogManager.getOrganizationManager().get(organizationId, null, ownerToken));
    }

    @Test
    public void createNewOrganizationTest() throws CatalogException {
        OpenCGAResult<Organization> result = catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId("org2"), INCLUDE_RESULT, opencgaToken);
        assertEquals(1, result.getNumInserted());
        assertEquals(1, result.getNumMatches());
        assertEquals("org2", result.first().getId());
    }

    @Test
    public void organizationInfoIncludeProjectsTest() throws CatalogException {
        Organization organization = catalogManager.getOrganizationManager().get(organizationId, QueryOptions.empty(), ownerToken).first();
        assertEquals(3, organization.getProjects().size());
        for (Project project : organization.getProjects()) {
            assertNotNull(project.getFqn());
            assertNotNull(project.getName());
            assertNotNull(project.getCreationDate());
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, "name");
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNull(organization.getProjects());

        options = new QueryOptions(QueryOptions.EXCLUDE, "projects");
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNull(organization.getProjects());

        options = new QueryOptions(QueryOptions.INCLUDE, "projects.fqn");
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertEquals(3, organization.getProjects().size());
        for (Project project : organization.getProjects()) {
            assertNotNull(project.getFqn());
            assertNull(project.getName());
            assertNull(project.getCreationDate());
        }

        options = new QueryOptions(QueryOptions.EXCLUDE, "projects.name");
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertEquals(3, organization.getProjects().size());
        for (Project project : organization.getProjects()) {
            assertNotNull(project.getFqn());
            assertNull(project.getName());
            assertNotNull(project.getCreationDate());
        }
    }

    @Test
    public void organizationInfoProjectsAuthTest() throws CatalogException {
        Organization organization = catalogManager.getOrganizationManager().get(organizationId, QueryOptions.empty(), ownerToken).first();
        assertEquals(3, organization.getProjects().size());

        organization = catalogManager.getOrganizationManager().get(organizationId, QueryOptions.empty(), orgAdminToken1).first();
        assertEquals(3, organization.getProjects().size());

        organization = catalogManager.getOrganizationManager().get(organizationId, QueryOptions.empty(), studyAdminToken1).first();
        assertEquals(2, organization.getProjects().size());
        assertArrayEquals(Arrays.asList(projectFqn1, projectFqn2).toArray(),
                organization.getProjects().stream().map(Project::getFqn).toArray());

        organization = catalogManager.getOrganizationManager().get(organizationId, QueryOptions.empty(), normalToken1).first();
        assertEquals(1, organization.getProjects().size());
        assertEquals(projectFqn1, organization.getProjects().get(0).getFqn());

        organization = catalogManager.getOrganizationManager().get(organizationId, QueryOptions.empty(), noAccessToken1).first();
        assertEquals(1, organization.getProjects().size());
        assertEquals(projectFqn1, organization.getProjects().get(0).getFqn());
    }

    @Test
    public void validateUserUpdateParamsTest() {
        OrganizationUserUpdateParams expiredDateParam = new OrganizationUserUpdateParams()
                .setAccount(new OrganizationUserUpdateParams.Account("20200101100000"));
        CatalogParameterException exception = assertThrows(CatalogParameterException.class, () -> catalogManager.getOrganizationManager()
                .updateUser(organizationId, normalUserId1, expiredDateParam, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("expired"));

        OrganizationUserUpdateParams invalidMailParam = new OrganizationUserUpdateParams()
                .setEmail("invalidEmail");
        exception = assertThrows(CatalogParameterException.class, () -> catalogManager.getOrganizationManager().updateUser(organizationId,
                normalUserId1, invalidMailParam, INCLUDE_RESULT, ownerToken));
        assertTrue(exception.getMessage().contains("not valid"));
    }

    @Test
    public void updateUserInformationTest() throws CatalogException {
        Date date = TimeUtils.getDate();
        Calendar cl = Calendar.getInstance();
        cl.setTime(date);
        cl.add(Calendar.YEAR, 1);
        String expirationTime = TimeUtils.getTime(cl.getTime());

        OrganizationUserUpdateParams userUpdateParams = new OrganizationUserUpdateParams()
                .setName("newName")
                .setEmail("mail@mail.com")
                .setAccount(new OrganizationUserUpdateParams.Account(expirationTime))
                .setQuota(new UserQuota(1000, 1000000, 1000, 1000000))
                .setAttributes(Collections.singletonMap("key1", "value1"));
        updateAndAssertChanges(organizationId, userUpdateParams, opencgaToken);

        userUpdateParams = new OrganizationUserUpdateParams()
                .setName("newName2")
                .setEmail("mai2l@mail.com")
                .setAccount(new OrganizationUserUpdateParams.Account(expirationTime))
                .setQuota(new UserQuota(1001, 1010000, 1010, 1100000))
                .setAttributes(Collections.singletonMap("key2", "value2"));
        updateAndAssertChanges(null, userUpdateParams, ownerToken);

        userUpdateParams = new OrganizationUserUpdateParams()
                .setName("newName3")
                .setEmail("mai3l@mail.com")
                .setAccount(new OrganizationUserUpdateParams.Account(expirationTime))
                .setQuota(new UserQuota(3001, 1010300, 1013, 1300000))
                .setAttributes(Collections.singletonMap("key3", "value3"));
        updateAndAssertChanges(null, userUpdateParams, orgAdminToken1);

        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getOrganizationManager().updateUser(organizationId, normalUserId1, userUpdateParams, INCLUDE_RESULT, normalToken1);
    }

    private void updateAndAssertChanges(String orgId, OrganizationUserUpdateParams userUpdateParams, String token) throws CatalogException {
        User user = catalogManager.getOrganizationManager().updateUser(orgId, normalUserId1, userUpdateParams, INCLUDE_RESULT, token).first();
        assertEquals(userUpdateParams.getName(), user.getName());
        assertEquals(userUpdateParams.getEmail(), user.getEmail());
        assertEquals(userUpdateParams.getAccount().getExpirationDate(), user.getAccount().getExpirationDate());
        assertEquals(userUpdateParams.getQuota().getCpuUsage(), user.getQuota().getCpuUsage());
        assertEquals(userUpdateParams.getQuota().getDiskUsage(), user.getQuota().getDiskUsage());
        assertEquals(userUpdateParams.getQuota().getMaxCpu(), user.getQuota().getMaxCpu());
        assertEquals(userUpdateParams.getQuota().getMaxDisk(), user.getQuota().getMaxDisk());
        for (String key : userUpdateParams.getAttributes().keySet()) {
            assertEquals(userUpdateParams.getAttributes().get(key), user.getAttributes().get(key));
        }
    }

    @Test
    public void migrationExecutionProjectionTest() throws CatalogException {
        Organization organization = catalogManager.getOrganizationManager().get(organizationId, null, ownerToken).first();
        assertNotNull(organization.getInternal());
        assertTrue(CollectionUtils.isNotEmpty(organization.getInternal().getMigrationExecutions()));

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.INTERNAL.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNotNull(organization.getInternal());
        assertTrue(CollectionUtils.isNotEmpty(organization.getInternal().getMigrationExecutions()));
        assertNull(organization.getName());

        options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.INTERNAL_MIGRATION_EXECUTIONS.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNotNull(organization.getInternal());
        assertTrue(CollectionUtils.isNotEmpty(organization.getInternal().getMigrationExecutions()));
        assertNull(organization.getName());

        options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.NAME.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNull(organization.getInternal());
        assertNotNull(organization.getName());

        options = new QueryOptions(QueryOptions.EXCLUDE, OrganizationDBAdaptor.QueryParams.INTERNAL.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNull(organization.getInternal());
        assertNotNull(organization.getName());

        options = new QueryOptions(QueryOptions.EXCLUDE, OrganizationDBAdaptor.QueryParams.INTERNAL_MIGRATION_EXECUTIONS.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNotNull(organization.getInternal());
        assertNull(organization.getInternal().getMigrationExecutions());
        assertNotNull(organization.getName());

        options = new QueryOptions(QueryOptions.EXCLUDE, OrganizationDBAdaptor.QueryParams.OWNER.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNotNull(organization.getInternal());
        assertTrue(CollectionUtils.isNotEmpty(organization.getInternal().getMigrationExecutions()));
        assertNotNull(organization.getName());
        assertNull(organization.getOwner());
    }

    @Test
    public void updateOrganizationTest() throws CatalogException {
        // Owner update
        Organization organization = catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setName("name"), INCLUDE_RESULT, ownerToken).first();
        assertEquals("name", organization.getName());

        // Admin update
        organization = catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setName("name2"), INCLUDE_RESULT, orgAdminToken1).first();
        assertEquals("name2", organization.getName());

        // Normal user update
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setName("name2"), INCLUDE_RESULT, normalToken1));

        // Admin update owner
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setOwner(normalUserId2), INCLUDE_RESULT, orgAdminToken1));
        // Admin update admins
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setAdmins(Collections.singletonList(normalUserId1)), INCLUDE_RESULT, orgAdminToken1));

        // Owner changes owner
        organization = catalogManager.getOrganizationManager().update(organizationId, new OrganizationUpdateParams().setOwner(normalUserId2),
                INCLUDE_RESULT, ownerToken).first();
        assertEquals(normalUserId2, organization.getOwner());

        QueryOptions studyOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.GROUPS.key())
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        OpenCGAResult<Study> result = catalogManager.getStudyManager().searchInOrganization(organizationId, new Query(), studyOptions,
                normalToken2);
        assertEquals(3, result.getNumResults());
        for (Study study : result.getResults()) {
            for (Group group : study.getGroups()) {
                if (ParamConstants.ADMINS_GROUP.equals(group.getId())) {
                    assertTrue(group.getUserIds().contains(normalUserId2));
                    assertFalse(group.getUserIds().contains(orgOwnerUserId));
                    assertFalse(group.getUserIds().contains(normalUserId1));
                }
            }
        }

        // Previous owner changes admins
        assertThrows(CatalogAuthorizationException.class, () -> catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setOwner(normalUserId2), INCLUDE_RESULT, ownerToken));

        // Current owner changes admins
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(OrganizationDBAdaptor.QueryParams.ADMINS.key(), ParamUtils.AddRemoveAction.ADD);
        QueryOptions options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        organization = catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setAdmins(Collections.singletonList(normalUserId1)), options, normalToken2).first();
        assertEquals(3, organization.getAdmins().size());
        assertTrue(organization.getAdmins().contains(normalUserId1));

        // Check study admins
        result = catalogManager.getStudyManager().searchInOrganization(organizationId, new Query(), studyOptions, normalToken2);
        assertEquals(3, result.getNumResults());
        for (Study study : result.getResults()) {
            for (Group group : study.getGroups()) {
                if (ParamConstants.ADMINS_GROUP.equals(group.getId())) {
                    assertTrue(group.getUserIds().contains(normalUserId1));
                }
            }
        }
    }

    @Test
    public void secretKeyIsAlwaysHiddenTest() throws CatalogException {
        Organization organization = catalogManager.getOrganizationManager().get(organizationId, null, ownerToken).first();
        assertNotNull(organization.getName());
        assertNull(organization.getConfiguration().getToken());
        assertNull(organization.getConfiguration().getAuthenticationOrigins().get(0).getOptions());

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.CONFIGURATION.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNull(organization.getName());
        assertNull(organization.getConfiguration().getToken());
        assertNull(organization.getConfiguration().getAuthenticationOrigins().get(0).getOptions());

        options = new QueryOptions(QueryOptions.EXCLUDE, OrganizationDBAdaptor.QueryParams.NAME.key());
        organization = catalogManager.getOrganizationManager().get(organizationId, options, ownerToken).first();
        assertNull(organization.getName());
        assertNull(organization.getConfiguration().getToken());
        assertNull(organization.getConfiguration().getAuthenticationOrigins().get(0).getOptions());
    }

}
