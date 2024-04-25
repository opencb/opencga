package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Optimizations;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationConfiguration;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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


        // Add new authentication origins
        configuration.setAuthenticationOrigins(Collections.singletonList(new AuthenticationOrigin("newId", AuthenticationOrigin.AuthenticationType.LDAP,
                "newHost", Collections.emptyMap())));

    }

//    @Test
//    public void avoidDuplicatedOPENCGAAuthOriginTest() throws CatalogException {
//        AuthenticationOrigin authOrigin = CatalogAuthenticationManager.createOpencgaAuthenticationOrigin();
//        AuthenticationOrigin authOrigin2 = CatalogAuthenticationManager.createOpencgaAuthenticationOrigin();
//        OrganizationUpdateParams updateParams = new OrganizationUpdateParams().setConfiguration(new OrganizationConfiguration(
//                Arrays.asList(authOrigin, authOrigin2), null, new TokenConfiguration()));
//
//        thrown.expect(CatalogException.class);
//        thrown.expectMessage("OPENCGA");
//        catalogManager.getOrganizationManager().update(organizationId, updateParams, null, ownerToken);
//    }
//
//    @Test
//    public void avoidDuplicatedAuthOriginIdTest() throws CatalogException {
//        AuthenticationOrigin authOrigin = CatalogAuthenticationManager.createOpencgaAuthenticationOrigin();
//        AuthenticationOrigin authOrigin2 = CatalogAuthenticationManager.createOpencgaAuthenticationOrigin();
//        authOrigin2.setType(AuthenticationOrigin.AuthenticationType.LDAP);
//        OrganizationUpdateParams updateParams = new OrganizationUpdateParams().setConfiguration(new OrganizationConfiguration(
//                Arrays.asList(authOrigin, authOrigin2), null, new TokenConfiguration()));
//
//        thrown.expect(CatalogException.class);
//        thrown.expectMessage("origin id");
//        catalogManager.getOrganizationManager().update(organizationId, updateParams, null, ownerToken);
//    }
//
//    @Test
//    public void updateAuthOriginTest() throws CatalogException {
//        AuthenticationOrigin authOrigin = CatalogAuthenticationManager.createOpencgaAuthenticationOrigin();
//        OrganizationUpdateParams updateParams = new OrganizationUpdateParams().setConfiguration(new OrganizationConfiguration(
//                Collections.singletonList(authOrigin), null, new TokenConfiguration()));
//
//        Organization organization = catalogManager.getOrganizationManager().update(organizationId, updateParams, INCLUDE_RESULT, ownerToken).first();
//        assertEquals(authOrigin.getId(), organization.getConfiguration().getAuthenticationOrigins().get(0).getId());
//        assertEquals(authOrigin.getType(), organization.getConfiguration().getAuthenticationOrigins().get(0).getType());
//    }

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
