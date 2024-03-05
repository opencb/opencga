package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationConfiguration;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;

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
    public void ensureAuthOriginCannotBeRemovedTest() throws CatalogException {
        OrganizationUpdateParams updateParams = new OrganizationUpdateParams().setConfiguration(new OrganizationConfiguration(
                Collections.emptyList(), null));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("OPENCGA");
        catalogManager.getOrganizationManager().update(organizationId, updateParams, INCLUDE_RESULT, ownerToken);
    }

    @Test
    public void avoidDuplicatedOPENCGAAuthOriginTest() throws CatalogException {
        AuthenticationOrigin authOrigin = CatalogAuthenticationManager.createRandomInternalAuthenticationOrigin();
        AuthenticationOrigin authOrigin2 = CatalogAuthenticationManager.createRandomInternalAuthenticationOrigin();
        OrganizationUpdateParams updateParams = new OrganizationUpdateParams().setConfiguration(new OrganizationConfiguration(
                Arrays.asList(authOrigin, authOrigin2), null));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("OPENCGA");
        catalogManager.getOrganizationManager().update(organizationId, updateParams, null, ownerToken);
    }

    @Test
    public void avoidDuplicatedAuthOriginIdTest() throws CatalogException {
        AuthenticationOrigin authOrigin = CatalogAuthenticationManager.createRandomInternalAuthenticationOrigin();
        AuthenticationOrigin authOrigin2 = CatalogAuthenticationManager.createRandomInternalAuthenticationOrigin();
        authOrigin2.setType(AuthenticationOrigin.AuthenticationType.LDAP);
        OrganizationUpdateParams updateParams = new OrganizationUpdateParams().setConfiguration(new OrganizationConfiguration(
                Arrays.asList(authOrigin, authOrigin2), null));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("origin id");
        catalogManager.getOrganizationManager().update(organizationId, updateParams, null, ownerToken);
    }

    @Test
    public void updateAuthOriginTest() throws CatalogException {
        AuthenticationOrigin authOrigin = CatalogAuthenticationManager.createRandomInternalAuthenticationOrigin();
        OrganizationUpdateParams updateParams = new OrganizationUpdateParams().setConfiguration(new OrganizationConfiguration(
                Collections.singletonList(authOrigin), null));

        Organization organization = catalogManager.getOrganizationManager().update(organizationId, updateParams, INCLUDE_RESULT, ownerToken).first();
        assertEquals(authOrigin.getId(), organization.getConfiguration().getAuthenticationOrigins().get(0).getId());
        assertEquals(authOrigin.getType(), organization.getConfiguration().getAuthenticationOrigins().get(0).getType());
        assertEquals(authOrigin.getExpiration(), organization.getConfiguration().getAuthenticationOrigins().get(0).getExpiration());
        assertEquals(authOrigin.getSecretKey(), organization.getConfiguration().getAuthenticationOrigins().get(0).getSecretKey());
        assertEquals(authOrigin.getAlgorithm(), organization.getConfiguration().getAuthenticationOrigins().get(0).getAlgorithm());
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


}
