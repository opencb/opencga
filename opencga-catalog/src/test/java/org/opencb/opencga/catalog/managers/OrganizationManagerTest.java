package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class OrganizationManagerTest extends AbstractManagerTest {

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
