package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.organizations.Organization;

public class FederationUtils {

    private static final QueryOptions INCLUDE_ORG_FEDERATION = new QueryOptions(QueryOptions.INCLUDE,
            OrganizationDBAdaptor.QueryParams.FEDERATION.key());

    public static void validateFederationId(String organizationId, String federationId, DBAdaptorFactory dbAdaptorFactory)
            throws CatalogParameterException {
        ParamUtils.checkParameter(organizationId, "organizationId");
        ParamUtils.checkParameter(federationId, "federationId");
        Organization organization = null;
        try {
            organization = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(INCLUDE_ORG_FEDERATION).first();
        } catch (CatalogDBException e) {
            throw new CatalogParameterException("Organization " + organizationId + " not found", e);
        }
        if (organization.getFederation() == null || CollectionUtils.isEmpty(organization.getFederation().getClients())
                || organization.getFederation().getClients().stream().noneMatch(f -> f.getId().equals(federationId))) {
            throw new CatalogParameterException("Organization " + organizationId + " is not federated with " + federationId);
        }
    }

}
