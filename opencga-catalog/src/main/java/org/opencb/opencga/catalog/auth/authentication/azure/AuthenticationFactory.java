package org.opencb.opencga.catalog.auth.authentication.azure;

import org.opencb.opencga.catalog.auth.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.user.AuthenticationResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AuthenticationFactory {

    // Map of organizationId -> authenticationOriginId -> AuthenticationManager
    private Map<String, Map<String, AuthenticationManager>> authenticationManagerMap;

    public AuthenticationFactory(List<Organization> organizationList) {
        this.authenticationManagerMap = new HashMap<>();
        if (organizationList != null) {
            for (Organization organization : organizationList) {
                configureOrganizationAuthenticationManager(organization);
            }
        }
    }

    public void configureOrganizationAuthenticationManager(Organization organization) {
        // TODO: Loop over all organizations and set the necessary AuthenticationManagers
    }

    public void validateToken(String organizationId, String authOriginId, String token) throws CatalogException {
        getOrganizationAuthenticationManager(organizationId, authOriginId).getUserId(token);
    }

    public AuthenticationResponse authenticate(String organizationId, String authenticationOriginId, String userId, String password)
            throws CatalogException {
        AuthenticationManager organizationAuthenticationManager = getOrganizationAuthenticationManager(organizationId,
                authenticationOriginId);
        return organizationAuthenticationManager.authenticate(userId, password);
    }

    private Map<String, AuthenticationManager> getOrganizationAuthenticationManagers(String organizationId) throws CatalogException {
        if (!authenticationManagerMap.containsKey(organizationId)) {
            throw new CatalogException("Organization '" + organizationId + "' not found.");
        }
        return authenticationManagerMap.get(organizationId);
    }

    private AuthenticationManager getOrganizationAuthenticationManager(String organizationId, String authOriginId) throws CatalogException {
        Map<String, AuthenticationManager> organizationAuthenticationManagers = getOrganizationAuthenticationManagers(organizationId);
        if (!organizationAuthenticationManagers.containsKey(authOriginId)) {
            throw new CatalogException("Authentication origin '" + authOriginId + "' for organization '" + organizationId + "' not found.");
        }
        return organizationAuthenticationManagers.get(authOriginId);
    }

}
