package org.opencb.opencga.catalog.auth.authentication.azure;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authentication.*;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Email;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class AuthenticationFactory {

    // Map of organizationId -> authenticationOriginId -> AuthenticationManager
    private final Map<String, Map<String, AuthenticationManager>> authenticationManagerMap;
    private final Logger logger = LoggerFactory.getLogger(AuthenticationFactory.class);
    private final DBAdaptorFactory catalogDBAdaptorFactory;

    public AuthenticationFactory(DBAdaptorFactory catalogDBAdaptorFactory) {
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        authenticationManagerMap = new ConcurrentHashMap<>();
    }

    public void configureOrganizationAuthenticationManager(Organization organization) throws CatalogException {
        // TODO: Pass proper email values
        Email email = new Email();

        Map<String, AuthenticationManager> tmpAuthenticationManagerMap = new HashMap<>();

        long expiration = organization.getConfiguration().getToken().getExpiration();
        String algorithm = organization.getConfiguration().getToken().getAlgorithm();
        String secretKey = organization.getConfiguration().getToken().getSecretKey();

        if (organization.getConfiguration() != null
                && CollectionUtils.isNotEmpty(organization.getConfiguration().getAuthenticationOrigins())) {
            for (AuthenticationOrigin authOrigin : organization.getConfiguration().getAuthenticationOrigins()) {
                if (authOrigin.getId() != null) {
                    switch (authOrigin.getType()) {
                        case LDAP:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(),
                                    new LDAPAuthenticationManager(authOrigin, algorithm, secretKey, expiration));
                            break;
                        case AzureAD:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(), new AzureADAuthenticationManager(authOrigin));
                            break;
                        case OPENCGA:
                            CatalogAuthenticationManager catalogAuthenticationManager =
                                    new CatalogAuthenticationManager(catalogDBAdaptorFactory, email, algorithm, secretKey, expiration);
                            tmpAuthenticationManagerMap.put(CatalogAuthenticationManager.INTERNAL, catalogAuthenticationManager);
                            tmpAuthenticationManagerMap.put(CatalogAuthenticationManager.OPENCGA, catalogAuthenticationManager);
                            break;
                        case SSO:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(), new SSOAuthenticationManager(algorithm, secretKey,
                                    expiration));
                            break;
                        default:
                            logger.warn("Unexpected authentication origin type '{}' for id '{}' found in organization '{}'. "
                                            + "Authentication origin will be ignored.", authOrigin.getType(), organization.getId(),
                                    authOrigin.getId());
                            break;
                    }
                }
            }
        }
        if (tmpAuthenticationManagerMap.isEmpty()) {
            throw new CatalogException("No authentication origin found for organization '" + organization.getId() + "'");
        }
        authenticationManagerMap.put(organization.getId(), tmpAuthenticationManagerMap);
    }

    public String createToken(String organizationId, String authOriginId, String userId) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).createToken(organizationId, userId);
    }

    public void validateToken(String organizationId, String authOriginId, String token) throws CatalogException {
        getOrganizationAuthenticationManager(organizationId, authOriginId).getUserId(token);
    }

    public AuthenticationResponse authenticate(String organizationId, String authenticationOriginId, String userId, String password)
            throws CatalogException {
        AuthenticationManager organizationAuthenticationManager = getOrganizationAuthenticationManager(organizationId,
                authenticationOriginId);
        return organizationAuthenticationManager.authenticate(organizationId, userId, password);
    }

    public void changePassword(String organizationId, String authOriginId, String userId, String oldPassword, String newPassword)
            throws CatalogException {
        getOrganizationAuthenticationManager(organizationId, authOriginId).changePassword(organizationId, userId, oldPassword, newPassword);
    }

    public OpenCGAResult resetPassword(String organizationId, String authOriginId, String userId) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).resetPassword(organizationId, userId);
    }

    public String getUserId(String organizationId, String authOriginId, String token) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getUserId(token);
    }

    public List<User> getRemoteUserInformation(String organizationId, String authOriginId, List<String> userStringList)
            throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getRemoteUserInformation(userStringList);
    }

    public List<User> getUsersFromRemoteGroup(String organizationId, String authOriginId, String group) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getUsersFromRemoteGroup(group);
    }

    public List<String> getRemoteGroups(String organizationId, String authOriginId, String token) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getRemoteGroups(token);
    }

    public Map<String, AuthenticationManager> getOrganizationAuthenticationManagers(String organizationId) throws CatalogException {
        if (!authenticationManagerMap.containsKey(organizationId)) {
            // Check if the organization exists (it must have been created on a different instance)
            for (String id : catalogDBAdaptorFactory.getOrganizationIds()) {
                if (id.equals(organizationId)) {
                    QueryOptions options = new QueryOptions(OrganizationManager.INCLUDE_ORGANIZATION_CONFIGURATION);
                    options.put(OrganizationDBAdaptor.IS_ORGANIZATION_ADMIN_OPTION, true);
                    Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(options)
                            .first();
                    configureOrganizationAuthenticationManager(organization);
                    return authenticationManagerMap.get(organizationId);
                }
            }
            throw new CatalogException("Organization '" + organizationId + "' not found.");
        }
        return authenticationManagerMap.get(organizationId);
    }

    public AuthenticationManager getOrganizationAuthenticationManager(String organizationId, String authOriginId)
            throws CatalogException {
        Map<String, AuthenticationManager> organizationAuthenticationManagers = getOrganizationAuthenticationManagers(organizationId);
        if (!organizationAuthenticationManagers.containsKey(authOriginId)) {
            throw new CatalogException("Authentication origin '" + authOriginId + "' for organization '" + organizationId + "' not found.");
        }
        return organizationAuthenticationManagers.get(authOriginId);
    }

}
