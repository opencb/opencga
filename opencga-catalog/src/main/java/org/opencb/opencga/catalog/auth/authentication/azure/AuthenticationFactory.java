package org.opencb.opencga.catalog.auth.authentication.azure;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authentication.*;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.models.federation.FederationServerParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class AuthenticationFactory {

    // Map of organizationId -> authenticationOriginId -> AuthenticationManager
    private final Map<String, Map<String, AuthenticationManager>> authenticationManagerMap;
    private final Logger logger = LoggerFactory.getLogger(AuthenticationFactory.class);
    private final DBAdaptorFactory catalogDBAdaptorFactory;
    private final Configuration configuration;

    public AuthenticationFactory(DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.configuration = configuration;
        authenticationManagerMap = new ConcurrentHashMap<>();
    }

    public void configureOrganizationAuthenticationManager(Organization organization) throws CatalogException {
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
                                    new LDAPAuthenticationManager(authOrigin, algorithm, secretKey, catalogDBAdaptorFactory, expiration));
                            break;
                        case AzureAD:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(), new AzureADAuthenticationManager(authOrigin,
                                    catalogDBAdaptorFactory));
                            break;
                        case OPENCGA:
                            CatalogAuthenticationManager catalogAuthenticationManager =
                                    new CatalogAuthenticationManager(catalogDBAdaptorFactory, configuration.getEmail(), algorithm,
                                            secretKey, expiration);
                            tmpAuthenticationManagerMap.put(CatalogAuthenticationManager.INTERNAL, catalogAuthenticationManager);
                            tmpAuthenticationManagerMap.put(CatalogAuthenticationManager.OPENCGA, catalogAuthenticationManager);
                            break;
                        case SSO:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(), new SSOAuthenticationManager(algorithm, secretKey,
                                    catalogDBAdaptorFactory, expiration));
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
        if (authenticationManagerMap.containsKey(organization.getId())) {
            for (AuthenticationManager authenticationManager : authenticationManagerMap.get(organization.getId()).values()) {
                try {
                    logger.info("Closing previous authentication manager for organization '{}'", organization.getId());
                    authenticationManager.close();
                } catch (IOException e) {
                    throw new CatalogException("Unable to close previous authentication manager for organization '" + organization.getId()
                            + "'.", e);
                }
            }
            logger.info("Reloading new set of AuthenticationManagers for organization '{}'", organization.getId());
        }
        authenticationManagerMap.put(organization.getId(), tmpAuthenticationManagerMap);
    }

    public String createToken(String organizationId, String authOriginId, String userId) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).createToken(organizationId, userId);
    }

    public void validateToken(String organizationId, Account.AuthenticationOrigin authenticationOrigin, JwtPayload jwtPayload)
            throws CatalogException {
        String securityKey = null;
        if (authenticationOrigin.isFederation()) {
            // The user is a federated user, so the token should have been encrypted using the security key
            securityKey = getFederationSecurityKey(organizationId, jwtPayload.getUserId());
        }
        getOrganizationAuthenticationManager(organizationId, authenticationOrigin.getId()).validateToken(jwtPayload.getToken(),
                securityKey);
    }

    public AuthenticationResponse authenticate(String organizationId, Account.AuthenticationOrigin authenticationOrigin, String userId,
                                               String password) throws CatalogException {
        AuthenticationManager organizationAuthenticationManager = getOrganizationAuthenticationManager(organizationId,
                authenticationOrigin.getId());
        return organizationAuthenticationManager.authenticate(organizationId, userId, password);
    }

    private String getFederationSecurityKey(String organizationId, String userId) throws CatalogException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.FEDERATION.key());
        Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(options).first();
        if (organization.getFederation() == null) {
            throw new CatalogException("Could not find federation information for federated user '" + userId + "'");
        }
        if (CollectionUtils.isNotEmpty(organization.getFederation().getServers())) {
            for (FederationServerParams server : organization.getFederation().getServers()) {
                if (server.getUserId().equals(userId)) {
                    return server.getSecurityKey();
                }
            }
        }
        if (CollectionUtils.isNotEmpty(organization.getFederation().getClients())) {
            for (FederationClientParams client : organization.getFederation().getClients()) {
                if (client.getUserId().equals(userId)) {
                    return client.getSecurityKey();
                }
            }
        }
        throw new CatalogException("Could not find federation information for federated user '" + userId + "'");
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

    public void validateAuthenticationOrigin(AuthenticationOrigin authenticationOrigin) throws CatalogException {
        ParamUtils.checkParameter(authenticationOrigin.getId(), "authentication origin id");
        ParamUtils.checkObj(authenticationOrigin.getType(), "authentication origin type");
        switch (authenticationOrigin.getType()) {
            case OPENCGA:
                CatalogAuthenticationManager.validateAuthenticationOriginConfiguration(authenticationOrigin);
                break;
            case LDAP:
                LDAPAuthenticationManager.validateAuthenticationOriginConfiguration(authenticationOrigin);
                break;
            case AzureAD:
                AzureADAuthenticationManager.validateAuthenticationOriginConfiguration(authenticationOrigin);
                break;
            case SSO:
                SSOAuthenticationManager.validateAuthenticationOriginConfiguration(authenticationOrigin);
                break;
            default:
                throw new CatalogException("Unknown authentication origin type '" + authenticationOrigin.getType() + "'");
        }
    }

}
