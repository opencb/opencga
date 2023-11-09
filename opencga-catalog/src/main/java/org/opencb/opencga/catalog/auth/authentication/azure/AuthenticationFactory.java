package org.opencb.opencga.catalog.auth.authentication.azure;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.catalog.auth.authentication.*;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.PasswordUtils;
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

public final class AuthenticationFactory {

    // Map of organizationId -> authenticationOriginId -> AuthenticationManager
    private static Map<String, Map<String, AuthenticationManager>> authenticationManagerMap;
    private static Logger logger = LoggerFactory.getLogger(AuthenticationFactory.class);

    static {
        authenticationManagerMap = new HashMap<>();
    }

    private AuthenticationFactory() {
    }

    public static void configureOrganizationAuthenticationManager(Organization organization, DBAdaptorFactory dbAdaptorFactory)
            throws CatalogException {
        // TODO: Pass proper email values
        Email email = new Email();

        Map<String, AuthenticationManager> tmpAuthenticationManagerMap = new HashMap<>();
        if (organization.getConfiguration() != null
                && CollectionUtils.isNotEmpty(organization.getConfiguration().getAuthenticationOrigins())) {
            for (AuthenticationOrigin authOrigin : organization.getConfiguration().getAuthenticationOrigins()) {
                if (authOrigin.getId() != null) {
                    switch (authOrigin.getType()) {
                        case LDAP:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(),
                                    new LDAPAuthenticationManager(authOrigin, authOrigin.getSecretKey(), authOrigin.getExpiration()));
                            break;
                        case AzureAD:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(), new AzureADAuthenticationManager(authOrigin));
                            break;
                        case OPENCGA:
                            tmpAuthenticationManagerMap.put(authOrigin.getId(),
                                    new CatalogAuthenticationManager(dbAdaptorFactory, email, authOrigin.getSecretKey(),
                                            authOrigin.getExpiration()));
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
        tmpAuthenticationManagerMap.putIfAbsent(CatalogAuthenticationManager.INTERNAL,
                new CatalogAuthenticationManager(dbAdaptorFactory, email,
                        PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH), 3600L));
        authenticationManagerMap.put(organization.getId(), tmpAuthenticationManagerMap);
    }

    public static String createToken(String organizationId, String authOriginId, String userId) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).createToken(organizationId, userId);
    }

    public static void validateToken(String organizationId, String authOriginId, String token) throws CatalogException {
        getOrganizationAuthenticationManager(organizationId, authOriginId).getUserId(token);
    }

    public static AuthenticationResponse authenticate(String organizationId, String authenticationOriginId, String userId, String password)
            throws CatalogException {
        AuthenticationManager organizationAuthenticationManager = getOrganizationAuthenticationManager(organizationId,
                authenticationOriginId);
        return organizationAuthenticationManager.authenticate(organizationId, userId, password);
    }

    public static void changePassword(String organizationId, String authOriginId, String userId, String oldPassword, String newPassword)
            throws CatalogException {
        getOrganizationAuthenticationManager(organizationId, authOriginId).changePassword(organizationId, userId, oldPassword, newPassword);
    }

    public static OpenCGAResult resetPassword(String organizationId, String authOriginId, String userId) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).resetPassword(organizationId, userId);
    }

    public static String getUserId(String organizationId, String authOriginId, String token) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getUserId(token);
    }

    public static List<User> getRemoteUserInformation(String organizationId, String authOriginId, List<String> userStringList)
            throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getRemoteUserInformation(userStringList);
    }

    public static List<User> getUsersFromRemoteGroup(String organizationId, String authOriginId, String group) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getUsersFromRemoteGroup(group);
    }

    public static List<String> getRemoteGroups(String organizationId, String authOriginId, String token) throws CatalogException {
        return getOrganizationAuthenticationManager(organizationId, authOriginId).getRemoteGroups(token);
    }

    public static Map<String, AuthenticationManager> getOrganizationAuthenticationManagers(String organizationId) throws CatalogException {
        if (!authenticationManagerMap.containsKey(organizationId)) {
            throw new CatalogException("Organization '" + organizationId + "' not found.");
        }
        return authenticationManagerMap.get(organizationId);
    }

    public static AuthenticationManager getOrganizationAuthenticationManager(String organizationId, String authOriginId)
            throws CatalogException {
        Map<String, AuthenticationManager> organizationAuthenticationManagers = getOrganizationAuthenticationManagers(organizationId);
        if (!organizationAuthenticationManagers.containsKey(authOriginId)) {
            throw new CatalogException("Authentication origin '" + authOriginId + "' for organization '" + organizationId + "' not found.");
        }
        return organizationAuthenticationManagers.get(authOriginId);
    }

    public static void clear() {
        authenticationManagerMap.clear();
    }
}
