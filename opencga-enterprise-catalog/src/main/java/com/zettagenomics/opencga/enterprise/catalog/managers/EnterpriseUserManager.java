package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.jasig.cas.client.authentication.AttributePrincipal;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserInternal;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EnterpriseUserManager extends EnterpriseAbstractManager {

    private final QueryOptions userAccountInfoQueryOptions;

    protected static Logger logger = LoggerFactory.getLogger(EnterpriseUserManager.class);

    public EnterpriseUserManager(CatalogManager catalogManager, EnterpriseConfiguration enterpriseConfiguration,
                                 String opencgaToken) {
        super(catalogManager, enterpriseConfiguration, opencgaToken);

        this.userAccountInfoQueryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.ID.key(), UserDBAdaptor.QueryParams.INTERNAL.key(),
                        UserDBAdaptor.QueryParams.ATTRIBUTES.key()));
    }

    public String ssoLogin(AttributePrincipal principal) throws CatalogException {
        String organizationId;
        if (principal.getAttributes() != null) {
            for (Map.Entry<String, Object> entry : principal.getAttributes().entrySet()) {
                // Print user attributes
                logger.debug("{}:\t{}", entry.getKey(), entry.getValue());
            }
            organizationId = getDefaultValue(principal.getAttributes(),
                    enterpriseConfiguration.getSso().getAttributes().getOrganization(), "");
        } else {
            throw CatalogParameterException.isNull("organizationId");
        }

        String userId = principal.getName();

        if (StringUtils.isEmpty(organizationId)) {
            // Try to automatically set the organization id
            logger.debug("Organization id field is null. Fetching current organizations in installation.");
            List<String> organizationIds = catalogManager.getAdminManager().getOrganizationIds(opencgaToken);
            logger.debug("List of available organization ids '{}'.", StringUtils.join(organizationIds, "', '"));
            if (organizationIds.size() == 2) {
                organizationId = organizationIds.stream().filter(s -> !ParamConstants.ADMIN_ORGANIZATION.equals(s))
                        .findFirst().get();
            } else {
                throw CatalogParameterException.isNull("organizationId");
            }
        }

        // Get authOrigin id
        Organization organization = catalogManager.getOrganizationManager().get(organizationId,
                OrganizationManager.INCLUDE_ORGANIZATION_CONFIGURATION, opencgaToken).first();
        String authOriginId = null;
        for (AuthenticationOrigin authenticationOrigin : organization.getConfiguration().getAuthenticationOrigins()) {
            if (authenticationOrigin.getType() == AuthenticationOrigin.AuthenticationType.SSO) {
                authOriginId = authenticationOrigin.getId();
                break;
            }
        }
        if (authOriginId == null) {
            throw new CatalogException("Missing SSO authentication origin in organization '" + organizationId + "'.");
        }

        // Check user exists
        Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userId);
        OpenCGAResult<User> result = catalogManager.getAdminManager().userSearch(organizationId, query,
                userAccountInfoQueryOptions, opencgaToken);

        if (result.getNumResults() == 1) {
            // Check account
            if (!authOriginId.equals(result.first().getInternal().getAccount().getAuthentication().getId())) {
                throw new CatalogException("User '" + principal.getName() + "' was already registered from a "
                        + "different authentication origin ("
                        + result.first().getInternal().getAccount().getAuthentication().getId() + ")");
            }
        } else {
            // User does not exist
            User user = new User()
                    .setId(principal.getName())
                    .setInternal(new UserInternal().setAccount(
                            new Account(null, null, 0, new Account.AuthenticationOrigin(authOriginId, false)))
                    )
                    .setAttributes(principal.getAttributes());
            if (enterpriseConfiguration.getSso().getAttributes() != null && principal.getAttributes() != null) {
                String name = getDefaultValue(principal.getAttributes(),
                        enterpriseConfiguration.getSso().getAttributes().getName(), principal.getName());
                String surname = getDefaultValue(principal.getAttributes(),
                        enterpriseConfiguration.getSso().getAttributes().getSurname(), "");
                if (StringUtils.isNotEmpty(surname)) {
                    user.setName(name + " " + surname);
                } else {
                    user.setName(name);
                }
                user.setEmail(getDefaultValue(principal.getAttributes(),
                        enterpriseConfiguration.getSso().getAttributes().getEmail(), ""));
                user.setOrganization(getDefaultValue(principal.getAttributes(),
                        enterpriseConfiguration.getSso().getAttributes().getOrganization(), ""));
            }

            catalogManager.getUserManager().create(user, null, opencgaToken);
        }

        syncGroups(organizationId, authOriginId, principal);

        return catalogManager.getUserManager().getToken(organizationId, principal.getName(), Collections.emptyMap(),
                null, opencgaToken);
    }

    private void syncGroups(String organizationId, String authOriginId, AttributePrincipal principal)
            throws CatalogException {
        List<String> groups = getGroupsFromSSO(principal);
        catalogManager.getAdminManager().syncRemoteGroups(organizationId, principal.getName(), groups, authOriginId,
                opencgaToken);
    }

    private List<String> getGroupsFromSSO(AttributePrincipal principal) {
        if (enterpriseConfiguration.getSso() == null || enterpriseConfiguration.getSso().getAttributes() == null
                || StringUtils.isEmpty(enterpriseConfiguration.getSso().getAttributes().getGroups())) {
            logger.warn("Cannot fetch groups from SSO user '{}'. Field 'sso.attributes.groups' from the "
                    + "enterprise-configuration.yml file is undefined.", principal.getName());
            return Collections.emptyList();
        }
        if (principal.getAttributes() != null) {
            String msg = StringUtils.join(principal.getAttributes().keySet(), ",");
            logger.debug("Attribute keys: {}", msg);
        }
        String groupsKey = enterpriseConfiguration.getSso().getAttributes().getGroups();
        if (principal.getAttributes() == null || !principal.getAttributes().containsKey(groupsKey)) {
            logger.warn("No remote groups found under key '{}' for SSO user '{}'.", groupsKey, principal.getName());
            if (principal.getAttributes() == null) {
                logger.warn("Principal object has no attributes");
            }
            return Collections.emptyList();
        }
        Object o = principal.getAttributes().get(groupsKey);
        if (o instanceof List) {
            return (List<String>) o;
        } else if (o instanceof String) {
            logger.debug("Groups value is instance of String: {}.", o);
            return Arrays.asList(((String) o).split(","));
        } else {
            logger.warn("Cannot fetch groups from SSO user '{}'. Groups value is instance of '{}'.",
                    principal.getName(), o.getClass());
            return Collections.emptyList();
        }
    }

    private String getDefaultValue(Map<String, Object> attributes, String key, String defaultValue) {
        if (StringUtils.isEmpty(key)) {
            return defaultValue;
        }
        String value = String.valueOf(attributes.get(key));
        return StringUtils.isNotEmpty(value) && !"null".equals(value) ? value : defaultValue;
    }

}
