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
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
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
                Arrays.asList(UserDBAdaptor.QueryParams.ID.key(), UserDBAdaptor.QueryParams.ACCOUNT.key(),
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
        // Check user exists
        Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userId);
        OpenCGAResult<User> result = catalogManager.getAdminManager().userSearch(organizationId, query,
                userAccountInfoQueryOptions, opencgaToken);

        if (result.getNumResults() == 1) {
            // Check account
            if (!"CAS".equals(result.first().getAccount().getAuthentication().getId())) {
                throw new CatalogException("User '" + principal.getName() + "' was already registered from a "
                        + "different authentication origin (" + result.first().getAccount().getAuthentication().getId()
                        + ")");
            }
        } else {
            // User does not exist
            User user = new User()
                    .setId(principal.getName())
                    .setAccount(new Account(null, null, new Account.AuthenticationOrigin("CAS", false)))
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

            catalogManager.getUserManager().create(organizationId, user, null, opencgaToken);
        }

        syncGroups(organizationId, principal);

        return catalogManager.getUserManager().getToken(organizationId, principal.getName(), Collections.emptyMap(),
                null, opencgaToken);
    }

    private void syncGroups(String organizationId, AttributePrincipal principal) throws CatalogException {
        List<String> groups = getGroupsFromSSO(principal);
        catalogManager.getAdminManager().syncRemoteGroups(organizationId, principal.getName(), groups, "CAS",
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
