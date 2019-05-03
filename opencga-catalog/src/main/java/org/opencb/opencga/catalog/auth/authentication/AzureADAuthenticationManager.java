package org.opencb.opencga.catalog.auth.authentication;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.graphrbac.ActiveDirectoryGroup;
import com.microsoft.azure.management.graphrbac.ActiveDirectoryObject;
import com.microsoft.azure.management.graphrbac.ActiveDirectoryUser;
import com.nimbusds.jose.Header;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.User;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AzureADAuthenticationManager extends AuthenticationManager {

    private String originId;

    private OIDCProviderMetadata oidcProviderMetadata;
    private String tenantId;

    private String authClientId;
    private String syncClientId;
    private String syncSecretKey;

    private Map<String, List<String>> filters;

    private Map<String, PublicKey> publicKeyMap;

    public AzureADAuthenticationManager(AuthenticationOrigin authenticationOrigin) throws CatalogException {
        super();

        this.originId = authenticationOrigin.getId();

        if (authenticationOrigin.getOptions() == null || authenticationOrigin.getOptions().isEmpty()) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'options' field.");
        }

        this.tenantId = (String) authenticationOrigin.getOptions().get("tenantId");
        if (StringUtils.isEmpty(this.tenantId)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'tenantId' option field.");
        }

        this.authClientId = (String) authenticationOrigin.getOptions().get("authClientId");
        if (StringUtils.isEmpty(this.authClientId)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'authClientId' option field.");
        }

        this.syncClientId = (String) authenticationOrigin.getOptions().get("syncClientId");
        if (StringUtils.isEmpty(this.syncClientId)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'syncClientId' option field.");
        }

        this.syncSecretKey = (String) authenticationOrigin.getOptions().get("syncSecretKey");
        if (StringUtils.isEmpty(this.syncSecretKey)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'syncSecretKey' option "
                    + "field.");
        }

        String filterString = (String) authenticationOrigin.getOptions().get("filters");
        this.filters = new HashMap<>();
        if (StringUtils.isNotEmpty(filterString)) {
            String[] filterList = filterString.split(";");
            for (String filterFieldBucket : filterList) {
                String[] split = filterFieldBucket.split("=");
                if (split.length != 2) {
                    throw new CatalogException("AzureAD authentication origin configuration error. 'filters' field could not be parsed.");
                }
                String filterKey = split[0];
                String[] allowedValues = split[1].split(",");

                filters.put(filterKey, Arrays.asList(allowedValues));
            }
        }

        if (StringUtils.isEmpty(authenticationOrigin.getHost())) {
            // Default host
            authenticationOrigin.setHost("https://login.microsoftonline.com/" + this.tenantId + "/v2.0/.well-known/openid-configuration");
        }
        try {
            this.oidcProviderMetadata = getProviderMetadata(authenticationOrigin.getHost());
        } catch (IOException | ParseException e) {
            throw new CatalogException("AzureAD authentication origin configuration error. Check 'host' field. Is it pointing to the main "
                    + "open-id configuration url? - " + e.getMessage(), e);
        }

        this.jwtManager = new JwtManager(SignatureAlgorithm.RS256.getValue());

        this.publicKeyMap = new HashMap<>();


        // Disable Azure loggers
        Logger.getLogger(AuthenticationContext.class).setLevel(Level.OFF);
        Logger.getLogger("com.microsoft.aad.adal4j.UserDiscoveryRequest").setLevel(Level.WARN);
        Logger.getLogger("com.microsoft.aad.adal4j.AuthenticationAuthority").setLevel(Level.WARN);
    }

    private OIDCProviderMetadata getProviderMetadata(String host) throws IOException, ParseException {
        URL providerConfigurationURL = new URL(host);
        InputStream stream = providerConfigurationURL.openStream();

        // Read all data from URL
        String providerInfo;
        try (java.util.Scanner s = new java.util.Scanner(stream)) {
            providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
        }

        return OIDCProviderMetadata.parse(providerInfo);
    }

    private PublicKey getPublicKey(String token) throws CatalogAuthenticationException {
        String kid;

        try {
            Header header = JWTParser.parse(token).getHeader();
            kid = String.valueOf(header.toJSONObject().get("kid"));

            if (!this.publicKeyMap.containsKey(kid)) {
                this.publicKeyMap.clear();

                logger.info("Public key not stored. Retrieving new set of public keys");

                // We look for the new public keys in the url
                URL providerConfigurationURL = this.oidcProviderMetadata.getJWKSetURI().toURL();
                InputStream stream = providerConfigurationURL.openStream();

                // Read all data from URL
                String providerInfo;
                try (java.util.Scanner s = new java.util.Scanner(stream)) {
                    providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
                }

                ObjectMap map = new ObjectMap(providerInfo);
                List keys = map.getAsList("keys");
                for (Object keyObject : keys) {
                    Map<String, Object> currentKey = (Map<String, Object>) keyObject;
                    String x5c = ((List<String>) currentKey.get("x5c")).get(0);
                    String currentKid = String.valueOf(currentKey.get("kid"));

                    InputStream in = new ByteArrayInputStream(java.util.Base64.getDecoder().decode(x5c));
                    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                    X509Certificate cert = (X509Certificate) certFactory.generateCertificate(in);

                    // And store the new keys in the map
                    this.publicKeyMap.put(currentKid, cert.getPublicKey());
                }

                logger.info("Found {} new public keys available", this.publicKeyMap.size());
            }

            if (!this.publicKeyMap.containsKey(kid)) {
                throw new CatalogAuthenticationException("Could not find public key for the token");
            }
        } catch (CertificateException | java.text.ParseException | IOException e) {
            throw new CatalogAuthenticationException("Could not get public key\n" + e.getMessage(), e);
        }

        return this.publicKeyMap.get(kid);
    }


    @Override
    public String authenticate(String username, String password) throws CatalogAuthenticationException {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(String.valueOf(this.oidcProviderMetadata.getAuthorizationEndpointURI()), false, service);
            Future<AuthenticationResult> future = context.acquireToken(authClientId, authClientId, username, password, null);
            result = future.get();
        } catch (Exception e) {
            throw CatalogAuthenticationException.incorrectUserOrPassword();
        } finally {
            service.shutdown();
        }

        if (result == null) {
            throw CatalogAuthenticationException.incorrectUserOrPassword();
        }

        if (jwtManager.passFilters(result.getAccessToken(), this.filters, getPublicKey(result.getAccessToken()))) {
            return result.getAccessToken();
        } else {
            throw CatalogAuthenticationException.userNotAllowed();
        }
    }

    @Override
    public List<User> getUsersFromRemoteGroup(String group) throws CatalogException {
        ApplicationTokenCredentials tokenCredentials = new ApplicationTokenCredentials(syncClientId, tenantId, syncSecretKey, null);
        Azure.Authenticated azureAuthenticated = Azure.authenticate(tokenCredentials);

        // This method should only be called from the admin environment to sync or obtain groups from AAD. We are going to force users to
        // pass always the group name instead of the group oid.
        /*ActiveDirectoryGroup azureADGroup = azureAuthenticated.activeDirectoryGroups().getById(group);
        if (azureADGroup == null) {
            // We try to get the group by name
            azureADGroup = azureAuthenticated.activeDirectoryGroups().getByName(group);
            if (azureADGroup == null) {
                logger.error("Group '{}' not found.");
                throw new CatalogException("Group '" + group + "' not found");
            }
        }*/
        ActiveDirectoryGroup azureADGroup = azureAuthenticated.activeDirectoryGroups().getByName(group);
        if (azureADGroup == null) {
            logger.error("Group '{}' not found.");
            throw new CatalogException("Group '" + group + "' not found");
        }

        List<ActiveDirectoryUser> azureADUserList = new ArrayList<>(azureADGroup.listMembers().size());
        for (ActiveDirectoryObject azureADUser : azureADGroup.listMembers()) {
            if (azureADUser instanceof ActiveDirectoryUser) {
                azureADUserList.add((ActiveDirectoryUser) azureADUser);
            }
        }

        return extractUserInformation(azureADUserList);
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        ApplicationTokenCredentials tokenCredentials = new ApplicationTokenCredentials(syncClientId, tenantId, syncSecretKey, null);
        Azure.Authenticated azureAuthenticated = Azure.authenticate(tokenCredentials);

        List<ActiveDirectoryUser> azureADUserList = new ArrayList<>(userStringList.size());
        for (String user : userStringList) {
            ActiveDirectoryUser azureADUser = azureAuthenticated.activeDirectoryUsers().getById(user);
            if (azureADUser == null) {
                logger.error("User '{}' not found");
                throw new CatalogException("User '" + user + "' not found");
            } else {
                azureADUserList.add(azureADUser);
            }
        }

        return extractUserInformation(azureADUserList);
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        List<String> groupOids = jwtManager.getGroups(token, "groups", getPublicKey(token));

        ApplicationTokenCredentials tokenCredentials = new ApplicationTokenCredentials(syncClientId, tenantId, syncSecretKey, null);
        Azure.Authenticated azureAuthenticated = Azure.authenticate(tokenCredentials);

        List<String> groupIds = new ArrayList<>();
        for (String groupOid : groupOids) {
            ActiveDirectoryGroup group = azureAuthenticated.activeDirectoryGroups().getById(groupOid);
            if (group == null) {
                // Try to get the group by name instead. This works as a validation that the group name exists.
                group = azureAuthenticated.activeDirectoryGroups().getByName(groupOid);
            }

            if (group != null) {
                groupIds.add(group.name());
            }
        }

        return groupIds;
    }

    private List<User> extractUserInformation(List<ActiveDirectoryUser> azureAdUserList) {
        List<User> userList = new ArrayList<>(azureAdUserList.size());

        String name;
        String mail;
        String id;
        Map<String, Object> attributes = new HashMap<>();

        for (ActiveDirectoryUser activeDirectoryUser : azureAdUserList) {
            id = activeDirectoryUser.id();
            name = activeDirectoryUser.name();
            if (!StringUtils.isEmpty(activeDirectoryUser.mail())) {
                mail = activeDirectoryUser.mail();
            } else {
                mail = activeDirectoryUser.userPrincipalName();
            }

            ObjectMap azureADMap = new ObjectMap()
                    .append("UserPrincipalName", activeDirectoryUser.userPrincipalName())
                    .append("TenantId", tenantId)
                    .append("UserType", activeDirectoryUser.inner().userType())
                    .append("AdditionalProperties", activeDirectoryUser.inner().additionalProperties());
            attributes.put("OPENCGA_REGISTRATION_TOKEN", azureADMap);

            User user = new User(id, name, mail, "", "", new Account().setType(Account.Type.GUEST).
                    setAuthentication(new Account.AuthenticationOrigin(originId, false)),
                    User.UserStatus.READY, "", -1, -1, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(),
                    attributes);

            userList.add(user);
        }

        return userList;
    }

    @Override
    public String getUserId(String token) throws CatalogException {
        return (String) jwtManager.getClaim(token, "oid", getPublicKey(token));
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult resetPassword(String userId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void newPassword(String userId, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createToken(String userId) {
        // Tokens are generated by Azure via authorization code or user-password
        throw new UnsupportedOperationException();
    }

}
