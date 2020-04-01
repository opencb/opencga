/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.auth.authentication;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.DirectoryObject;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesPage;
import com.nimbusds.jose.Header;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.auth.authentication.azure.AuthenticationProvider;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;

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

    private final OIDCProviderMetadata oidcProviderMetadata;
    private final IGraphServiceClient graphServiceClient;
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

        // Initialise GraphServiceClient to have access to the graph API
        AuthenticationProvider provider = new AuthenticationProvider(syncClientId, syncSecretKey, tenantId);
        // Create the service client from the configuration
        graphServiceClient =  GraphServiceClient.builder().authenticationProvider(provider).buildClient();

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
    public List<User> getUsersFromRemoteGroup(String groupId) throws CatalogException {
        IDirectoryObjectCollectionWithReferencesPage membersPage;
        try {
            membersPage = graphServiceClient.groups(groupId).members().buildRequest().get();
        } catch (GraphServiceException e) {
            logger.error("Group '{}' not found.", groupId);
            throw new CatalogException("Group '" + groupId + "' not found");
        } catch (ClientException e) {
            logger.error("Graph query could not be performed: {}", e.getMessage());
            throw e;
        }

        List<com.microsoft.graph.models.extensions.User> graphUserList = new ArrayList<>();

        ObjectMapper jsonObjectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        boolean moreElems = true;
        while (membersPage.getCurrentPage() != null && moreElems) {
            for (DirectoryObject directoryObject : membersPage.getCurrentPage()) {
                com.microsoft.graph.models.extensions.User graphUser;
                if ("#microsoft.graph.user".equals(directoryObject.oDataType)) {
                    try {
                        graphUser = jsonObjectMapper.readValue(String.valueOf(directoryObject.getRawObject()),
                                com.microsoft.graph.models.extensions.User.class);
                        graphUserList.add(graphUser);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (membersPage.getNextPage() != null) {
                membersPage = membersPage.getNextPage().buildRequest().get();
            } else {
                moreElems = false;
            }
        }
        return extractUserInformation(graphUserList);
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        List<com.microsoft.graph.models.extensions.User> graphUserList = new ArrayList<>(userStringList.size());
        for (String userId : userStringList) {
            com.microsoft.graph.models.extensions.User graphUser;
            try {
                graphUser = graphServiceClient.users(userId).buildRequest().get();
            } catch (GraphServiceException e) {
                logger.error("User '{}' not found", userId);
                throw new CatalogException("User '" + userId + "' not found");
            } catch (ClientException e) {
                logger.error("Graph query could not be performed: {}", e.getMessage());
                throw e;
            }

            graphUserList.add(graphUser);
        }

        return extractUserInformation(graphUserList);
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        return jwtManager.getGroups(token, "groups", getPublicKey(token));
    }

    private List<User> extractUserInformation(List<com.microsoft.graph.models.extensions.User> graphUserList) {
        List<User> userList = new ArrayList<>(graphUserList.size());

        String name;
        String mail;
        String id;
        Map<String, Object> attributes = new HashMap<>();

        for (com.microsoft.graph.models.extensions.User graphUser : graphUserList) {
            id = graphUser.id;
            name = graphUser.displayName;
            mail = graphUser.mail;
//            if (!StringUtils.isEmpty(graphUser.mail)) {
//                mail = graphUser.mail;
//            } else {
//                mail = graphUser.userPrincipalName;
//            }

            Map<String, String> additionalProperties = new HashMap<>();
            if (graphUser.getRawObject() != null) {
                for (Map.Entry<String, JsonElement> entry : graphUser.getRawObject().entrySet()) {
                    if (!entry.getValue().isJsonNull() && entry.getValue().isJsonPrimitive()) {
                        additionalProperties.put(entry.getKey(), entry.getValue().getAsString());
                    }
                }
            }

            ObjectMap azureADMap = new ObjectMap()
                    .append("UserPrincipalName", graphUser.userPrincipalName)
                    .append("TenantId", tenantId)
                    .append("AdditionalProperties", additionalProperties);
            attributes.put("OPENCGA_REGISTRATION_TOKEN", azureADMap);

            User user = new User(id, name, mail, "", new Account().setType(Account.AccountType.GUEST).
                    setAuthentication(new Account.AuthenticationOrigin(originId, false)), new UserInternal(new UserStatus()),
                    new UserQuota(-1, -1, -1, -1), Collections.emptyList(), Collections.emptyMap(), new LinkedList<>(), attributes);

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
    public OpenCGAResult resetPassword(String userId) throws CatalogException {
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
