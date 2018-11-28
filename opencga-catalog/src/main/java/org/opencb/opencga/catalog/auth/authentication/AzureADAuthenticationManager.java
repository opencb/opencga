package org.opencb.opencga.catalog.auth.authentication;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.nimbusds.jose.Header;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.User;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AzureADAuthenticationManager extends AuthenticationManager {

    private OIDCProviderMetadata oidcProviderMetadata;
    private String clientId;
    private Map<String, List<String>> filters;

    private String idMapping;
    private String groupMapping;

    private Map<String, PublicKey> publicKeyMap;

    public AzureADAuthenticationManager(String host, Map<String, String> options, Configuration configuration) throws CatalogException {
        super();
        init(host, options);
    }

    private void init(String host, Map<String, String> options) throws CatalogException {
        if (StringUtils.isEmpty(host)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'host' field.");
        }
        try {
            this.oidcProviderMetadata = getProviderMetadata(host);
        } catch (IOException | ParseException e) {
            throw new CatalogException("AzureAD authentication origin configuration error. Check 'host' field. Is it pointing to the main "
                    + "open-id configuration url? - " + e.getMessage(), e);
        }

        if (options == null || options.isEmpty()) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'options' field.");
        }

        this.clientId = options.get("clientId");
        if (StringUtils.isEmpty(this.clientId)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'clientId' option field.");
        }

        String filterString = options.get("filters");
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

        String mappingString = options.get("mappings");
        if (StringUtils.isNotEmpty(mappingString)) {
            String[] mappingList = mappingString.split(";");
            for (String mappingBucket : mappingList) {
                String[] split = mappingBucket.split("=");
                if (split.length != 2) {
                    throw new CatalogException("AzureAD authentication origin configuration error. 'mappings' field could not be parsed.");
                }
                if ("id".equals(split[0])) {
                    this.idMapping = split[1];
                } else if ("groups".equals(split[0])) {
                    this.groupMapping = split[1];
                } else {
                    throw new CatalogException("AzureAD authentication origin configuration error. Unexpected '" + split[0] + "' key found"
                            + " in the 'mappings' field. Expected keys are 'id' and 'groups'");
                }
            }
        } else {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'mappings' option field.");
        }

        if (StringUtils.isEmpty(this.idMapping)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'id' key from the 'mappings' "
                    + "field in 'options'.");
        }
        if (StringUtils.isEmpty(this.groupMapping)) {
            throw new CatalogException("AzureAD authentication origin configuration error. Missing mandatory 'groups' key from the "
                    + "'mappings' field in 'options'.");
        }

        this.publicKeyMap = new HashMap<>();
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

    private PublicKey getPublicKey(String token) throws IOException, java.text.ParseException, CertificateException, CatalogException {
        Header header = JWTParser.parse(token).getHeader();
        String kid = String.valueOf(header.toJSONObject().get("kid"));

        if (!this.publicKeyMap.containsKey(kid)) {
            this.publicKeyMap.clear();

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

                InputStream in = new ByteArrayInputStream(java.util.Base64.getDecoder().decode(x5c));
                CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                X509Certificate cert = (X509Certificate)certFactory.generateCertificate(in);

                // And store the new keys in the map
                this.publicKeyMap.put(kid, cert.getPublicKey());
            }
        }

        if (!this.publicKeyMap.containsKey(kid)) {
            throw new CatalogException("Could not find public key for the token");
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
            Future<AuthenticationResult> future = context.acquireToken(clientId, clientId, username, password, null);
            result = future.get();
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            throw CatalogAuthenticationException.incorrectUserOrPassword();
        } finally {
            service.shutdown();
        }

        if (result == null) {
            throw CatalogAuthenticationException.incorrectUserOrPassword();
        }

        if (jwtManager.passFilters(result.getAccessToken(), this.filters)) {
            return result.getAccessToken();
        } else {
            throw CatalogAuthenticationException.userNotAllowed();
        }
    }

    @Override
    public List<User> getUsersFromRemoteGroup(String group) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        return jwtManager.getGroups(token, groupMapping);
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
