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

import com.sun.jndi.ldap.LdapCtxFactory;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.organizations.TokenConfiguration;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.security.Key;
import java.util.*;
import java.util.concurrent.*;

import static org.opencb.opencga.core.config.AuthenticationOrigin.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LDAPAuthenticationManager extends AuthenticationManager {

    private static final String OPENCGA_DISTINGUISHED_NAME = "opencga_dn";
    private static final String OPENCGA_REMOTE_GROUPS = "opencga_remote_groups";
    private final String originId;
    private final ExecutorService executorService;
    private final String authUserId;
    private final String authPassword;
    private final String groupsSearch;
    private final String usersSearch;
    private final String fullNameKey;
    private final String memberKey;
    private final String dnKey;
    private final String dnFormat;
    private final String uidKey;
    private final String uidFormat;
    private final String isMemberOfKey;
    private final int readTimeout;
    private final int connectTimeout;
    private final Hashtable<String, Object> env;

    private final boolean sslInvalidCertificatesAllowed;
    private String host;
    private boolean ldaps;

    public LDAPAuthenticationManager(AuthenticationOrigin authenticationOrigin, String algorithm, String secretKeyString,
                                     DBAdaptorFactory dbAdaptorFactory, long expiration) {
        super(dbAdaptorFactory, expiration);
        this.logger = LoggerFactory.getLogger(LDAPAuthenticationManager.class);
        this.host = authenticationOrigin.getHost();

        if (this.host.startsWith("ldaps://")) {  // use LDAPS if specified explicitly
            this.ldaps = true;
        } else if (!this.host.startsWith("ldap://")) {  // otherwise default to LDAP
            this.host = "ldap://" + this.host;
        }

        this.originId = authenticationOrigin.getId();

        ObjectMap authOptions = new ObjectMap(authenticationOrigin.getOptions());
        this.authUserId = takeString(authOptions, LDAP_AUTHENTICATION_USER, "");
        this.authPassword = takeString(authOptions, LDAP_AUTHENTICATION_PASSWORD, "");
        this.groupsSearch = takeString(authOptions, LDAP_GROUPS_SEARCH, "");
        this.usersSearch = takeString(authOptions, LDAP_USERS_SEARCH, "");
        this.fullNameKey = takeString(authOptions, LDAP_FULLNAME_KEY, "displayname");
        this.memberKey = takeString(authOptions, LDAP_MEMBER_KEY, "member");
        this.dnKey = takeString(authOptions, LDAP_DN_KEY, "dn");
        this.dnFormat = takeString(authOptions, LDAP_DN_FORMAT, "%s");
        this.uidKey = takeString(authOptions, LDAP_UID_KEY, "uid");
        this.uidFormat = takeString(authOptions, LDAP_UID_FORMAT, "%s");  // no formatting by default
        this.readTimeout = Integer.parseInt(takeString(authOptions, READ_TIMEOUT, String.valueOf(DEFAULT_READ_TIMEOUT)));
        this.connectTimeout = Integer.parseInt(takeString(authOptions, CONNECTION_TIMEOUT, String.valueOf(DEFAULT_CONNECTION_TIMEOUT)));
        this.sslInvalidCertificatesAllowed = Boolean.parseBoolean(takeString(authOptions, LDAP_SSL_INVALID_CERTIFICATES_ALLOWED, "false"));
        this.isMemberOfKey = takeString(authOptions, LDAP_IS_MEMBER_OF_KEY, "");

        // Every other key that is not recognized goes to the default ENV
        this.env = new Hashtable<>();
        for (String key : authOptions.keySet()) {
            // Ensure all values are strings
            env.put(key, authOptions.getString(key));
        }

        executorService = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
                .namingPattern("ldap-authentication-request-pool-%s")
                .build());

        logger.info("Init LDAP AuthenticationManager. Host: {}, env:{}", host, envToStringRedacted(getDefaultEnv()));

        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.valueOf(algorithm);
        Key secretKey = this.converStringToKeyObject(secretKeyString, signatureAlgorithm.getJcaName());
        this.jwtManager = new JwtManager(signatureAlgorithm.getValue(), secretKey);
    }

    protected static String envToStringRedacted(Hashtable<String, Object> env) {
        // Replace credentials only if exists
        Object remove = env.replace(DirContext.SECURITY_CREDENTIALS, "*********");

        String string = env.toString();

        // Restore credentials, if any
        env.replace(DirContext.SECURITY_CREDENTIALS, remove);

        return string;
    }

    public static void validateAuthenticationOriginConfiguration(AuthenticationOrigin authenticationOrigin) throws CatalogException {
        if (authenticationOrigin.getType() != AuthenticationType.LDAP) {
            throw new CatalogException("Unknown authentication type. Expected type '" + AuthenticationType.LDAP + "' but received '"
                    + authenticationOrigin.getType() + "'.");
        }
        ParamUtils.checkParameter(authenticationOrigin.getHost(), AuthenticationType.LDAP + " host.");

        TokenConfiguration defaultTokenConfig = TokenConfiguration.init();
        LDAPAuthenticationManager ldapAuthenticationManager = new LDAPAuthenticationManager(authenticationOrigin,
                defaultTokenConfig.getAlgorithm(), defaultTokenConfig.getSecretKey(), null, defaultTokenConfig.getExpiration());

        if (StringUtils.isNotEmpty(ldapAuthenticationManager.authUserId)) {
            // Service account configured: validate connectivity strictly
            DirContext dirContext = ldapAuthenticationManager.getDirContext(ldapAuthenticationManager.getDefaultEnv(), 1);
            if (dirContext == null) {
                throw new CatalogException("LDAP: Could not connect to the LDAP server using the provided configuration.");
            }
            ldapAuthenticationManager.closeDirContext(dirContext);
        } else {
            // No service account (direct bind mode): try anonymous connection, log warning if rejected
            try {
                DirContext dirContext = ldapAuthenticationManager.getDirContext(ldapAuthenticationManager.getDefaultEnv(), 1);
                if (dirContext != null) {
                    ldapAuthenticationManager.closeDirContext(dirContext);
                }
            } catch (CatalogAuthenticationException e) {
                ldapAuthenticationManager.logger.warn("LDAP: Could not validate connectivity without service account credentials. "
                        + "This is expected when using direct bind mode. Error: {}", e.getMessage());
            }
        }
        ldapAuthenticationManager.close();
    }

    @Override
    public AuthenticationResponse authenticate(String organizationId, String userId, String password)
            throws CatalogAuthenticationException {
        Map<String, Object> claims = new HashMap<>();

        if (StringUtils.isEmpty(authUserId)) {
            // Direct bind mode: construct DN from dnFormat and bind directly with user credentials
            logger.debug("Authenticating user '{}' using direct bind mode", userId);
            String userDn = String.format(dnFormat, Rdn.escapeValue(userId));
            logger.debug("Constructed user DN: '{}'", userDn);
            claims.put(OPENCGA_DISTINGUISHED_NAME, userDn);

            Hashtable<String, Object> userEnv = getEnv(userDn, password);
            DirContext userCtx = getDirContext(userEnv);
            try {
                if (StringUtils.isNotEmpty(isMemberOfKey)) {
                    List<String> groups = getGroupsFromIsMemberOf(userCtx, userDn, userId);
                    claims.put(OPENCGA_REMOTE_GROUPS, groups);
                }
            } finally {
                closeDirContext(userCtx);
            }
            logger.debug("Successfully authenticated user '{}' via direct bind", userId);
        } else {
            // Service-account mode: search for user DN via service account, then bind with user credentials
            logger.debug("Authenticating user '{}' using service-account mode", userId);
            List<Attributes> userInfoFromLDAP = getUserInfoFromLDAP(Arrays.asList(userId), usersSearch);
            if (userInfoFromLDAP.isEmpty()) {
                logger.error("User '{}' not found in LDAP under base '{}'", userId, usersSearch);
                throw new CatalogAuthenticationException("LDAP: The user id " + userId + " could not be found.");
            }

            for (Attributes attributes : userInfoFromLDAP) {
                logger.debug("User attributes: {}", attributes);
                NamingEnumeration<String> iDs = attributes.getIDs();
                try {
                    while (iDs.hasMore()) {
                        logger.debug("User id: {}", iDs.next());
                    }
                } catch (NamingException e) {
                    logger.warn(e.getMessage());
                }
            }
            String rdn = getDN(userInfoFromLDAP.get(0));
            logger.debug("Resolved DN for user '{}': '{}'", userId, rdn);
            claims.put(OPENCGA_DISTINGUISHED_NAME, rdn);

            Hashtable<String, Object> userEnv = getEnv(rdn, password);
            DirContext userCtx = getDirContext(userEnv);
            try {
                if (StringUtils.isNotEmpty(isMemberOfKey)) {
                    List<String> groups = getGroupsFromIsMemberOf(userCtx, rdn, userId);
                    claims.put(OPENCGA_REMOTE_GROUPS, groups);
                }
            } finally {
                closeDirContext(userCtx);
            }
            logger.debug("Successfully authenticated user '{}' via service-account mode", userId);
        }

        return new AuthenticationResponse(createToken(organizationId, userId, claims));
    }

    @Override
    public List<User> getUsersFromRemoteGroup(String group) throws CatalogException {
        logger.debug("Fetching users from remote LDAP group '{}' (groupsSearch: '{}')", group, groupsSearch);
        List<String> usersFromLDAP = getUsersFromLDAPGroup(group, groupsSearch);
        logger.debug("Found {} user(s) in LDAP group '{}'", usersFromLDAP.size(), group);
        return getRemoteUserInformation(usersFromLDAP);
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        logger.debug("Retrieving remote user information for {} user(s): {}", userStringList.size(), userStringList);
        List<User> userList = new ArrayList<>(userStringList.size());

        List<Attributes> userAttrList = getUserInfoFromLDAP(userStringList, usersSearch);

        if (userAttrList.isEmpty()) {
            logger.warn("No users were found in LDAP for the provided list: {}", userStringList);
            return Collections.emptyList();
        }

        // Complete basic user information
        String displayName;
        String mail;
        String uid;
        String rdn;
        for (Attributes attrs : userAttrList) {
            displayName = getFullName(attrs);
            mail = getMail(attrs);
            uid = getUID(attrs);
            rdn = getDN(attrs);

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("LDAP_RDN", rdn);
            Account account = new Account()
                    .setAuthentication(new Account.AuthenticationOrigin(originId, false));
            User user = new User(uid, displayName, mail, usersSearch, TimeUtils.getTime(), TimeUtils.getTime(),
                    new UserInternal(new UserStatus(), account),
                    new UserQuota(-1, -1, -1, -1), new HashMap<>(), new LinkedList<>(), attributes);

            userList.add(user);
        }

        logger.debug("Successfully retrieved information for {} user(s) from LDAP", userList.size());
        return userList;
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        if (StringUtils.isNotEmpty(isMemberOfKey)) {
            // Groups were stored in the token during authentication via isMemberOf attribute
            logger.debug("Retrieving remote groups from token using isMemberOf approach (key: '{}')", isMemberOfKey);
            Object claim = jwtManager.getClaim(token, OPENCGA_REMOTE_GROUPS);
            if (claim == null) {
                logger.warn("LDAP: Token does not contain '{}' claim. Token may have been issued before isMemberOf support was enabled."
                        + " Returning empty list.", OPENCGA_REMOTE_GROUPS);
                return Collections.emptyList();
            }
            if (claim instanceof List) {
                List<?> rawList = (List<?>) claim;
                List<String> groups = new ArrayList<>(rawList.size());
                for (Object element : rawList) {
                    if (element instanceof String) {
                        groups.add((String) element);
                    } else {
                        logger.warn("LDAP: Unexpected non-String element in '{}' claim (found {}). Skipping.",
                                OPENCGA_REMOTE_GROUPS, element == null ? "null" : element.getClass().getSimpleName());
                    }
                }
                logger.debug("Retrieved {} group(s) from token via isMemberOf: {}", groups.size(), groups);
                return groups;
            }
            logger.warn("LDAP: Token claim '{}' is not a List (found {}). Returning empty group list.",
                    OPENCGA_REMOTE_GROUPS, claim.getClass().getSimpleName());
            return Collections.emptyList();
        } else {
            // Search-based approach: use groupsSearch
            logger.debug("Retrieving remote groups using search-based approach (groupsSearch: '{}')", groupsSearch);
            String userRdn = (String) jwtManager.getClaim(token, OPENCGA_DISTINGUISHED_NAME);
            if (userRdn == null) {
                logger.warn("LDAP: Token does not contain '{}' claim. Cannot retrieve remote groups. Returning empty list.",
                        OPENCGA_DISTINGUISHED_NAME);
                return Collections.emptyList();
            }
            String opencgaUser = jwtManager.getUser(token);
            logger.debug("Searching LDAP groups for user '{}' (DN: '{}')", opencgaUser, userRdn);
            List<String> groups = getGroupsFromLdapUser(opencgaUser, userRdn, groupsSearch);
            logger.debug("Found {} group(s) for user '{}': {}", groups.size(), opencgaUser, groups);
            return groups;
        }
    }

    @Override
    public void changePassword(String organizationId, String userId, String oldPassword, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException("Please, contact the LDAP administrator to change the password.");
    }

    @Override
    public OpenCGAResult resetPassword(String organizationId, String userId) throws CatalogException {
        throw new UnsupportedOperationException("Please, contact the LDAP administrator to reset the password.");
    }

    @Override
    public void newPassword(String organizationId, String userId, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException("Please, contact the LDAP administrator to renew the password.");
    }

    @Override
    public String createToken(String organizationId, String userId, Map<String, Object> claims, long expiration)
            throws CatalogAuthenticationException {
        List<JwtPayload.FederationJwtPayload> federations = getFederations(organizationId, userId);
        return jwtManager.createJWTToken(organizationId, AuthenticationType.LDAP, userId, claims, federations, expiration);
    }

    @Override
    public String createNonExpiringToken(String organizationId, String userId, Map<String, Object> claims)
            throws CatalogAuthenticationException {
        List<JwtPayload.FederationJwtPayload> federations = getFederations(organizationId, userId);
        return jwtManager.createJWTToken(organizationId, AuthenticationType.LDAP, userId, claims, federations, 0L);
    }

    /* Private methods */
    private DirContext getDirContext() throws CatalogAuthenticationException {
        return getDirContext(getDefaultEnv());
    }

    private DirContext getDirContext(Hashtable<String, Object> env) throws CatalogAuthenticationException {
        return getDirContext(env, 3);
    }

    private DirContext getDirContext(Hashtable<String, Object> env, int maxAttempts) throws CatalogAuthenticationException {
        logger.debug("Opening LDAP DirContext connection to '{}' (maxAttempts: {})", host, maxAttempts);
        int count = 0;
        DirContext dctx = null;
        do {
            try {
                Future<DirContext> future = executorService.submit(() -> {
                    StopWatch stopWatch = StopWatch.createStarted();
                    DirContext thisDctx = LdapCtxFactory.getLdapCtxInstance(host, env);
                    long time = stopWatch.getTime(TimeUnit.MILLISECONDS);
                    if (time > 1000) {
                        logger.warn("Slow response from LDAP DirContext. Took {}", TimeUtils.durationToString(time));
                    }
                    return thisDctx;
                });
                dctx = future.get(readTimeout + connectTimeout, TimeUnit.MILLISECONDS);
            } catch (ExecutionException | TimeoutException e) {
                if (e instanceof ExecutionException) {
                    // Check cause
                    if (e.getCause() instanceof AuthenticationException) {
                        throw wrapException(e);
                    }
                }

                count++;
                logger.warn("Error opening DirContext connection. Attempt " + count + "/" + maxAttempts
                        + ((count == maxAttempts) ? ". Do not retry" : ". Ignore exception and retry"), e);
                if (count == maxAttempts) {
                    // After 'maxAttempts' attempts, we will raise an error.
                    throw wrapException(e);
                }
                try {
                    // Sleep 0.5 seconds
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    logger.warn("Catch interrupted exception!", e1);
                    Thread.currentThread().interrupt();
                    // Stop retrying. Leave now propagating original exception
                    throw wrapException(e);
                }
            } catch (InterruptedException e) {
                // Interrupt and propagate
                Thread.currentThread().interrupt();
                throw wrapException(e);
            }
        } while (dctx == null);

        logger.debug("Successfully opened LDAP DirContext connection to '{}'", host);
        return dctx;
    }

    private List<String> getUsersFromLDAPGroup(String groupName, String groupBase) throws CatalogException {
        Set<String> users = new HashSet<>();
        DirContext dirContext = getDirContext();

        try {
            String groupFilter = "(cn=" + groupName + ")";
            logger.debug("Searching LDAP group with filter '{}' under base '{}'", groupFilter, groupBase);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            NamingEnumeration<SearchResult> search = dirContext.search(groupBase, groupFilter, sc);

            if (!search.hasMore()) {
                logger.warn("LDAP group '{}' not found under base '{}'", groupName, groupBase);
                throw new CatalogException("Group '" + groupName + "' not found");
            }
            while (search.hasMore()) {
                SearchResult sr = search.next();
                Attributes attrs = sr.getAttributes();

                BasicAttribute members = (BasicAttribute) attrs.get(this.memberKey);
                if (members != null) {
                    NamingEnumeration<?> all = members.getAll();

                    while (all.hasMore()) {
                        String member = (String) all.next();
                        if (member.toLowerCase().startsWith("uid")) {
                            users.add(member.substring("uid".length() + 1).split(",")[0]);
                        } else if (member.toLowerCase().startsWith("cn")) {
                            String commonName = member.substring("cn".length() + 1).split(",")[0];
                            String baseDn = member.substring("cn".length() + commonName.length() + 2);

                            // Get uid
                            List<Attributes> cn = getUserInfoFromLDAP(Collections.singletonList(commonName), baseDn, "cn");
                            users.add(getUID(cn.get(0)));
                        }
                    }

                }
            }
            dirContext.close();
        } catch (NamingException | RuntimeException e) {
            logger.error("Error retrieving users from LDAP group '{}': {}", groupName, e.getMessage());
            closeDirContextAndSuppress(dirContext, e);
            throw wrapException(e, "Could not retrieve users of the group" + groupName);
        }

        logger.debug("Retrieved {} user(s) from LDAP group '{}': {}", users.size(), groupName, users);
        return new ArrayList<>(users);
    }

    private List<Attributes> getUserInfoFromLDAP(List<String> userList, String userBase) throws CatalogAuthenticationException {
        return getUserInfoFromLDAP(userList, userBase, this.uidKey);
    }

    private List<Attributes> getUserInfoFromLDAP(List<String> userList, String userBase, String key) throws CatalogAuthenticationException {
        List<Attributes> resultList = new ArrayList<>();
        DirContext dirContext = getDirContext();

        try {
            String userFilter;

            if (userList.size() == 1) {
                userFilter = "(" + key + "=" + userList.get(0) + ")";
            } else {
                userFilter = StringUtils.join(userList, ")(" + key + "=");
                userFilter = "(|(" + key + "=" + userFilter + "))";
            }

            logger.debug("Searching LDAP users with filter '{}' under base '{}'", userFilter, userBase);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            NamingEnumeration<SearchResult> search = dirContext.search(userBase, userFilter, sc);
            while (search.hasMore()) {
                SearchResult result = search.next();
                Attributes attrs = result.getAttributes();
                if (attrs.get(dnKey) == null) {
                    logger.debug("DN attribute '{}' not found in LDAP response; injecting from getNameInNamespace(): '{}'",
                            dnKey, result.getNameInNamespace());
                    attrs.put(dnKey, result.getNameInNamespace());
                }
                resultList.add(attrs);
            }
            dirContext.close();
        } catch (NamingException | RuntimeException e) {
            logger.error("Error retrieving user information from LDAP (base: '{}', users: {}): {}",
                    userBase, userList, e.getMessage());
            closeDirContextAndSuppress(dirContext, e);
            throw wrapException(e, "Could not retrieve user information");
        }

        logger.debug("LDAP user search returned {} result(s) for {} user(s)", resultList.size(), userList.size());
        return resultList;
    }

    private String getMail(Attributes attributes) throws CatalogAuthenticationException {
        return getAttribute(attributes, "mail", "");
    }

    private String getUID(Attributes attributes) throws CatalogAuthenticationException {
        String fullUid = getAttribute(attributes, uidKey);
        if (fullUid != null) {
            return String.format(uidFormat, fullUid);
        } else {
            throw new CatalogAuthenticationException("UID under '" + uidKey + "' key not found. Please, configure the proper '"
                    + LDAP_UID_KEY + "' and possibly an '" + LDAP_UID_FORMAT + "' format for your LDAP installation");
        }
    }

    private String getDN(Attributes attributes) throws CatalogAuthenticationException {
        String fullDn = getAttribute(attributes, dnKey);
        if (fullDn != null) {
            return String.format(dnFormat, fullDn);
        } else {
            throw new CatalogAuthenticationException("DN id under '" + dnKey + "' key not found. Please, configure the proper '"
                    + LDAP_DN_KEY + "' and possibly an '" + LDAP_DN_FORMAT + "' format for your LDAP installation");
        }
    }

    private String getFullName(Attributes attributes) throws CatalogAuthenticationException {
        return getAttribute(attributes, fullNameKey);
    }

    private String getAttribute(Attributes attributes, String key) throws CatalogAuthenticationException {
        return getAttribute(attributes, key, null);
    }

    private String getAttribute(Attributes attributes, String key, String defaultValue) throws CatalogAuthenticationException {
        try {
            if (attributes.get(key) == null) {
                return defaultValue;
            } else {
                String value = (String) attributes.get(key).get(0);
                if (value == null) {
                    return defaultValue;
                } else {
                    return value;
                }
            }
        } catch (NamingException e) {
            // This is actually impossible
            throw wrapException(e);
        }
    }

    private List<String> getGroupsFromLdapUser(String opencgaUser, String user, String base) throws CatalogAuthenticationException {
        List<String> resultList = new ArrayList<>();
        DirContext dirContext = getDirContext();

        try {
            String userFilter = "(" + this.memberKey + "=" + user + ")";
            logger.debug("Searching LDAP groups for user '{}' with filter '{}' under base '{}'", opencgaUser, userFilter, base);

            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sc.setReturningAttributes(new String[]{"cn"});
            NamingEnumeration<SearchResult> search = dirContext.search(base, userFilter, sc);

            while (search.hasMore()) {
                resultList.add((String) search.next().getAttributes().get("cn").get(0));
            }
            dirContext.close();
        } catch (NamingException | RuntimeException e) {
            logger.error("Error retrieving LDAP groups for user '{}' (DN: '{}', base: '{}'): {}",
                    opencgaUser, user, base, e.getMessage());
            closeDirContextAndSuppress(dirContext, e);
            throw wrapException(e, "Could not retrieve groups of user " + opencgaUser);
        }
        logger.debug("Found {} group(s) for user '{}': {}", resultList.size(), opencgaUser, resultList);
        return resultList;
    }

    private List<String> getGroupsFromIsMemberOf(DirContext ctx, String userDn, String userId) {
        logger.debug("Retrieving groups via '{}' attribute for user '{}' (DN: '{}')", isMemberOfKey, userId, userDn);
        List<String> groups = new ArrayList<>();
        NamingEnumeration<SearchResult> search = null;
        try {
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.OBJECT_SCOPE);
            sc.setReturningAttributes(new String[]{isMemberOfKey});
            search = ctx.search(userDn, "(objectClass=*)", sc);
            if (search.hasMore()) {
                Attributes attrs = search.next().getAttributes();
                Attribute memberOfAttr = attrs.get(isMemberOfKey);
                if (memberOfAttr != null) {
                    NamingEnumeration<?> values = memberOfAttr.getAll();
                    try {
                        while (values.hasMore()) {
                            Object raw = values.next();
                            if (!(raw instanceof String)) {
                                logger.warn("Unexpected type for '{}' attribute value for user {}: {}",
                                        isMemberOfKey, userId, raw == null ? "null" : raw.getClass().getSimpleName());
                                continue;
                            }
                            String groupDn = (String) raw;
                            String cn = extractCnFromDn(groupDn);
                            if (cn != null) {
                                groups.add(cn);
                            }
                        }
                    } finally {
                        values.close();
                    }
                }
            }
        } catch (NamingException e) {
            logger.warn("Could not retrieve '{}' attribute for user {}: {}", isMemberOfKey, userId, e.getMessage());
        } finally {
            if (search != null) {
                try {
                    search.close();
                } catch (NamingException e) {
                    logger.warn("Could not close LDAP search enumeration for user {}: {}", userId, e.getMessage());
                }
            }
        }
        logger.debug("Found {} group(s) via '{}' for user '{}': {}", groups.size(), isMemberOfKey, userId, groups);
        return groups;
    }

    private String extractCnFromDn(String dn) {
        try {
            LdapName ldapName = new LdapName(dn);
            for (Rdn rdn : ldapName.getRdns()) {
                if ("cn".equalsIgnoreCase(rdn.getType())) {
                    return (String) rdn.getValue();
                }
            }
        } catch (NamingException e) {
            logger.warn("Could not parse DN '{}': {}", dn, e.getMessage());
        }
        return null;
    }

    private Hashtable<String, Object> getDefaultEnv() {
        return getEnv(authUserId, authPassword);
    }

    private Hashtable<String, Object> getEnv(String user, String password) {
        Hashtable<String, Object> env = new Hashtable<>(this.env);
//        env.put(DirContext.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put("com.sun.jndi.ldap.connect.timeout", String.valueOf(connectTimeout));
        env.put("com.sun.jndi.ldap.read.timeout", String.valueOf(readTimeout));
//        env.put(DirContext.PROVIDER_URL, host);

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            env.put(DirContext.SECURITY_AUTHENTICATION, "simple");
            env.put(DirContext.SECURITY_PRINCIPAL, user);
            env.put(DirContext.SECURITY_CREDENTIALS, password);
            logger.debug("LDAP env configured with authenticated bind (principal: '{}')", user);
        } else {
            logger.debug("LDAP env configured with anonymous bind");
        }

        if (ldaps) {
            env.put(DirContext.SECURITY_PROTOCOL, "ssl");
            if (sslInvalidCertificatesAllowed) {
                env.put("java.naming.ldap.factory.socket", "org.opencb.opencga.catalog.auth.authentication.MySSLSocketFactory");
            }
        }
        return env;
    }

    private void closeDirContext(DirContext dirContext) {
        try {
            dirContext.close();
        } catch (NamingException e) {
            logger.warn("Error closing DirContext: {}", e.getMessage());
        }
    }

    private void closeDirContextAndSuppress(DirContext dirContext, Exception e) {
        try {
            dirContext.close();
        } catch (Exception ex) {
            e.addSuppressed(ex);
        }
    }

    private CatalogAuthenticationException wrapException(Exception e) {
        return wrapException(e, null);
    }

    private CatalogAuthenticationException wrapException(Exception e, String msg) {
        if (e instanceof CatalogAuthenticationException) {
            return ((CatalogAuthenticationException) e);
        }
        if (msg == null) {
            if (e instanceof ExecutionException) {
                if (e.getCause() == null) {
                    msg = e.getMessage();
                } else {
                    if (e.getCause() instanceof AuthenticationException) {
                        return CatalogAuthenticationException.incorrectUserOrPassword("LDAP", e);
                    }
                    msg = e.getCause().getMessage();
                }
            } else {
                msg = e.getMessage();
            }
        }
        logger.error("LDAP error: {}", msg, e);
        return new CatalogAuthenticationException("LDAP: " + msg, e);
    }

    /**
     * Get String from objectMap and remove.
     *
     * @param objectMap    ObjectMap
     * @param key          key
     * @param defaultValue default value
     * @return taken value, or the default value.
     */
    private String takeString(ObjectMap objectMap, String key, String defaultValue) {
        String value = objectMap.getString(key, defaultValue);
        objectMap.remove(key);
        return value;
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
