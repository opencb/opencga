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
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.security.Key;
import java.util.*;
import java.util.concurrent.*;

import static org.opencb.opencga.core.config.AuthenticationOrigin.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LDAPAuthenticationManager extends AuthenticationManager {

    private static final String OPENCGA_DISTINGUISHED_NAME = "opencga_dn";
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
    private final int readTimeout;
    private final int connectTimeout;
    private final Hashtable<String, Object> env;

    private final long expiration;
    private final boolean sslInvalidCertificatesAllowed;
    private String host;
    private boolean ldaps;

    public LDAPAuthenticationManager(AuthenticationOrigin authenticationOrigin, String secretKeyString, long expiration) {
        super();
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

        this.expiration = expiration;
        Key secretKey = this.converStringToKeyObject(secretKeyString, SignatureAlgorithm.HS256.getJcaName());
        this.jwtManager = new JwtManager(SignatureAlgorithm.HS256.getValue(), secretKey);
    }

    protected static String envToStringRedacted(Hashtable<String, Object> env) {
        // Replace credentials only if exists
        Object remove = env.replace(DirContext.SECURITY_CREDENTIALS, "*********");

        String string = env.toString();

        // Restore credentials, if any
        env.replace(DirContext.SECURITY_CREDENTIALS, remove);

        return string;
    }

    @Override
    public AuthenticationResponse authenticate(String userId, String password) throws CatalogAuthenticationException {
        Map<String, Object> claims = new HashMap<>();

        List<Attributes> userInfoFromLDAP = getUserInfoFromLDAP(Arrays.asList(userId), usersSearch);
        if (userInfoFromLDAP.isEmpty()) {
            throw new CatalogAuthenticationException("LDAP: The user id " + userId + " could not be found.");
        }

        String rdn = getDN(userInfoFromLDAP.get(0));
        claims.put(OPENCGA_DISTINGUISHED_NAME, rdn);

        // Attempt to authenticate
        Hashtable<String, Object> env = getEnv(rdn, password);
        try {
            getDirContext(env).close();
        } catch (NamingException e) {
            throw wrapException(e);
        }

        return new AuthenticationResponse(jwtManager.createJWTToken(userId, claims, expiration));
    }

    @Override
    public AuthenticationResponse refreshToken(String refreshToken) throws CatalogAuthenticationException {
        String userId = getUserId(refreshToken);
        if (!"*".equals(userId)) {
            return new AuthenticationResponse(createToken(userId));
        } else {
            throw new CatalogAuthenticationException("Cannot refresh token for '*'");
        }
    }

    @Override
    public List<User> getUsersFromRemoteGroup(String group) throws CatalogException {
        List<String> usersFromLDAP = getUsersFromLDAPGroup(group, groupsSearch);
        return getRemoteUserInformation(usersFromLDAP);
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        List<User> userList = new ArrayList<>(userStringList.size());

        List<Attributes> userAttrList = getUserInfoFromLDAP(userStringList, usersSearch);

        if (userAttrList.isEmpty()) {
            logger.warn("No users were found. Nothing to do.");
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
            User user = new User(uid, displayName, mail, usersSearch, new Account().setType(Account.AccountType.GUEST)
                    .setAuthentication(new Account.AuthenticationOrigin(originId, false)), new UserInternal(new UserStatus()),
                    new UserQuota(-1, -1, -1, -1), new ArrayList<>(), new ArrayList<>(), new HashMap<>(), new LinkedList<>(), attributes);

            userList.add(user);
        }

        return userList;
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        // Get LDAP_RDN of the user from the token we generate
        String userRdn = (String) jwtManager.getClaim(token, OPENCGA_DISTINGUISHED_NAME);
        String opencgaUser = jwtManager.getUser(token);
        return getGroupsFromLdapUser(opencgaUser, userRdn, usersSearch);
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException("Please, contact the LDAP administrator to change the password.");
    }

    @Override
    public OpenCGAResult resetPassword(String userId) throws CatalogException {
        throw new UnsupportedOperationException("Please, contact the LDAP administrator to reset the password.");
    }

    @Override
    public void newPassword(String userId, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException("Please, contact the LDAP administrator to renew the password.");
    }

    @Override
    public String createToken(String userId) {
        return jwtManager.createJWTToken(userId, expiration);
    }

    /* Private methods */
    private DirContext getDirContext() throws CatalogAuthenticationException {
        return getDirContext(getDefaultEnv());
    }

    private DirContext getDirContext(Hashtable<String, Object> env) throws CatalogAuthenticationException {
        return getDirContext(env, 3);
    }

    private DirContext getDirContext(Hashtable<String, Object> env, int maxAttempts) throws CatalogAuthenticationException {
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

        return dctx;
    }

    private List<String> getUsersFromLDAPGroup(String groupName, String groupBase) throws CatalogException {
        Set<String> users = new HashSet<>();
        DirContext dirContext = getDirContext();

        try {
            String groupFilter = "(cn=" + groupName + ")";
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            NamingEnumeration<SearchResult> search = dirContext.search(groupBase, groupFilter, sc);

            if (!search.hasMore()) {
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
            closeDirContextAndSuppress(dirContext, e);
            throw wrapException(e, "Could not retrieve users of the group" + groupName);
        }

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

            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            NamingEnumeration<SearchResult> search = dirContext.search(userBase, userFilter, sc);
            while (search.hasMore()) {
                resultList.add(search.next().getAttributes());
            }
            dirContext.close();
        } catch (NamingException | RuntimeException e) {
            closeDirContextAndSuppress(dirContext, e);
            throw wrapException(e, "Could not retrieve user information");
        }

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

            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sc.setReturningAttributes(new String[]{"cn"});
            NamingEnumeration<SearchResult> search = dirContext.search(base, userFilter, sc);

            while (search.hasMore()) {
                resultList.add((String) search.next().getAttributes().get("cn").get(0));
            }
            dirContext.close();
        } catch (NamingException | RuntimeException e) {
            closeDirContextAndSuppress(dirContext, e);
            throw wrapException(e, "Could not retrieve groups of user " + opencgaUser);
        }
        return resultList;
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
        }

        if (ldaps) {
            env.put(DirContext.SECURITY_PROTOCOL, "ssl");
            if (sslInvalidCertificatesAllowed) {
                env.put("java.naming.ldap.factory.socket", "org.opencb.opencga.catalog.auth.authentication.MySSLSocketFactory");
            }
        }
        return env;
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
}
