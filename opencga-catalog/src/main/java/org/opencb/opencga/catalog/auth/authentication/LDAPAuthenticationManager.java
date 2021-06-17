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

import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.security.Key;
import java.util.*;

import static org.opencb.opencga.core.config.AuthenticationOrigin.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LDAPAuthenticationManager extends AuthenticationManager {

    private String originId;
    private String host;

    private static DirContext dctx;

    private String authUserId;
    private String authPassword;
    private String groupsSearch;
    private String usersSearch;
    private String fullNameKey;
    private String memberKey;
    private String dnKey;
    private String dnFormat;
    private String uidKey;
    private String uidFormat;

    private long expiration;

    private static final String OPENCGA_DISTINGUISHED_NAME = "opencga_dn";
    private boolean ldaps;
    private boolean sslInvalidCertificatesAllowed;

    public LDAPAuthenticationManager(AuthenticationOrigin authenticationOrigin, String secretKeyString, long expiration) {
        super();

        this.host = authenticationOrigin.getHost();

        if (host.startsWith("ldaps://")) {  // use LDAPS if specified explicitly
            this.ldaps = true;
        } else if (!this.host.startsWith("ldap://")) {  // otherwise default to LDAP
            this.host = "ldap://" + this.host;
        }

        this.originId = authenticationOrigin.getId();

        Map<String, Object> authOptions = authenticationOrigin.getOptions();
        this.authUserId = String.valueOf(authOptions.getOrDefault(LDAP_AUTHENTICATION_USER, ""));
        this.authPassword = String.valueOf(authOptions.getOrDefault(LDAP_AUTHENTICATION_PASSWORD, ""));
        this.groupsSearch = String.valueOf(authOptions.getOrDefault(LDAP_GROUPS_SEARCH, ""));
        this.usersSearch = String.valueOf(authOptions.getOrDefault(LDAP_USERS_SEARCH, ""));
        this.fullNameKey = String.valueOf(authOptions.getOrDefault(LDAP_FULLNAME_KEY, "displayname"));
        this.memberKey = String.valueOf(authOptions.getOrDefault(LDAP_MEMBER_KEY, "member"));
        this.dnKey = String.valueOf(authOptions.getOrDefault(LDAP_DN_KEY, "dn"));
        this.dnFormat = String.valueOf(authOptions.getOrDefault(LDAP_DN_FORMAT, "%s"));
        this.uidKey = String.valueOf(authOptions.getOrDefault(LDAP_UID_KEY, "uid"));
        this.uidFormat = String.valueOf(authOptions.getOrDefault(LDAP_UID_FORMAT, "%s"));  // no formatting by default
        this.sslInvalidCertificatesAllowed = Boolean.parseBoolean(
                String.valueOf(authOptions.getOrDefault(LDAP_SSL_INVALID_CERTIFICATES_ALLOWED, false))
        );

        this.expiration = expiration;

        Key secretKey = this.converStringToKeyObject(secretKeyString, SignatureAlgorithm.HS256.getJcaName());
        this.jwtManager = new JwtManager(SignatureAlgorithm.HS256.getValue(), secretKey);

        this.logger = LoggerFactory.getLogger(LDAPAuthenticationManager.class);
    }

    @Override
    public AuthenticationResponse authenticate(String userId, String password) throws CatalogAuthenticationException {
        Map<String, Object> claims = new HashMap<>();

        try {
            List<Attributes> userInfoFromLDAP = getUserInfoFromLDAP(host, Arrays.asList(userId), usersSearch);
            if (userInfoFromLDAP.isEmpty()) {
                throw new CatalogAuthenticationException("The user id " + userId + " could not be found in LDAP.");
            }

            String rdn = getDN(userInfoFromLDAP.get(0));
            claims.put(OPENCGA_DISTINGUISHED_NAME, rdn);

            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, host);

            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_CREDENTIALS, password);

            if (ldaps) {
//                String uid = userInfoFromLDAP.get(0).get(dnKey).get(0).toString();
//                String dn = String.format(dnFormat, uid);
                if (sslInvalidCertificatesAllowed) {
                    env.put("java.naming.ldap.factory.socket", "org.opencb.opencga.catalog.auth.authentication.MySSLSocketFactory");
                }
                env.put(Context.SECURITY_PRINCIPAL, rdn);
                env.put(Context.SECURITY_PROTOCOL, "ssl");
            } else {
                env.put(Context.SECURITY_PRINCIPAL, rdn);
            }

            // Create the initial context
            new InitialDirContext(env);
        } catch (NamingException e) {
            logger.error("{}", e.getMessage(), e);
            throw CatalogAuthenticationException.incorrectUserOrPassword();
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
        List<String> usersFromLDAP;
        try {
            usersFromLDAP = getUsersFromLDAPGroup(host, group, groupsSearch);
        } catch (NamingException e) {
            logger.error("Could not retrieve users of the group {}\n{}", group, e.getMessage(), e);
            throw new CatalogException("Could not retrieve users of the group " + group + "\n" + e.getMessage(), e);
        }

        return getRemoteUserInformation(usersFromLDAP);
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        List<User> userList = new ArrayList<>(userStringList.size());

        List<Attributes> userAttrList;
        try {
            userAttrList = getUserInfoFromLDAP(host, userStringList, usersSearch);
        } catch (NamingException e) {
            logger.error("Could not retrieve user information {}\n{}", e.getMessage(), e);
            throw new CatalogException("Could not retrieve user information\n" + e.getMessage(), e);
        }

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
            try {
                displayName = getFullName(attrs, fullNameKey);
                mail = getMail(attrs);
                uid = getUID(attrs);
                rdn = getDN(attrs);
            } catch (NamingException e) {
                logger.error("Could not retrieve user information\n{}", e.getMessage(), e);
                throw new CatalogException("Could not retrieve user information\n" + e.getMessage(), e);
            }

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
        try {
            return getGroupsFromLdapUser(host, userRdn, usersSearch);
        } catch (NamingException e) {
            String user = jwtManager.getUser(token);
            throw new CatalogException("Could not retrieve groups of user " + user + "\n" + e.getMessage(), e);
        }
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
        return jwtManager.createJWTToken(userId, expiration);
    }

    /* Private methods */
    private DirContext getDirContext(String host) throws NamingException {
        int count = 0;
        if (dctx == null || !isConnectionAlive()) {
            // Obtain users from external origin
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, host);

            if (StringUtils.isNotEmpty(authUserId) && StringUtils.isNotEmpty(authPassword)) {
                env.put(DirContext.SECURITY_AUTHENTICATION, "simple");
                env.put(DirContext.SECURITY_PRINCIPAL, authUserId);
                env.put(DirContext.SECURITY_CREDENTIALS, authPassword);
            }

            if (ldaps && sslInvalidCertificatesAllowed) {
                env.put("java.naming.ldap.factory.socket", "org.opencb.opencga.catalog.auth.authentication.MySSLSocketFactory");
            }

            // Specify timeout to be 0.5 seconds
            env.put("com.sun.jndi.ldap.connect.timeout", "500");

            dctx = null;
            while (dctx == null) {
                try {
                    dctx = new InitialDirContext(env);
                } catch (NamingException e) {
                    if (count == 3) {
                        // After 3 attempts, we will raise an error.
                        throw e;
                    }
                    count++;
                    try {
                        // Sleep 0.5 seconds
                        Thread.sleep(500);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }

        return dctx;
    }

    private static boolean isConnectionAlive() {
        try {
            dctx.getAttributes("");
            return true;
        } catch (NamingException e) {
            return false;
        }
    }

    private List<String> getUsersFromLDAPGroup(String host, String groupName, String groupBase) throws NamingException, CatalogException {
        String groupFilter = "(cn=" + groupName + ")";

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> search = getDirContext(host).search(groupBase, groupFilter, sc);

        Set<String> users = new HashSet<>();
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
                        List<Attributes> cn = getUserInfoFromLDAP(host, Collections.singletonList(commonName), baseDn, "cn");
                        System.out.println(commonName);
                        users.add(getUID(cn.get(0)));
                    }
                }

            }
        }

        return new ArrayList<>(users);
    }

    private List<Attributes> getUserInfoFromLDAP(String host, List<String> userList, String userBase) throws NamingException {
        return getUserInfoFromLDAP(host, userList, userBase, this.uidKey);
    }

    private List<Attributes> getUserInfoFromLDAP(String host, List<String> userList, String userBase, String key) throws NamingException {
        String userFilter;

        if (userList.size() == 1) {
            userFilter = "(" + key + "=" + userList.get(0) + ")";
        } else {
            userFilter = StringUtils.join(userList, ")(" + key + "=");
            userFilter = "(|(" + key + "=" + userFilter + "))";
        }

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> search = getDirContext(host).search(userBase, userFilter, sc);

        List<Attributes> resultList = new ArrayList<>();
        while (search.hasMore()) {
            resultList.add(search.next().getAttributes());
        }

        return resultList;
    }

    private String getMail(Attributes attributes) throws NamingException {
        return attributes.get("mail") != null ? (String) attributes.get("mail").get(0) : "";
    }

    private String getUID(Attributes attributes) throws NamingException {
        if (attributes.get(uidKey) != null) {
            String fullUid = (String) attributes.get(uidKey).get(0);
            return String.format(uidFormat, fullUid);
        } else {
            throw new NamingException("UID under '" + uidKey + "' key not found. Please, configure the proper '" + LDAP_UID_KEY
                    + "' and possibly an '" + LDAP_UID_FORMAT + "' format for your LDAP installation");
        }
    }

    private String getDN(Attributes attributes) throws NamingException {
        if (attributes.get(dnKey) != null) {
            String fullDn = (String) attributes.get(dnKey).get(0);
            return String.format(dnFormat, fullDn);
        } else {
            throw new NamingException("DN id under '" + dnKey + "' key not found. Please, configure the proper '" + LDAP_DN_KEY
                    + "' and possibly an '" + LDAP_DN_FORMAT + "' format for your LDAP installation");
        }
    }

    private String getFullName(Attributes attributes, String fullNameKey) throws NamingException {
        return (String) attributes.get(fullNameKey).get(0);
    }

    private List<String> getGroupsFromLdapUser(String host, String user, String base) throws NamingException {
        String userFilter = "(" + this.memberKey + "=" + user + ")";

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
        sc.setReturningAttributes(new String[]{"cn"});
        NamingEnumeration<SearchResult> search = getDirContext(host).search(base, userFilter, sc);

        List<String> resultList = new ArrayList<>();
        while (search.hasMore()) {
            resultList.add((String) search.next().getAttributes().get("cn").get(0));
        }
        return resultList;
    }

}
