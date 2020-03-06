/*
 * Copyright 2015-2017 OpenCB
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
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;
import java.security.Key;
import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LDAPAuthenticationManager extends AuthenticationManager {

    private String originId;
    private String host;

    private String groupsSearch;
    private String usersSearch;

    private long expiration;

    private static final String OPENCGA_GECOS = "opencga_gecos";

    public LDAPAuthenticationManager(AuthenticationOrigin authenticationOrigin, String secretKeyString, long expiration) {
        super();

        this.host = authenticationOrigin.getHost();
        if (!this.host.startsWith("ldap://")) {
            this.host = "ldap://" + this.host;
        }
        this.originId = authenticationOrigin.getId();
        this.groupsSearch = String.valueOf(authenticationOrigin.getOptions().get(AuthenticationOrigin.GROUPS_SEARCH));
        this.usersSearch = String.valueOf(authenticationOrigin.getOptions().get(AuthenticationOrigin.USERS_SEARCH));

        this.expiration = expiration;

        Key secretKey = this.converStringToKeyObject(secretKeyString, SignatureAlgorithm.HS256.getJcaName());
        this.jwtManager = new JwtManager(SignatureAlgorithm.HS256.getValue(), secretKey);

        this.logger = LoggerFactory.getLogger(LDAPAuthenticationManager.class);
    }

    @Override
    public String authenticate(String username, String password) throws CatalogAuthenticationException {
        Map<String, Object> claims = new HashMap<>();

        try {
            List<Attributes> userInfoFromLDAP = LDAPUtils.getUserInfoFromLDAP(host, Arrays.asList(username), usersSearch);
            if (userInfoFromLDAP == null || userInfoFromLDAP.isEmpty()) {
                throw new CatalogAuthenticationException("The user id " + username + " could not be found in LDAP.");
            }

            String rdn = LDAPUtils.getRDN(userInfoFromLDAP.get(0));
            claims.put("opencga_gecos", rdn);

            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, host);

            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, rdn);
            env.put(Context.SECURITY_CREDENTIALS, password);

            // Create the initial context
            new InitialDirContext(env);
        } catch (NamingException e) {
            logger.error("{}", e.getMessage(), e);
            throw CatalogAuthenticationException.incorrectUserOrPassword();
        }

        return jwtManager.createJWTToken(username, claims, expiration);
    }

    @Override
    public List<User> getUsersFromRemoteGroup(String group) throws CatalogException {
        List<String> usersFromLDAP;
        try {
            usersFromLDAP = LDAPUtils.getUsersFromLDAPGroup(host, group, groupsSearch);
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
            userAttrList = LDAPUtils.getUserInfoFromLDAP(host, userStringList, usersSearch);
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
                displayName = LDAPUtils.getFullName(attrs);
                mail = LDAPUtils.getMail(attrs);
                uid = LDAPUtils.getUID(attrs);
                rdn = LDAPUtils.getRDN(attrs);
            } catch (NamingException e) {
                logger.error("Could not retrieve user information\n{}", e.getMessage(), e);
                throw new CatalogException("Could not retrieve user information\n" + e.getMessage(), e);
            }

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("LDAP_RDN", rdn);
            User user = new User(uid, displayName, mail, usersSearch, new Account().setType(Account.AccountType.GUEST)
                    .setAuthentication(new Account.AuthenticationOrigin(originId, false)), new UserInternal(new UserStatus()),
                    new UserQuota(-1, -1, -1, -1), new ArrayList<>(), new HashMap<>(), new LinkedList<>(), attributes);

            userList.add(user);
        }

        return userList;
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        // Get LDAP_RDN of the user from the token we generate
        String userRdn = (String) jwtManager.getClaim(token, OPENCGA_GECOS);
        try {
            return LDAPUtils.getGroupsFromLdapUser(host, userRdn, usersSearch);
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

}
