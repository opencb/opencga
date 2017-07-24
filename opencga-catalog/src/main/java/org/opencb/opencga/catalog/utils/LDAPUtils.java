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

package org.opencb.opencga.catalog.utils;

import org.apache.commons.lang3.StringUtils;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Created by pfurio on 11/04/17.
 */
public class LDAPUtils {

    private static DirContext dctx;

    private static DirContext getDirContext(String host) throws NamingException {
        if (dctx == null) {
            // Obtain users from external origin
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, host);

            dctx = new InitialDirContext(env);
        }

        return dctx;

    }

    public static List<String> getUsersFromLDAPGroup(String host, String groupName, String groupBase) throws NamingException {


        String groupFilter = "(cn=" + groupName + ")";

        String[] attributeFilter = {"uniqueMember"};
        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
        sc.setReturningAttributes(attributeFilter);
        NamingEnumeration<SearchResult> search = getDirContext(host).search(groupBase, groupFilter, sc);

        List<String> users = new ArrayList<>();
        while (search.hasMore()) {
            SearchResult sr = search.next();
            Attributes attrs = sr.getAttributes();

            BasicAttribute uniquemember = (BasicAttribute) attrs.get("uniqueMember");
            NamingEnumeration<?> all = uniquemember.getAll();

            while (all.hasMore()) {
                String next = (String) all.next();
                for (String s : next.split(",")) {
                    if (s.contains("uid")) {
                        users.add(s.split("=")[1]);
                        continue;
                    }
                }
            }
        }

        return users;
    }

    public static List<Attributes> getUserInfoFromLDAP(String host, List<String> userList, String userBase) throws NamingException {
        String userFilter;

        if (userList.size() == 1) {
            userFilter = "(uid=" + userList.get(0) + ")";
        } else {
            userFilter = StringUtils.join(userList, ")(uid=");
            userFilter = "(|(uid=" + userFilter + "))";
        }

//        String[] attributeFilter = {"displayname", "mail", "uid", "gecos"};
        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
//        sc.setReturningAttributes(attributeFilter);
        NamingEnumeration<SearchResult> search = getDirContext(host).search(userBase, userFilter, sc);

        List<Attributes> resultList = new ArrayList<>();
        while (search.hasMore()) {
            resultList.add(search.next().getAttributes());
        }

        return resultList;
    }

    public static String getMail(Attributes attributes) throws NamingException {
        return (String) attributes.get("mail").get(0);
    }

    public static String getUID(Attributes attributes) throws NamingException {
        return (String) attributes.get("uid").get(0);
    }

    public static String getRDN(Attributes attributes) throws NamingException {
        return (String) attributes.get("gecos").get(0);
    }

    public static String getFullName(Attributes attributes) throws NamingException {
        return (String) attributes.get("displayname").get(0);
    }

    public static List<String> getGroupsFromLdapUser(String host, String user, String base) throws NamingException {
        String userFilter = "(uniqueMember=" + user + ")";

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
