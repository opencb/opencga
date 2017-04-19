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

    public static DirContext getDirContext(String host) throws NamingException {
        if (dctx == null) {
            // Obtain users from external origin
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, host);

            dctx = new InitialDirContext(env);
        }

        return dctx;

    }

    public static List<String> getUsersFromLDAPGroup(DirContext dirContext, String groupName, String groupBase) throws NamingException {
        String groupFilter = "(cn=" + groupName + ")";

        String[] attributeFilter = {"uniqueMember"};
        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
        sc.setReturningAttributes(attributeFilter);
        NamingEnumeration<SearchResult> search = dirContext.search(groupBase, groupFilter, sc);

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

    public static List<Attributes> getUserInfoFromLDAP(DirContext dirContext, List<String> userList, String userBase)
            throws NamingException {
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
        NamingEnumeration<SearchResult> search = dirContext.search(userBase, userFilter, sc);

        List<Attributes> resultList = new ArrayList<>();
        while (search.hasMore()) {
            resultList.add(search.next().getAttributes());
        }

        return resultList;
    }

//    public static boolean checkUserBelongsToGroup(DirContext dirContext, String user, String group, String userBase)
//            throws NamingException {
//        String userFilter = "(&(objectclass=user)(samaccountname=+" + user + "+))";
//
////        String[] attributeFilter = {"cn", "memberOf"};
//        SearchControls sc = new SearchControls();
//        sc.setSearchScope(SearchControls.ONELEVEL_SCOPE);
////        sc.setReturningAttributes(attributeFilter);
//        NamingEnumeration<SearchResult> search = dirContext.search(userBase, userFilter, sc);
//
//        List<Attributes> resultList = new ArrayList<>();
//        while (search.hasMore()) {
//            resultList.add(search.next().getAttributes());
//        }
//
//        return true;
//    }
}
