package org.opencb.opencga.catalog.utils;

import org.junit.Ignore;
import org.junit.Test;

import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 11/04/17.
 */
// Ignore because these tests can only be run if there is an LDAP connection ready.
@Ignore
public class LDAPUtilsTest {

    @Test
    public void getUsersFromLDAPGroup() throws Exception {
        DirContext dirContext = LDAPUtils.getDirContext("ldap://localhost:9000");
        List<String> bioUsers = LDAPUtils.getUsersFromLDAPGroup(dirContext, "bio", "ou=general,ou=groups,dc=ge,dc=co,dc=uk");
        assertEquals(59, bioUsers.size());
    }

    @Test
    public void getUserInfoFromLDAP() throws Exception {
        DirContext dirContext = LDAPUtils.getDirContext("ldap://localhost:9000");
        List<Attributes> userInfoList = LDAPUtils.getUserInfoFromLDAP(dirContext, Arrays.asList("pfurio", "imedina"), "dc=ge,dc=co,dc=uk");
        assertEquals(2, userInfoList.size());

        List<String> userList = Arrays.asList("pfurio", "imedina");
        for (Attributes attributes : userInfoList) {
            assertTrue(userList.contains(attributes.get("uid").get(0)));
        }
    }

}