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

package org.opencb.opencga.catalog.utils;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.opencga.catalog.auth.authentication.LDAPUtils;

import javax.naming.directory.Attributes;
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
        List<String> bioUsers = LDAPUtils.getUsersFromLDAPGroup("ldap://localhost:9000", "bio", "ou=groups,dc=ge,dc=co,dc=uk");
        assertEquals(59, bioUsers.size());
    }

    @Test
    public void getUsersFromEmptyLDAPGroup() throws Exception {
        List<String> bioUsers = LDAPUtils.getUsersFromLDAPGroup("ldap://localhost:9000", "cipapi-ldp-rxn", "ou=groups,dc=ge,dc=co,dc=uk");
        assertEquals(0, bioUsers.size());
    }

    @Test
    public void getUserInfoFromLDAP() throws Exception {
        List<Attributes> userInfoList = LDAPUtils.getUserInfoFromLDAP("ldap://localhost:9000", Arrays.asList("pfurio", "imedina"), "dc=ge,dc=co,dc=uk");
        assertEquals(2, userInfoList.size());

        List<String> userList = Arrays.asList("pfurio", "imedina");
        for (Attributes attributes : userInfoList) {
            assertTrue(userList.contains(attributes.get("uid").get(0)));
        }
    }

}