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

package org.opencb.opencga.catalog.session;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 06/06/17.
 */
public class JwtSessionManagerTest extends GenericTest {

    private JwtSessionManager jwtSessionManager;
    private String jwtToken;
    private String ip = "172.20.56.1";

    @Before
    public void setUp() throws Exception  {
        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());
        configuration.getAdmin().setSecretKey("12345");
        configuration.getAdmin().setAlgorithm("HS256");
        jwtSessionManager = new JwtSessionManager(configuration);
        testCreateJWTToken();
    }

    @Test
    public void testCreateJWTToken() throws Exception {
        jwtToken = jwtSessionManager.createJWTToken("testUser", 60L);
    }

    @Test
    public void testParseClaims() throws Exception {
        Jws<Claims> claims = jwtSessionManager.parseClaims(jwtToken);
        assertEquals(claims.getBody().getSubject(), "testUser");
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testExpiredToken() throws CatalogAuthenticationException {
        String expiredToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJPcGVuQ0dBIEF1dGhlbnRpY2F0aW9uIiwiZXhwIjoxNDk2NzQ3MjI2LCJ1c2VySWQiOiJ0ZXN0VXNlciIsInR5cGUiOiJVU0VSIiwiaXAiOiIxNzIuMjAuNTYuMSJ9.cZbGHh46tP88QDATv4pwWODRf49tG9N2H_O8lXyjjIc";
        jwtSessionManager.parseClaims(expiredToken);
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testInvalidToken() throws CatalogAuthenticationException {
        String invalidToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJPcGVuQ0dBIEF1dGhlbnRpY2F0aW9uIiwiZXhwIjoxNDk2NzQ3MjI2LCJ1c2VySWQiOiJ0ZXN0VXNlciIsInR5cGUiOiJVU0VSIiwiaXAiOiIxNzIuMjAuNTYuMSJ9.cZbGHh46tP88QDATv4pwWODRf49tG9N2H_O8lXyjj";
        jwtSessionManager.parseClaims(invalidToken);
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testInvalidSecretKey() throws CatalogAuthenticationException {
        jwtSessionManager.setSecretKey("wrongKey");
        jwtSessionManager.parseClaims(jwtToken);
    }

    @Test
    public void testNonExpiringToken() throws CatalogException {
        String nonExpiringToken = jwtSessionManager.createJWTToken("System", -1L);
        Jws<Claims> claims = jwtSessionManager.parseClaims(nonExpiringToken);
        assertEquals(claims.getBody().getSubject(), "System");
        assertNull(claims.getBody().getExpiration());
    }
}
