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
import io.jsonwebtoken.impl.TextCodec;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 06/06/17.
 */
public class JwtSessionManagerTest extends GenericTest {

    private JwtManager jwtSessionManager;
    private String jwtToken;

    @Before
    public void setUp() throws Exception  {
        Key key = new SecretKeySpec(TextCodec.BASE64.decode("12345"), SignatureAlgorithm.HS256.getJcaName());
        jwtSessionManager = new JwtManager(SignatureAlgorithm.HS256.getValue(), key, key);
        testCreateJWTToken();
    }

    @Test
    public void testCreateJWTToken() throws Exception {
        jwtToken = jwtSessionManager.createJWTToken("testUser", Collections.emptyMap(), 60L);
    }

    @Test
    public void testParseClaims() throws Exception {
        assertEquals(jwtSessionManager.getUser(jwtToken), "testUser");
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testExpiredToken() throws CatalogAuthenticationException {
        String expiredToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJPcGVuQ0dBIEF1dGhlbnRpY2F0aW9uIiwiZXhwIjoxNDk2NzQ3MjI2LCJ1c2VySWQiOiJ0ZXN0VXNlciIsInR5cGUiOiJVU0VSIiwiaXAiOiIxNzIuMjAuNTYuMSJ9.cZbGHh46tP88QDATv4pwWODRf49tG9N2H_O8lXyjjIc";
        jwtSessionManager.validateToken(expiredToken);
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testInvalidToken() throws CatalogAuthenticationException {
        String invalidToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJPcGVuQ0dBIEF1dGhlbnRpY2F0aW9uIiwiZXhwIjoxNDk2NzQ3MjI2LCJ1c2VySWQiOiJ0ZXN0VXNlciIsInR5cGUiOiJVU0VSIiwiaXAiOiIxNzIuMjAuNTYuMSJ9.cZbGHh46tP88QDATv4pwWODRf49tG9N2H_O8lXyjj";
        jwtSessionManager.validateToken(invalidToken);
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testInvalidSecretKey() throws CatalogAuthenticationException {
        jwtSessionManager.setPublicKey(new SecretKeySpec(TextCodec.BASE64.decode("wrongKey"), SignatureAlgorithm.HS256.getJcaName()));
        jwtSessionManager.validateToken(jwtToken);
    }

    @Test
    public void testNonExpiringToken() throws CatalogException {
        String nonExpiringToken = jwtSessionManager.createJWTToken("System", null, -1L);
        assertEquals(jwtSessionManager.getUser(nonExpiringToken), "System");
        assertNull(jwtSessionManager.getExpiration(nonExpiringToken));
    }
}
