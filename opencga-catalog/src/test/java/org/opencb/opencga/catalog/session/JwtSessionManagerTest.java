package org.opencb.opencga.catalog.session;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.exceptions.CatalogTokenException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
        jwtToken = jwtSessionManager.createJWTToken("testUser");
    }

    @Test
    public void testParseClaims() throws Exception {
        Jws<Claims> claims = jwtSessionManager.parseClaims(jwtToken);
        assertEquals(claims.getBody().getSubject(), "testUser");
    }

    @Test(expected = CatalogTokenException.class)
    public void testExpiredToken() throws CatalogTokenException {
        String expiredToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJPcGVuQ0dBIEF1dGhlbnRpY2F0aW9uIiwiZXhwIjoxNDk2NzQ3MjI2LCJ1c2VySWQiOiJ0ZXN0VXNlciIsInR5cGUiOiJVU0VSIiwiaXAiOiIxNzIuMjAuNTYuMSJ9.cZbGHh46tP88QDATv4pwWODRf49tG9N2H_O8lXyjjIc";
        jwtSessionManager.parseClaims(expiredToken);
    }

    @Test(expected = CatalogTokenException.class)
    public void testInvalidToken() throws CatalogTokenException {
        String invalidToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJPcGVuQ0dBIEF1dGhlbnRpY2F0aW9uIiwiZXhwIjoxNDk2NzQ3MjI2LCJ1c2VySWQiOiJ0ZXN0VXNlciIsInR5cGUiOiJVU0VSIiwiaXAiOiIxNzIuMjAuNTYuMSJ9.cZbGHh46tP88QDATv4pwWODRf49tG9N2H_O8lXyjj";
        jwtSessionManager.parseClaims(invalidToken);
    }

    @Test(expected = CatalogTokenException.class)
    public void testInvalidSecretKey() throws CatalogTokenException {
        jwtSessionManager.setSecretKey("wrongKey");
        jwtSessionManager.parseClaims(jwtToken);
    }
}
