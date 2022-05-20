package org.opencb.opencga.catalog.utils;

import org.junit.Test;

public class JwtUtilsTest {

    @Test
    public void getExpirationDateTest() {
        String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwZnVyaW8iLCJhdWQiOiJPcGVuQ0dBIHVzZXJzIiwiaWF0IjoxNjUyNzgzMjA5LCJleHAiOjE2NTI3ODY4MDl9.-OgAOY4yYdToGF8rvkwwUzLcJl5xV8HDBOmm7US48ME";
        JwtUtils.getExpirationDate(token);
    }
}
