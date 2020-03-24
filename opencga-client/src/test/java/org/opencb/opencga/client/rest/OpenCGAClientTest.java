package org.opencb.opencga.client.rest;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class OpenCGAClientTest {

    @Test
    public void testGetUserFromToken() {
        String token = Jwts.builder().signWith(SignatureAlgorithm.HS256, "dummy")
                .setExpiration(Date.from(Instant.now().plusMillis(1000)))
                .setSubject("joe")
                .compact();

        assertEquals("joe", OpenCGAClient.getUserFromToken(token));
    }

    @Test
    public void testGetUserFromTokenExpired() {
        String token = Jwts.builder().signWith(SignatureAlgorithm.HS256, "dummy")
                .setExpiration(Date.from(Instant.ofEpochMilli(System.currentTimeMillis() - 1000)))
                .setSubject("joe")
                .compact();

        assertEquals("joe", OpenCGAClient.getUserFromToken(token));
    }
}