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

import io.jsonwebtoken.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.security.Key;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class JwtManager {

    private SignatureAlgorithm algorithm;

    private Key privateKey;
    private Key publicKey;

    private final Logger logger;

    // 32 characters to ensure it is at least 256 bits long
    public static final int SECRET_KEY_MIN_LENGTH = 32;

    public JwtManager(String algorithm) {
        this(algorithm, null, null);
    }

    JwtManager(String algorithm, Key secretKey) {
        this(algorithm, secretKey, secretKey);
    }

    JwtManager(String algorithm, @Nullable Key privateKey, @Nullable Key publicKey) {
        this.algorithm = SignatureAlgorithm.forName(algorithm);
        this.privateKey = privateKey;
        this.publicKey = publicKey;

        logger = LoggerFactory.getLogger(JwtManager.class);
    }

    public SignatureAlgorithm getAlgorithm() {
        return algorithm;
    }

    public JwtManager setAlgorithm(SignatureAlgorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public Key getPrivateKey() {
        return privateKey;
    }

    public JwtManager setPrivateKey(Key privateKey) {
        this.privateKey = privateKey;
        return this;
    }

    public Key getPublicKey() {
        return publicKey;
    }

    public JwtManager setPublicKey(Key publicKey) {
        this.publicKey = publicKey;
        return this;
    }

    String createJWTToken(String userId, long expiration) {
        return createJWTToken(userId, Collections.emptyMap(), expiration);
    }

    String createJWTToken(String userId, Map<String, Object> claims, long expiration) {
        long currentTime = System.currentTimeMillis();

        JwtBuilder jwtBuilder = Jwts.builder();
        if (claims != null && !claims.isEmpty()) {
            jwtBuilder.setClaims(claims);
        }
        jwtBuilder.setSubject(userId)
                .setAudience("OpenCGA users")
                .setIssuedAt(new Date(currentTime))
                .signWith(privateKey, algorithm);

        // Set the expiration in number of seconds only if 'expiration' is greater than 0
        if (expiration > 0) {
            jwtBuilder.setExpiration(new Date(currentTime + expiration * 1000L));
        }

        return jwtBuilder.compact();
    }

    void validateToken(String token) throws CatalogAuthenticationException {
        validateToken(token, this.publicKey);
    }

    void validateToken(String token, Key publicKey) throws CatalogAuthenticationException {
        parseClaims(token, publicKey);
    }

    String getAudience(String token) throws CatalogAuthenticationException {
        return getAudience(token, this.publicKey);
    }

    String getAudience(String token, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().getAudience();
    }

    String getUser(String token) throws CatalogAuthenticationException {
        return getUser(token, this.publicKey);
    }

    String getUser(String token, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().getSubject();
    }

    String getUser(String token, String fieldKey) throws CatalogAuthenticationException {
        return String.valueOf(parseClaims(token, publicKey).getBody().get(fieldKey));
    }

    List<String> getGroups(String token, String fieldKey) throws CatalogAuthenticationException {
        return getGroups(token, fieldKey, this.publicKey);
    }

    List<String> getGroups(String token, String fieldKey, Key publicKey) throws CatalogAuthenticationException {
        Object o = parseClaims(token, publicKey).getBody().get(fieldKey);

        if (o instanceof List) {
            return (List<String>) o;
        } else {
            return Collections.singletonList(String.valueOf(o));
        }
    }

    Date getExpiration(String token) throws CatalogAuthenticationException {
        return getExpiration(token, this.publicKey);
    }

    Date getExpiration(String token, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().getExpiration();
    }

    Object getClaim(String token, String claimId) throws CatalogAuthenticationException {
        return getClaim(token, claimId, this.publicKey);
    }

    Object getClaim(String token, String claimId, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().get(claimId);
    }

    private Jws<Claims> parseClaims(String token, Key publicKey) throws CatalogAuthenticationException {
        try {
            return Jwts.parser().setSigningKey(publicKey).parseClaimsJws(token);
        } catch (ExpiredJwtException e) {
            logger.error("JWT Error: '{}'", e.getMessage(), e);
            throw CatalogAuthenticationException.tokenExpired(token);
        } catch (MalformedJwtException | SignatureException e) {
            logger.error("JWT Error: '{}'", e.getMessage(), e);
            throw CatalogAuthenticationException.invalidAuthenticationToken(token);
        } catch (JwtException e) {
            logger.error("JWT Error: '{}'", e.getMessage(), e);
            throw CatalogAuthenticationException.unknownJwtException(token);
        }
    }

    // Check if the token contains the key and any of the values from 'filters'
    public boolean passFilters(String token, Map<String, List<String>> filters) throws CatalogAuthenticationException {
        return passFilters(token, filters, this.publicKey);
    }

    public boolean passFilters(String token, Map<String, List<String>> filters, Key publicKey) throws CatalogAuthenticationException {
        if (filters == null || filters.isEmpty()) {
            return true;
        }

        Claims body = parseClaims(token, publicKey).getBody();
        for (Map.Entry<String, List<String>> entry : filters.entrySet()) {
            if (!entry.getValue().contains(String.valueOf(body.get(entry.getKey())))) {
                return false;
            }
        }

        return true;
    }

}
