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
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.common.JwtUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.JwtPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.security.Key;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.models.JwtPayload.AUTH_ORIGIN;

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

    public JwtManager(String algorithm, Key secretKey) {
        this(algorithm, secretKey, secretKey);
    }

    public JwtManager(String algorithm, @Nullable Key privateKey, @Nullable Key publicKey) {
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

    public String createJWTToken(String organizationId, AuthenticationOrigin.AuthenticationType type, String userId,
                                 Map<String, Object> claims, List<JwtPayload.FederationJwtPayload> federations, long expiration) {
        long currentTime = System.currentTimeMillis();

        JwtBuilder jwtBuilder = Jwts.builder();
        if (claims != null && !claims.isEmpty()) {
            jwtBuilder.setClaims(claims);
        }
        if (type != null) {
            jwtBuilder.addClaims(Collections.singletonMap(AUTH_ORIGIN, type));
        }
        if (CollectionUtils.isNotEmpty(federations)) {
            jwtBuilder.addClaims(Collections.singletonMap(JwtPayload.FEDERATIONS, federations));
        }

        jwtBuilder.setSubject(userId)
                .setAudience(organizationId)
                .setIssuer("OpenCGA")
                .setIssuedAt(new Date(currentTime))
                .signWith(privateKey, algorithm);

        // Set the expiration in number of seconds only if 'expiration' is greater than 0
        if (expiration > 0) {
            jwtBuilder.setExpiration(new Date(currentTime + expiration * 1000L));
        }

        return jwtBuilder.compact();
    }

    public void validateToken(String token) throws CatalogAuthenticationException {
        parseClaims(token);
    }

    public JwtPayload getPayload(String token) throws CatalogAuthenticationException {
        Claims body = parseClaims(token).getBody();
        return new JwtPayload(body.getSubject(), body.getAudience(), getAuthOrigin(body), body.getIssuer(), body.getIssuedAt(),
                body.getExpiration(), JwtUtils.getFederations(body), token);
    }

    public JwtPayload getPayload(String token, Key publicKey) throws CatalogAuthenticationException {
        Claims body = parseClaims(token, publicKey).getBody();
        return new JwtPayload(body.getSubject(), body.getAudience(), getAuthOrigin(body), body.getIssuer(), body.getIssuedAt(),
                body.getExpiration(), JwtUtils.getFederations(body), token);
    }

    private AuthenticationOrigin.AuthenticationType getAuthOrigin(Claims claims) {
        String o = claims.get(AUTH_ORIGIN, String.class);
        if (o != null) {
            return AuthenticationOrigin.AuthenticationType.valueOf(o);
        } else {
            return null;
        }
    }

    public String getAudience(String token) throws CatalogAuthenticationException {
        return getAudience(token, this.publicKey);
    }

    public String getAudience(String token, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().getAudience();
    }

    public String getUser(String token) throws CatalogAuthenticationException {
        return getUser(token, this.publicKey);
    }

    public String getUser(String token, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().getSubject();
    }

    public String getUser(String token, String fieldKey) throws CatalogAuthenticationException {
        return String.valueOf(parseClaims(token).getBody().get(fieldKey));
    }

    public List<String> getGroups(String token, String fieldKey) throws CatalogAuthenticationException {
        return getGroups(token, fieldKey, this.publicKey);
    }

    public List<String> getGroups(String token, String fieldKey, Key publicKey) throws CatalogAuthenticationException {
        Object o = parseClaims(token, publicKey).getBody().get(fieldKey);

        if (o instanceof List) {
            return (List<String>) o;
        } else {
            return Collections.singletonList(String.valueOf(o));
        }
    }

    public Date getExpiration(String token) throws CatalogAuthenticationException {
        return getExpiration(token, this.publicKey);
    }

    public Date getExpiration(String token, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().getExpiration();
    }

    public Object getClaim(String token, String claimId) throws CatalogAuthenticationException {
        return getClaim(token, claimId, this.publicKey);
    }

    public Object getClaim(String token, String claimId, Key publicKey) throws CatalogAuthenticationException {
        return parseClaims(token, publicKey).getBody().get(claimId);
    }

    private Jws<Claims> parseClaims(String token) throws CatalogAuthenticationException {
        return parseClaims(token, null);
    }

    private Jws<Claims> parseClaims(String token, Key publicKey) throws CatalogAuthenticationException {
        Key key = publicKey != null ? publicKey : this.publicKey;
        try {
            return Jwts.parser().setSigningKey(key).parseClaimsJws(token);
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
