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

package org.opencb.opencga.catalog.auth.authentication;

import io.jsonwebtoken.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Date;

public class JwtManager {

    private Configuration configuration;

    private String secretKey;
    private Long expiration;
    private Logger logger;

    JwtManager(Configuration configuration) {
        this.configuration = configuration;

        this.secretKey = this.configuration.getAdmin().getSecretKey();
        this.expiration = this.configuration.getAuthentication().getExpiration();

        logger = LoggerFactory.getLogger(JwtManager.class);
    }

    String getSecretKey() {
        return secretKey;
    }

    JwtManager setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    Long getExpiration() {
        return expiration;
    }

    JwtManager setExpiration(Long expiration) {
        this.expiration = expiration;
        return this;
    }

    String createJWTToken(String userId) {
        return createJWTToken(userId, expiration);
    }

    String createJWTToken(String userId, long expiration) {
        String jwt = null;
        try {
            long currentTime = System.currentTimeMillis();
            JwtBuilder jwtBuilder = Jwts.builder()
                    .setSubject(userId)
                    .setAudience("OpenCGA users")
                    .setIssuedAt(new Date(currentTime))
                    .signWith(SignatureAlgorithm.forName(configuration.getAdmin().getAlgorithm()), this.secretKey.getBytes("UTF-8"));

            // Set the expiration in number of seconds only if 'expiration' is greater than 0
            if (expiration > 0) {
                jwtBuilder.setExpiration(new Date(currentTime + expiration * 1000L));
            }

            jwt = jwtBuilder.compact();
        } catch (UnsupportedEncodingException e) {
            logger.error("error while creating jwt token");
        }
        return jwt;
    }

    void validateToken(String jwtKey) throws CatalogAuthenticationException {
        parseClaims(jwtKey);
    }

    String getAudience(String jwtKey) throws CatalogAuthenticationException {
        return parseClaims(jwtKey).getBody().getAudience();
    }

    String getUser(String jwtKey) throws CatalogAuthenticationException {
        return parseClaims(jwtKey).getBody().getSubject();
    }

    Date getExpiration(String jwtKey) throws CatalogAuthenticationException {
        return parseClaims(jwtKey).getBody().getExpiration();
    }

    private Jws<Claims> parseClaims(String jwtKey) throws CatalogAuthenticationException {
        try {
            return Jwts.parser().setSigningKey(this.secretKey.getBytes("UTF-8")).parseClaimsJws(jwtKey);
        } catch (ExpiredJwtException e) {
            throw CatalogAuthenticationException.tokenExpired(jwtKey);
        } catch (MalformedJwtException | SignatureException e) {
            throw CatalogAuthenticationException.invalidAuthenticationToken(jwtKey);
        } catch (UnsupportedEncodingException e) {
            throw CatalogAuthenticationException.invalidAuthenticationEncodingToken(jwtKey);
        }
    }


}
