

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

import io.jsonwebtoken.*;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.Session.Type;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Date;

public class JwtSessionManager {
    protected static Logger logger = LoggerFactory.getLogger(JwtSessionManager.class);
    protected Configuration configuration;
    protected String secretKey;
    protected Long expiration;

    public JwtSessionManager(Configuration configuration) {
        this.configuration = configuration;
        this.secretKey = this.configuration.getAdmin().getSecretKey();
        this.expiration = this.configuration.getAuthentication().getExpiration();
    }

    String createJWTToken(String userId, long expiration) {
        String jwt = null;

        try {
            long currentTime = System.currentTimeMillis();
            JwtBuilder jwtBuilder = Jwts.builder()
                    .setSubject(userId)
                    .setAudience("OpenCGA users")
                    .setIssuedAt(new Date(currentTime))
                    .signWith(SignatureAlgorithm.forName(configuration.getAdmin().getAlgorithm()),
                            this.secretKey.getBytes("UTF-8"));
            if (expiration > -1) {
                jwtBuilder.setExpiration(new Date(currentTime + expiration * 1000L));
            }
            jwt = jwtBuilder.compact();
        } catch (UnsupportedEncodingException e) {
            logger.error("error while creating jwt token");
        }

        return jwt;
    }

    Jws<Claims> parseClaims(String jwtKey) throws CatalogAuthenticationException {
        try {
            Jws claims = Jwts.parser().setSigningKey(this.secretKey.getBytes("UTF-8")).parseClaimsJws(jwtKey);
            return claims;
        } catch (ExpiredJwtException e) {
            throw CatalogAuthenticationException.tokenExpired(jwtKey);
        } catch (MalformedJwtException | SignatureException e) {
            throw CatalogAuthenticationException.invalidAuthenticationToken(jwtKey);
        } catch (UnsupportedEncodingException e) {
            throw CatalogAuthenticationException.invalidAuthenticationEncodingToken(jwtKey);
        }
    }

    public String getUserId(String jwtKey) throws CatalogAuthenticationException {
        return parseClaims(jwtKey).getBody().getSubject();
    }

    public QueryResult<Session> createToken(String userId, String ip, Type type) {
        QueryResult result = new QueryResult();
        String jwtToken = null;

        if (type.equals(Type.SYSTEM)) {
            jwtToken = this.createJWTToken(userId, -1L);
        } else {
            jwtToken = this.createJWTToken(userId, this.getExpiration());
        }

        Session session = new Session(jwtToken, ip, TimeUtils.getTime(), type);
        result.setResult(Collections.singletonList(session));
        return result;
    }

    public String getSecretKey() {
        return this.secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public Long getExpiration() {
        return this.expiration;
    }

    public void setExpiration(Long expiration) {
        this.expiration = expiration;
    }

    public void clearToken(String userId, String sessionId) throws CatalogException {
    }

    public void checkAdminSession(String sessionId) throws CatalogException {
    }
}
