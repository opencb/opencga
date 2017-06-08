

package org.opencb.opencga.catalog.session;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Date;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogTokenException;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.Session.Type;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JWTSessionManager implements SessionManager {
    protected static Logger logger = LoggerFactory.getLogger(JWTSessionManager.class);
    protected Configuration configuration;
    protected String secretKey;
    protected Long expiration;

    public JWTSessionManager(Configuration configuration) {
        this.configuration = configuration;
        this.secretKey = this.configuration.getAdmin().getSecretKey();
        this.expiration = this.configuration.getAuthentication().getExpiration();
    }

    public String createJWTToken(String userId) throws CatalogException {
        String jwt = null;

        try {
            Long e = Long.valueOf(System.currentTimeMillis());
            jwt = Jwts.builder()
                    .setSubject(userId)
                    .setExpiration(new Date(e.longValue() + this.expiration.longValue() * 1000L))
                    .setAudience("OpenCGA users")
                    .setIssuedAt(new Date(e.longValue()))
                    .signWith(SignatureAlgorithm.HS256, this.secretKey.getBytes("UTF-8"))
                    .compact();
        } catch (UnsupportedEncodingException e) {
            logger.error("error while creating jwt token");
        }

        return jwt;
    }

    public Jws<Claims> parseClaims(String jwtKey) throws CatalogTokenException {
        try {
            Jws claims = Jwts.parser().setSigningKey(this.secretKey.getBytes("UTF-8")).parseClaimsJws(jwtKey);
            return claims;
        } catch (ExpiredJwtException e) {
            throw new CatalogTokenException("authentication token is expired : " + jwtKey);
        } catch (MalformedJwtException | SignatureException e) {
            throw new CatalogTokenException("invalid authentication token : " + jwtKey);
        } catch (UnsupportedEncodingException e) {
            throw new CatalogTokenException("invalid authentication token encoding : " + jwtKey);
        }
    }

    public String getUserId(String jwtKey) throws CatalogTokenException {
        if (jwtKey == null || jwtKey.isEmpty() || jwtKey.equalsIgnoreCase("null")) {
            return "anonymous";
        }

        return parseClaims(jwtKey).getBody().getSubject();
    }

    public QueryResult<Session> createToken(String userId, String ip, Type type) throws CatalogException {
        QueryResult result = new QueryResult();
        String jwtToken = this.createJWTToken(userId);
        Session session = new Session(jwtToken, ip, TimeUtils.getTime(), type);
        result.addAllResults(Collections.singletonList(session));
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
