package org.opencb.opencga.catalog.auth.authentication;

import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.NotImplementedException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.util.List;
import java.util.Map;

public class SSOAuthenticationManager extends AuthenticationManager {

    public SSOAuthenticationManager(String algorithm, String secretKeyString, long expiration) {
        super(expiration);

        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.valueOf(algorithm);
        Key secretKey = this.converStringToKeyObject(secretKeyString, signatureAlgorithm.getJcaName());
        this.jwtManager = new JwtManager(signatureAlgorithm.getValue(), secretKey);

        this.logger = LoggerFactory.getLogger(SSOAuthenticationManager.class);
    }

    @Override
    public AuthenticationResponse authenticate(String organizationId, String userId, String password)
            throws CatalogAuthenticationException {
        throw new NotImplementedException("Authentication should be done through SSO");
    }

    @Override
    public List<User> getUsersFromRemoteGroup(String group) throws CatalogException {
        throw new NotImplementedException("Operation not implemented");
    }

    @Override
    public List<User> getRemoteUserInformation(List<String> userStringList) throws CatalogException {
        throw new NotImplementedException("Operation not implemented");
    }

    @Override
    public List<String> getRemoteGroups(String token) throws CatalogException {
        throw new NotImplementedException("Operation not implemented");
    }

    @Override
    public void changePassword(String organizationId, String userId, String oldPassword, String newPassword) throws CatalogException {
        throw new NotImplementedException("Change password should be done through SSO");
    }

    @Override
    public OpenCGAResult resetPassword(String organizationId, String userId) throws CatalogException {
        throw new NotImplementedException("Reset password should be done through SSO");
    }

    @Override
    public void newPassword(String organizationId, String userId, String newPassword) throws CatalogException {
        throw new NotImplementedException("Setting a new password should be done through SSO");
    }

    @Override
    public String createToken(String organizationId, String userId, Map<String, Object> claims, long expiration) {
        return jwtManager.createJWTToken(organizationId, AuthenticationOrigin.AuthenticationType.SSO, userId, claims, expiration);
    }

    @Override
    public String createNonExpiringToken(String organizationId, String userId, Map<String, Object> claims) {
        return jwtManager.createJWTToken(organizationId, AuthenticationOrigin.AuthenticationType.SSO, userId, claims, 0L);
    }
}
