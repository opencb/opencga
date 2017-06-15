package org.opencb.opencga.catalog.config;

import java.util.List;

/**
 * Created by wasim on 06/06/17.
 */

public class Authentication {
    private Long expiration;
    private List<AuthenticationOrigin> authenticationOrigins;

    public Authentication() {
    }

    public Authentication(Long expiration, List<AuthenticationOrigin> authenticationOrigins) {
        this.expiration = expiration;
        this.authenticationOrigins = authenticationOrigins;
    }

    public Long getExpiration() {
        return expiration;
    }

    public Authentication setExpiration(Long expiration) {
        this.expiration = expiration;
        return this;
    }

    public List<AuthenticationOrigin> getAuthenticationOrigins() {
        return authenticationOrigins;
    }

    public Authentication setAuthenticationOrigins(List<AuthenticationOrigin> authenticationOrigins) {
        this.authenticationOrigins = authenticationOrigins;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Authentication{");
        sb.append("expiration=").append(expiration);
        sb.append(", authenticationOrigins=").append(authenticationOrigins);
        sb.append('}');
        return sb.toString();
    }
}
