package org.opencb.opencga.core.models.organizations;

import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Optimizations;

import java.util.List;

public class OrganizationConfiguration {

    private List<AuthenticationOrigin> authenticationOrigins;
    private Optimizations optimizations;
    private TokenConfiguration token;

    public OrganizationConfiguration() {
    }

    public OrganizationConfiguration(List<AuthenticationOrigin> authenticationOrigins, Optimizations optimizations,
                                     TokenConfiguration token) {
        this.authenticationOrigins = authenticationOrigins;
        this.optimizations = optimizations;
        this.token = token;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationConfiguration{");
        sb.append("authenticationOrigins=").append(authenticationOrigins);
        sb.append(", optimizations=").append(optimizations);
        sb.append(", token=").append(token);
        sb.append('}');
        return sb.toString();
    }

    public List<AuthenticationOrigin> getAuthenticationOrigins() {
        return authenticationOrigins;
    }

    public OrganizationConfiguration setAuthenticationOrigins(List<AuthenticationOrigin> authenticationOrigins) {
        this.authenticationOrigins = authenticationOrigins;
        return this;
    }

    public Optimizations getOptimizations() {
        return optimizations;
    }

    public OrganizationConfiguration setOptimizations(Optimizations optimizations) {
        this.optimizations = optimizations;
        return this;
    }

    public TokenConfiguration getToken() {
        return token;
    }

    public OrganizationConfiguration setToken(TokenConfiguration token) {
        this.token = token;
        return this;
    }
}
