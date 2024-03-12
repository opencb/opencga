package org.opencb.opencga.core.models.organizations;

import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Optimizations;

import java.util.LinkedList;
import java.util.List;

public class OrganizationConfiguration {

    private List<AuthenticationOrigin> authenticationOrigins;
    private Optimizations optimizations;

    public OrganizationConfiguration() {
        this.authenticationOrigins = new LinkedList<>();
        this.optimizations = new Optimizations();
    }

    public OrganizationConfiguration(List<AuthenticationOrigin> authenticationOrigins, Optimizations optimizations) {
        this.authenticationOrigins = authenticationOrigins;
        this.optimizations = optimizations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationConfiguration{");
        sb.append("authenticationOrigins=").append(authenticationOrigins);
        sb.append(", optimizations=").append(optimizations);
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
}
