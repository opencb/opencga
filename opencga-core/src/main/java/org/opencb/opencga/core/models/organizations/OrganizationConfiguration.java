package org.opencb.opencga.core.models.organizations;

import org.opencb.opencga.core.config.Authentication;
import org.opencb.opencga.core.config.Optimizations;

public class OrganizationConfiguration {

    private Authentication authentication;
    private Optimizations optimizations;

    public OrganizationConfiguration() {
        this.authentication = new Authentication();
        this.optimizations = new Optimizations();
    }

    public OrganizationConfiguration(Authentication authentication, Optimizations optimizations) {
        this.authentication = authentication;
        this.optimizations = optimizations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationConfiguration{");
        sb.append("authentication=").append(authentication);
        sb.append(", optimizations=").append(optimizations);
        sb.append('}');
        return sb.toString();
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public OrganizationConfiguration setAuthentication(Authentication authentication) {
        this.authentication = authentication;
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
