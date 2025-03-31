package org.opencb.opencga.core.models.organizations;

import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Optimizations;
import org.opencb.opencga.core.config.UserOrganizationConfiguration;

import java.util.List;

public class OrganizationConfiguration {

    private List<AuthenticationOrigin> authenticationOrigins;
    private UserOrganizationConfiguration user;
    @Deprecated
    private String defaultUserExpirationDate;
    private Optimizations optimizations;
    private TokenConfiguration token;

    public OrganizationConfiguration() {
    }

    public OrganizationConfiguration(List<AuthenticationOrigin> authenticationOrigins, UserOrganizationConfiguration user,
                                     Optimizations optimizations, TokenConfiguration token) {
        this.authenticationOrigins = authenticationOrigins;
        this.user = user;
        this.optimizations = optimizations;
        this.token = token;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationConfiguration{");
        sb.append("authenticationOrigins=").append(authenticationOrigins);
        sb.append(", user=").append(user);
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

    public UserOrganizationConfiguration getUser() {
        return user;
    }

    public OrganizationConfiguration setUser(UserOrganizationConfiguration user) {
        this.user = user;
        return this;
    }

//    @Deprecated
//    public String getDefaultUserExpirationDate() {
//        return null;
//    }

    @Deprecated
    public OrganizationConfiguration setDefaultUserExpirationDate(String defaultUserExpirationDate) {
        if (this.user == null) {
            this.user = new UserOrganizationConfiguration();
        }
        this.user.setDefaultExpirationDate(defaultUserExpirationDate);
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
