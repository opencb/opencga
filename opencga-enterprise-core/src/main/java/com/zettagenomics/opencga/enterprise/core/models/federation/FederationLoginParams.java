package com.zettagenomics.opencga.enterprise.core.models.federation;

import org.opencb.opencga.core.models.user.LoginParams;

public class FederationLoginParams extends LoginParams {

    private String secretKey;

    public FederationLoginParams() {
    }

    public FederationLoginParams(String refreshToken, String secretKey) {
        super(refreshToken);
        this.secretKey = secretKey;
    }

    public FederationLoginParams(String organization, String user, String password, String secretKey) {
        super(organization, user, password);
        this.secretKey = secretKey;
    }

    public FederationLoginParams(String organization, String user, String password, String refreshToken,
                                 String secretKey) {
        super(organization, user, password, refreshToken);
        this.secretKey = secretKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationLoginParams{");
        sb.append("organization='").append(getOrganization()).append('\'');
        sb.append(", user='").append(getUser()).append('\'');
        sb.append(", password='").append(getPassword()).append('\'');
        sb.append(", refreshToken='").append(getRefreshToken()).append('\'');
        sb.append(", secretKey='").append(secretKey).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSecretKey() {
        return secretKey;
    }

    public FederationLoginParams setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    @Override
    public FederationLoginParams setOrganization(String organization) {
        super.setOrganization(organization);
        return this;
    }

    @Override
    public FederationLoginParams setUser(String user) {
        super.setUser(user);
        return this;
    }

    @Override
    public FederationLoginParams setPassword(String password) {
        super.setPassword(password);
        return this;
    }

    @Override
    public FederationLoginParams setRefreshToken(String refreshToken) {
        super.setRefreshToken(refreshToken);
        return this;
    }
}
