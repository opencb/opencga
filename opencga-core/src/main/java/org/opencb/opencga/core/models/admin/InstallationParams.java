package org.opencb.opencga.core.models.admin;

public class InstallationParams {

    private String secretKey;
    private String password;
    private String email;
    private String organization;

    public InstallationParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InstallationParams{");
        sb.append("secretKey='").append(secretKey).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSecretKey() {
        return secretKey;
    }

    public InstallationParams setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public InstallationParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public InstallationParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public InstallationParams setOrganization(String organization) {
        this.organization = organization;
        return this;
    }
}
