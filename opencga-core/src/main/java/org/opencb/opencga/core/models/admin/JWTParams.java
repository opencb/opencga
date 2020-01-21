package org.opencb.opencga.core.models.admin;

public class JWTParams {

    private String secretKey;

    public JWTParams() {
    }

    public JWTParams(String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JWTParams{");
        sb.append("secretKey='").append(secretKey).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSecretKey() {
        return secretKey;
    }

    public JWTParams setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }
}
