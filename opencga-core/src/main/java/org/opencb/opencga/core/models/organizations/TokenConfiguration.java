package org.opencb.opencga.core.models.organizations;

import io.jsonwebtoken.SignatureAlgorithm;
import org.opencb.opencga.core.common.PasswordUtils;

public class TokenConfiguration {

    private String algorithm;
    private String secretKey;
    private long expiration;

    public TokenConfiguration() {
    }

    public TokenConfiguration(String algorithm, String secretKey, long expiration) {
        this.algorithm = algorithm;
        this.secretKey = secretKey;
        this.expiration = expiration;
    }

    public static TokenConfiguration init() {
        return new TokenConfiguration(SignatureAlgorithm.HS256.getValue(), PasswordUtils.getStrongRandomPassword(32), 3600L);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TokenConfiguration{");
        sb.append("algorithm='").append(algorithm).append('\'');
        sb.append(", secretKey='").append(secretKey).append('\'');
        sb.append(", expiration=").append(expiration);
        sb.append('}');
        return sb.toString();
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public TokenConfiguration setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public TokenConfiguration setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public long getExpiration() {
        return expiration;
    }

    public TokenConfiguration setExpiration(long expiration) {
        this.expiration = expiration;
        return this;
    }
}
