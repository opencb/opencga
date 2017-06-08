package org.opencb.opencga.catalog.config;

/**
 * Created by wasim on 06/06/17.
 */

public class Authentication {
    private Long expiration;

    public Authentication() {
    }

    public Authentication(Long expiration) {
        this.expiration = expiration;
    }

    public Long getExpiration() {
        return this.expiration;
    }

    public void setExpiration(Long expiration) {
        this.expiration = expiration;
    }

    public String toString() {
        return "Authentication{expiration=" + this.expiration + '}';
    }
}
