package org.opencb.opencga.core.config;

import java.util.List;

public class Health {

    private List<UriCheck> uris;

    public Health() {
    }

    public Health(List<UriCheck> uris) {
        this.uris = uris;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Health{");
        sb.append("uris=").append(uris);
        sb.append('}');
        return sb.toString();
    }

    public List<UriCheck> getUris() {
        return uris;
    }

    public Health setUris(List<UriCheck> uris) {
        this.uris = uris;
        return this;
    }
}
