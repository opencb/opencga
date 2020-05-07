package org.opencb.opencga.core.config;

public class Panel {

    private String host;

    public Panel() {
    }

    public Panel(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Panel{");
        sb.append("host='").append(host).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getHost() {
        return host;
    }

    public Panel setHost(String host) {
        this.host = host;
        return this;
    }
}
