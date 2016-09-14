package org.opencb.opencga.catalog.config;

import java.util.Map;

/**
 * Created by pfurio on 02/09/16.
 */
public class AuthenticationOrigin {

    private String id;
    private String mode;
    private String host;
    private String port;
    private Map<String, Object> options;

    public AuthenticationOrigin() {
    }

    public AuthenticationOrigin(String id, String mode, String host, String port, Map<String, Object> options) {
        this.id = id;
        this.mode = mode;
        this.host = host;
        this.port = port;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Auth{");
        sb.append("id='").append(id).append('\'');
        sb.append(", mode='").append(mode).append('\'');
        sb.append(", host='").append(host).append('\'');
        sb.append(", port='").append(port).append('\'');
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public AuthenticationOrigin setId(String id) {
        this.id = id;
        return this;
    }

    public String getMode() {
        return mode;
    }

    public AuthenticationOrigin setMode(String mode) {
        this.mode = mode;
        return this;
    }

    public String getHost() {
        return host;
    }

    public AuthenticationOrigin setHost(String host) {
        this.host = host;
        return this;
    }

    public String getPort() {
        return port;
    }

    public AuthenticationOrigin setPort(String port) {
        this.port = port;
        return this;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public AuthenticationOrigin setOptions(Map<String, Object> options) {
        this.options = options;
        return this;
    }
}

