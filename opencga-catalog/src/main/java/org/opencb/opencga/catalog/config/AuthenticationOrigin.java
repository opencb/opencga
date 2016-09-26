package org.opencb.opencga.catalog.config;

import java.util.Collections;
import java.util.Map;

/**
 * Created by pfurio on 02/09/16.
 */
public class AuthenticationOrigin {

    private String id;
    private AuthenticationType type;
    private String host;
    private Map<String, Object> options;

    public enum AuthenticationType {
        OPENCGA,
        LDAP
    }

    // Possible keys of the options map
    public static final String USERS_SEARCH = "usersSearch";
    public static final String GROUPS_SEARCH = "groupsSearch";

    public AuthenticationOrigin() {
        this("internal", AuthenticationType.OPENCGA.name(), "localhost", Collections.emptyMap());
    }

    public AuthenticationOrigin(String id, String type, String host, Map<String, Object> options) {
        this.id = id;
        this.type = AuthenticationType.valueOf(type);
        this.host = host;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Auth{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", host='").append(host).append('\'');
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

    public AuthenticationType getType() {
        return type;
    }

    public AuthenticationOrigin setType(AuthenticationType type) {
        this.type = type;
        return this;
    }

    public String getHost() {
        return host;
    }

    public AuthenticationOrigin setHost(String host) {
        this.host = host;
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

