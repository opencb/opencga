/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.config;

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
        LDAP,
        AzureAD
    }

    // Possible keys of the options map
    public static final String LDAP_AUTHENTICATION_USER = "authUserId";
    public static final String LDAP_AUTHENTICATION_PASSWORD = "authPassword";
    public static final String LDAP_USERS_SEARCH = "usersSearch";
    public static final String LDAP_GROUPS_SEARCH = "groupsSearch";
    public static final String LDAP_FULLNAME_KEY = "fullNameKey";
    public static final String LDAP_MEMBER_KEY = "memberKey";
    public static final String LDAP_DN_KEY = "dnKey";
    public static final String LDAP_DN_FORMAT = "dnFormat";
    public static final String LDAP_UID_KEY = "uidKey";
    public static final String LDAP_UID_FORMAT = "uidFormat";
    public static final String LDAP_SSL_INVALID_CERTIFICATES_ALLOWED = "sslInvalidCertificatesAllowed";
    public static final String READ_TIMEOUT = "readTimeout";
    public static final String CONNECT_TIMEOUT = "connectTimeout";

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

