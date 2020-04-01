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

import java.util.List;

/**
 * Created by wasim on 06/06/17.
 */

public class Authentication {
    private Long expiration;
    private List<AuthenticationOrigin> authenticationOrigins;

    public Authentication() {
    }

    public Authentication(Long expiration, List<AuthenticationOrigin> authenticationOrigins) {
        this.expiration = expiration;
        this.authenticationOrigins = authenticationOrigins;
    }

    public Long getExpiration() {
        return expiration;
    }

    public Authentication setExpiration(Long expiration) {
        this.expiration = expiration;
        return this;
    }

    public List<AuthenticationOrigin> getAuthenticationOrigins() {
        return authenticationOrigins;
    }

    public Authentication setAuthenticationOrigins(List<AuthenticationOrigin> authenticationOrigins) {
        this.authenticationOrigins = authenticationOrigins;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Authentication{");
        sb.append("expiration=").append(expiration);
        sb.append(", authenticationOrigins=").append(authenticationOrigins);
        sb.append('}');
        return sb.toString();
    }
}
