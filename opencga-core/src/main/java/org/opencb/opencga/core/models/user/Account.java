/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.core.models.user;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Calendar;

/**
 * Created by pfurio on 02/09/16.
 */
public class Account {

    private Type type;
    private String creationDate;
    private String expirationDate;
    private AuthenticationOrigin authentication;

    public Account() {
        String creationDate = TimeUtils.getTime();

        Calendar cal = Calendar.getInstance();
        cal.setTime(TimeUtils.toDate(creationDate));
        cal.add(Calendar.YEAR, +1);
        String expirationDate = TimeUtils.getTime(cal.getTime());

        this.type = Type.FULL;
        this.creationDate = creationDate;
        this.expirationDate = expirationDate;
        this.authentication = null;
    }

    public Account(Type type, String creationDate, String expirationDate, AuthenticationOrigin authentication) {
        this.type = type;
        this.expirationDate = expirationDate;
        this.creationDate = creationDate;
        this.authentication = authentication;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Account{");
        sb.append("type='").append(type).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", expirationDate='").append(expirationDate).append('\'');
        sb.append(", authentication='").append(authentication).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Type getType() {
        return type;
    }

    public Account setType(Type type) {
        this.type = type;
        return this;
    }

    public String getExpirationDate() {
        return expirationDate;
    }

    public Account setExpirationDate(String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Account setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public AuthenticationOrigin getAuthentication() {
        return authentication;
    }

    public Account setAuthentication(AuthenticationOrigin authentication) {
        this.authentication = authentication;
        return this;
    }

    public enum Type {
        GUEST,
        FULL,
        ADMINISTRATOR
    }

    public static class AuthenticationOrigin {

        private String id;
        private boolean application;

        public AuthenticationOrigin() {
        }

        public AuthenticationOrigin(String id, boolean application) {
            this.id = id;
            this.application = application;
        }

        public String getId() {
            return id;
        }

        public AuthenticationOrigin setId(String id) {
            this.id = id;
            return this;
        }

        public boolean getApplication() {
            return application;
        }

        public AuthenticationOrigin setApplication(boolean application) {
            this.application = application;
            return this;
        }
    }
}
