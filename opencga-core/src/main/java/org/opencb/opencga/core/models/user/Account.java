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

package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Calendar;

/**
 * Created by pfurio on 02/09/16.
 */
public class Account {

    @DataField(id = "expirationDate", indexed = true,
            description = FieldConstants.INTERNAL_ACCOUNT_EXPIRATION_DATE_DESCRIPTION)
    private String expirationDate;

    @DataField(id = "password", since = "3.2.1", description = FieldConstants.INTERNAL_ACCOUNT_PASSWORD_DESCRIPTION)
    private Password password;

    @DataField(id = "failedAttempts", description = FieldConstants.INTERNAL_ACCOUNT_FAILED_ATTEMPTS_DESCRIPTION)
    private int failedAttempts;

    @DataField(id = "authentication", indexed = true, uncommentedClasses = {"AccountType"},
            description = FieldConstants.INTERNAL_ACCOUNT_AUTHENTICATION)
    private AuthenticationOrigin authentication;

    public Account() {
        String creationDate = TimeUtils.getTime();

        // Default 1 year
        Calendar cal = Calendar.getInstance();
        cal.setTime(TimeUtils.toDate(creationDate));
        cal.add(Calendar.YEAR, +1);
        String expirationDate = TimeUtils.getTime(cal.getTime());

        this.expirationDate = expirationDate;
        this.password = new Password();
        this.failedAttempts = 0;
        this.authentication = null;
    }

    public Account(String expirationDate, Password password, int failedAttempts, AuthenticationOrigin authentication) {
        this.expirationDate = expirationDate;
        this.password = password;
        this.failedAttempts = failedAttempts;
        this.authentication = authentication;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Account{");
        sb.append("expirationDate='").append(expirationDate).append('\'');
        sb.append(", password=").append(password);
        sb.append(", failedAttempts=").append(failedAttempts);
        sb.append(", authentication=").append(authentication);
        sb.append('}');
        return sb.toString();
    }

    public String getExpirationDate() {
        return expirationDate;
    }

    public Account setExpirationDate(String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    public Password getPassword() {
        return password;
    }

    public Account setPassword(Password password) {
        this.password = password;
        return this;
    }

    public int getFailedAttempts() {
        return failedAttempts;
    }

    public Account setFailedAttempts(int failedAttempts) {
        this.failedAttempts = failedAttempts;
        return this;
    }

    public AuthenticationOrigin getAuthentication() {
        return authentication;
    }

    public Account setAuthentication(AuthenticationOrigin authentication) {
        this.authentication = authentication;
        return this;
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

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("AuthenticationOrigin{");
            sb.append("id='").append(id).append('\'');
            sb.append(", application=").append(application);
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

        public boolean getApplication() {
            return application;
        }

        public AuthenticationOrigin setApplication(boolean application) {
            this.application = application;
            return this;
        }
    }
}
